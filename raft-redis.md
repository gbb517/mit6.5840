# Raft-Redis 实现总结（mit6.824 C++ 版本）

本文档总结当前仓库中 Redis 协议兼容层（`redisproxy`）与后端 ShardKV 集群编排（`shardkv_backend`）的具体实现，包括：
- 关键模块与调用链
- 核心函数行为
- 数据结构编码与键空间设计
- 命令实现思路
- 快速失败机制
- 一键起停脚本
- 约束、边界与改进建议

---

## 1. 总体架构

### 1.1 组件分层

1. **RedisProxyServer（RESP 网关）**
   - 文件：`include/redisproxy/RedisProxyServer.h`、`src/redisproxy/RedisProxyServer.cpp`
   - 职责：
     - 监听 TCP，解析 RESP 请求
     - 命令路由（`PING/SET/GET/INCR/...`）
     - 将命令映射到底层 KV 读写

2. **ShardKVClerk（后端访问适配）**
   - `RedisProxyServer` 内部持有 `ShardKVClerk clerk_`
   - 通过 `putAppend/get/del/prefixScan` 访问底层分片 KV

3. **ShardKV + ShardCtrler 集群（后端）**
   - ShardCtrler 维护配置（分片到 gid 的映射、group hosts）
   - ShardKV 提供分片数据服务
   - RPC 定义在 `src/rpc/KVRaft.thrift`

4. **BackendMain（一键后端启动器）**
   - 文件：`src/shardkv/BackendMain.cpp`
   - 单进程拉起：`3 x ShardCtrler` + `2 x ShardKV`
   - 并自动执行 group join

5. **脚本层（运维包装）**
   - `scripts/start_raft-redis.sh`
   - `scripts/stop_raft-redis.sh`

---

## 2. RPC 能力扩展（对 Redis 兼容层的基础支撑）

在 `src/rpc/KVRaft.thrift` 中，`KVRaft` 服务已扩展：
- `del(DeleteParams)`
- `prefixScan(PrefixScanParams)`

对应数据结构：
- `DeleteParams/DeleteReply`
- `PrefixScanParams/PrefixScanReply`

这两个能力使 RedisProxy 能实现：
- 逻辑删除（`DEL`）
- 按前缀扫描（`SCAN`、内部集合元数据遍历）

此外，`src/shardkv/KVService.cpp` 使用：
- `unordered_map um_` 存值
- `map ordered_` 做有序索引

`prefixScan` 依赖 `ordered_` 做游标分页扫描。

---

## 3. RedisProxyServer：网络与协议处理

### 3.1 生命周期

- `run()`
  - 建立监听 socket（`SO_REUSEADDR`）
  - `bind + listen`
  - 每个连接 `accept` 后创建线程 `handleClient()`

- `stop()`
  - 设置 `stopped_`
  - 关闭 `serverFd_`

### 3.2 RESP 解析

- `readRespValue()` 支持：
  - `*` 数组
  - `$` bulk string（含 `$-1` nil）
  - `+` simple string
  - `:` integer

- `readLine()`、`readNBytes()` 是基础 I/O 原语。

### 3.3 请求执行流

- `execute(const RespValue&)`
  - 校验请求必须是数组
  - 仅接受字符串参数类型
  - 组装 `argv` 后进入 `executeCommand()`

- `executeCommand(const vector<string>&)`
  - 命令名大写化 `upper()`
  - 手写 if-chain 路由（当前无注册表）

---

## 4. 命令实现（函数级别）

> 下述函数均位于 `src/redisproxy/RedisProxyServer.cpp`。

### 4.1 通用命令

1. `cmdPing`
   - `PING` -> `+PONG`
   - `PING msg` -> bulk 返回 msg

2. `cmdDel`
   - 支持多 key
   - 逐个调用 `delLogicalKey()`
   - 返回删除键数量

3. `cmdExists`
   - 通过 `meta:{key}` 是否存在判断逻辑 key 是否存在

4. `cmdType`
   - 不存在返回 `none`
   - 存在返回 `string/hash/set/zset/list`

5. `cmdScan`
   - 内部遍历 `meta:` 前缀键
   - 支持 `MATCH`（当前实现是前缀匹配，不是完整 glob）
   - 支持 `COUNT`
   - 返回 `(cursor, keys[])`

### 4.2 String

1. `cmdSet`
   - `SET key value`
   - `ensureTypeForWrite(key, "string")`
   - 写 `str:{key}`，并维护 `meta:{key}=string`

2. `cmdGet`
   - 检查 `meta` 类型必须是 string
   - 读 `str:{key}`，不存在返回 nil

3. `cmdIncr`（新增）
   - 参数：`INCR key`
   - 不存在：创建 string 键并置为 `1`
   - 已存在：解析整数并 +1
   - 非整数或溢出：`ERR value is not an integer or out of range`
   - 非 string：`WRONGTYPE`

### 4.3 Hash

- `cmdHSet/HGet/HDel/HExists/HGetAll`
- 结构：
  - `h:fields:{key}` 存 field 列表（长度前缀编码）
  - `h:f:{key}:{field}` 存 field value

### 4.4 Set

- `cmdSAdd/SRem/SIsMember/SMembers/SCard`
- 结构：
  - `s:members:{key}` 成员列表
  - `s:m:{key}:{member}` 成员存在位（值常为 "1"）

### 4.5 ZSet

- `cmdZAdd/ZScore/ZRank/ZRange/ZRem`
- 结构：
  - `z:members:{key}` member 列表
  - `z:ms:{key}:{member}` member->score
  - `z:order:{key}` 有序条目列表
- 有序条目编码：`"score\x1fmember"`，再走长度前缀列表编码
- 排序规则：`score` 升序，`member` 字典序打破并列

### 4.6 List

- `cmdLPush/RPush/LPop/RPop/LLen/LRange`
- 结构：
  - `l:vals:{key}` 存整条 list 的长度前缀编码
- 当前是“整体读-改-写”策略，不是链表节点细分存储。

---

## 5. 内部键空间与编码策略

### 5.1 类型元数据

- `meta:{key}` -> `string/hash/set/zset/list`
- 作用：统一类型检查与逻辑删除入口

### 5.2 业务值键

- String: `str:{key}`
- Hash fields list: `h:fields:{key}`
- Hash value: `h:f:{key}:{field}`
- Set members list: `s:members:{key}`
- Set member bit: `s:m:{key}:{member}`
- ZSet members list: `z:members:{key}`
- ZSet order list: `z:order:{key}`
- ZSet member score: `z:ms:{key}:{member}`
- List values: `l:vals:{key}`

### 5.3 通用编码函数

1. `encodeList/decodeList`
   - 格式：`<len>:<data><len>:<data>...`
   - 适用于成员列表、字段列表、list 内容、zset 有序条目列表

2. `encodeZOrder/decodeZOrder`
   - 在 `encodeList/decodeList` 上再包一层

3. `parseInt64`
   - 严格全串数字校验

---

## 6. 后端访问与快速失败机制

### 6.1 底层访问函数

- `kvSet` -> `clerk_.putAppend(PUT)`
- `kvGet` -> `clerk_.get`
- `kvDel` -> `clerk_.del`
- `kvPrefixScan` -> `clerk_.prefixScan`

这些函数是 Redis 命令到 ShardKV RPC 的唯一出口。

### 6.2 快速失败（Fast-fail）

为避免后端不可用时长时间阻塞，增加了探活 + 熔断：

1. `backendUnavailableFast()`
   - 若当前仍在熔断窗口内，立即返回不可用
   - 否则对 `ctrlerHosts_` 做短超时拨号探活（`canDial`, 50ms）
   - 全失败则开启 300ms 熔断窗口

2. `markBackendDownFor(200ms)`
   - 任一次底层操作返回失败，会短期标记后端不可用

3. 效果
   - 后端挂掉时命令可在毫秒级返回，而非长重试卡死

---

## 7. 一键后端启动器（`shardkv_backend`）

文件：`src/shardkv/BackendMain.cpp`

### 7.1 启动流程

1. 解析参数：
   - `cluster_log_dir`
   - `ctrl_ports`（默认 `8101,8102,8103`）
   - `kv_ports`（默认 `8201,8202`）
   - `group_gids`（默认 `100,101`）

2. 拉起 3 个 ShardCtrler（通过 `ProcessManager`）

3. 检查 controller ready：
   - 用 `ShardctrlerClerk query(LATEST_CONFIG_NUM)` 重试等待

4. 拉起 2 个 ShardKV

5. 自动 `join`：
   - 把每个 `kv host` 加入对应 `gid`

6. 前台驻留，收到 SIGINT/SIGTERM 退出

### 7.2 重要细节

- 每个子进程日志目录独立：
  - `.../ShardCtrler1`、`.../ShardKV1` 等
- 启动失败会明确 `LOG(ERROR)` 并退出

---

## 8. 脚本化起停

### 8.1 `scripts/start_raft-redis.sh`

- 检查二进制存在，不存在则自动 `make shardkv redisproxy`
- 启动 `shardkv_backend`（后台）
- `wait_port` 等待 `8101` ready
- 启动 `redisproxy`（后台）
- `wait_port` 等待 `REDIS_PORT` ready
- 记录 PID 到 `logs/raft-redis/pids/*.pid`

支持环境变量：
- `REDIS_PORT`
- `CTRL_HOSTS`

### 8.2 `scripts/stop_raft-redis.sh`

- 先停 `redisproxy`，再停 `shardkv_backend`
- 先 `kill`，超时后 `kill -9`
- 清理 pid 文件

---

## 9. 关键边界与当前限制

1. **一致性语义**
   - `INCR` 当前为读改写流程，不是单条原子 compare-and-swap。
   - 并发高冲突场景下，语义可能弱于原生 Redis。

2. **扫描语义**
   - `SCAN MATCH` 目前按前缀处理（截断 `*`），并非完整 glob 语法。

3. **内存/性能特征**
   - Hash/Set/List/ZSet 大量使用“整表列表编码 + 全量改写”，
   - 简化实现但在大 key 下成本较高。

4. **TTL 未落地**
   - 设计文档提及 TTL，但当前实现未包含 EXPIRE/TTL 实际执行路径。

5. **错误码风格**
   - 一部分命令已统一 `ERR backend unavailable`，
   - 仍有部分读路径保留具体 `ERR xxx failed` 文案。

---

## 10. 建议的后续演进

1. 将命令路由从 if-chain 改为注册表（命令名 -> handler）
2. 为 `INCR/DECR` 增加后端原子操作 RPC（减少并发写丢失）
3. 为 `SCAN` 增加真正 glob 匹配（`* ? []`）
4. 为 List/Set/Hash 引入增量结构，降低大 key 改写成本
5. 完成 TTL 命令与后台过期清理
6. 补充 Redis 兼容性回归测试（命令语义 + 错误码）

---

## 11. 快速验收命令

```bash
./scripts/start_raft-redis.sh
redis-cli -p 6381 PING
redis-cli -p 6381 SET k1 v1
redis-cli -p 6381 GET k1
redis-cli -p 6381 INCR age
redis-cli -p 6381 HSET h1 f1 x
redis-cli -p 6381 HGET h1 f1
./scripts/stop_raft-redis.sh
```

如果出现 `ERR backend unavailable`，优先检查：
- `logs/raft-redis/out/shardkv_backend.out`
- `logs/backend/shardkv_backend.ERROR`
- 控制器端口是否被占用（`ss -ltnp | grep 8101`）

---

## 12. 五大数据结构详细实现方案（含代码示例）

本节分成两层：
- **当前实现（已落地）**：基于“元数据键 + 列表编码 + 业务键”
- **增强方案（建议）**：在大 key 与并发场景下更稳、更快

以下示例均以 `RedisProxyServer` 的封装函数为基础：
- `kvSet/kvGet/kvDel/kvPrefixScan`
- `getTypeMeta/setTypeMeta/ensureTypeForWrite`

### 12.1 String（字符串）

#### 当前实现方案

- 元数据：`meta:{key} = "string"`
- 实际值：`str:{key} = value`
- `SET`：类型检查 -> 写 `str:`
- `GET`：类型检查 -> 读 `str:`
- `INCR`：
  1. 若 key 不存在，创建 string 并置 1
  2. 若存在，解析 int64 后加一
  3. 溢出或非整数时报错

#### 关键边界

- 非 string 类型必须返回 `WRONGTYPE`
- `INCR` 在 `int64_t` 上限时报 `out of range`
- 后端异常时返回 `ERR backend unavailable`

#### 代码示例（当前可用实现风格）

```cpp
RedisProxyServer::RespValue RedisProxyServer::cmdIncr(const std::vector<std::string> &argv)
{
   if (argv.size() != 2) return makeErr("ERR wrong number of arguments for 'incr'");

   const auto &key = argv[1];
   std::string t;
   bool typeExists = false;
   if (!getTypeMeta(key, t, typeExists)) return makeErr("ERR backend unavailable");

   if (!typeExists)
   {
      if (!setTypeMeta(key, "string")) return makeErr("ERR backend unavailable");
      if (!kvSet(strKey(key), "1")) return makeErr("ERR backend unavailable");
      return makeInt(1);
   }

   if (t != "string") return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

   std::string value;
   bool exists = false;
   if (!kvGet(strKey(key), value, exists)) return makeErr("ERR backend unavailable");

   int64_t cur = 0;
   if (exists && !parseInt64(value, cur)) return makeErr("ERR value is not an integer or out of range");
   if (cur == std::numeric_limits<int64_t>::max()) return makeErr("ERR value is not an integer or out of range");

   int64_t next = cur + 1;
   if (!kvSet(strKey(key), std::to_string(next))) return makeErr("ERR backend unavailable");
   return makeInt(next);
}
```

#### 增强方案（建议）

- 增加后端原子命令（如 `INCRBY` RPC），避免并发下读改写丢更新。

---

### 12.2 Hash（哈希）

#### 当前实现方案

- 元数据：`meta:{key} = "hash"`
- 字段目录：`h:fields:{key}`（长度前缀编码列表）
- 字段值：`h:f:{key}:{field} = value`

操作：
- `HSET`: 读 fields 列表 -> 更新去重集合 -> 写 field value -> 回写 fields 列表
- `HGET`: 直接读 `h:f:{key}:{field}`
- `HDEL`: 读 fields -> 过滤掉删除字段 -> 删除对应 field value -> 回写 fields
- `HGETALL`: 按 fields 列表顺序逐个取值拼数组

#### 复杂度（当前）

- `HSET/HDEL/HGETALL`：通常为 $O(n)$（n 为字段数）
- `HGET/HEXISTS`：$O(1)$（忽略 RPC 网络成本）

#### 代码示例

```cpp
RedisProxyServer::RespValue RedisProxyServer::cmdHSet(const std::vector<std::string> &argv)
{
   if (argv.size() < 4 || (argv.size() % 2) != 0)
      return makeErr("ERR wrong number of arguments for 'hset'");

   bool created = false, backendError = false;
   if (!ensureTypeForWrite(argv[1], "hash", created, backendError))
      return backendError ? makeErr("ERR backend unavailable")
                     : makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

   std::vector<std::string> fields;
   std::string raw;
   bool exists = false;
   if (!kvGet(hashFieldsKey(argv[1]), raw, exists)) return makeErr("ERR hset failed");
   if (exists && !decodeList(raw, fields)) return makeErr("ERR hset decode failed");

   std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
   int added = 0;
   for (size_t i = 2; i < argv.size(); i += 2)
   {
      const auto &f = argv[i];
      const auto &v = argv[i + 1];
      if (fieldSet.insert(f).second) { fields.push_back(f); added++; }
      if (!kvSet(hashFieldKey(argv[1], f), v)) return makeErr("ERR hset write failed");
   }

   if (!kvSet(hashFieldsKey(argv[1]), encodeList(fields))) return makeErr("ERR hset meta failed");
   return makeInt(added);
}
```

#### 增强方案（建议）

- 将 fields 列表改为分段索引（chunk）降低大 hash 回写成本。

---

### 12.3 Set（集合）

#### 当前实现方案

- 元数据：`meta:{key} = "set"`
- 成员目录：`s:members:{key}`（列表编码）
- 成员存在位：`s:m:{key}:{member} = "1"`

操作：
- `SADD`: 读 members -> 去重 -> 写每个 member 位 -> 回写 members
- `SREM`: 读 members -> 删除位键 -> 回写过滤后的 members
- `SISMEMBER`: 读位键
- `SMEMBERS/SCARD`: 读 members 列表

#### 代码示例

```cpp
RedisProxyServer::RespValue RedisProxyServer::cmdSIsMember(const std::vector<std::string> &argv)
{
   if (argv.size() != 3) return makeErr("ERR wrong number of arguments for 'sismember'");

   std::string t;
   bool ex = false;
   if (!getTypeMeta(argv[1], t, ex)) return makeErr("ERR sismember failed");
   if (!ex) return makeInt(0);
   if (t != "set") return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

   std::string v;
   bool m = false;
   if (!kvGet(setMemberKey(argv[1], argv[2]), v, m)) return makeErr("ERR sismember failed");
   return makeInt(m ? 1 : 0);
}
```

#### 增强方案（建议）

- 对超大 set 引入哈希桶（如 `s:b:{key}:{bucket}`）减少单键膨胀。

---

### 12.4 ZSet（有序集合）

#### 当前实现方案

- 元数据：`meta:{key} = "zset"`
- 成员目录：`z:members:{key}`
- 分数字典：`z:ms:{key}:{member} = score`
- 有序序列：`z:order:{key}`（条目为 `score\x1fmember`，再做长度前缀列表编码）

操作：
- `ZADD`: 更新 member->score，重建有序条目并排序
- `ZSCORE`: 直接读分数字典
- `ZRANK`: 解码有序列表后顺序查 rank
- `ZRANGE`: 解码后按区间返回，支持 `WITHSCORES`
- `ZREM`: 删除 member score 与 order 条目

#### 复杂度（当前）

- `ZADD/ZREM/ZRANK/ZRANGE` 基本为 $O(n)$（n 为成员数）
- 对大 zset 成本较高

#### 代码示例

```cpp
std::sort(order.begin(), order.end(), [](const std::pair<int64_t, std::string> &a,
                               const std::pair<int64_t, std::string> &b)
{
   if (a.first != b.first) return a.first < b.first;
   return a.second < b.second;
});

if (!kvSet(zsetMembersKey(argv[1]), encodeList(members)))
   return makeErr("ERR zadd members failed");
if (!kvSet(zsetOrderKey(argv[1]), encodeZOrder(order)))
   return makeErr("ERR zadd order failed");
```

#### 增强方案（建议）

- 使用跳表或 B+Tree 风格多键索引（按 score 分桶）实现近似 $O(\log n)$。

---

### 12.5 List（列表）

#### 当前实现方案

- 元数据：`meta:{key} = "list"`
- 内容键：`l:vals:{key}`（整个 list 编码后存单值）

操作：
- `LPUSH/RPUSH`: 读整表 -> 头/尾插 -> 回写整表
- `LPOP/RPOP`: 读整表 -> 头/尾删 -> 回写整表
- `LLEN/LRANGE`: 读整表后计算

#### 复杂度（当前）

- 大多数写操作 $O(n)$，因为整表回写

#### 代码示例

```cpp
RedisProxyServer::RespValue RedisProxyServer::cmdLRange(const std::vector<std::string> &argv)
{
   if (argv.size() != 4) return makeErr("ERR wrong number of arguments for 'lrange'");

   std::string t;
   bool ex = false;
   if (!getTypeMeta(argv[1], t, ex)) return makeErr("ERR lrange failed");
   if (!ex) return makeArray({});
   if (t != "list") return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

   int64_t start, stop;
   if (!parseInt64(argv[2], start) || !parseInt64(argv[3], stop))
      return makeErr("ERR value is not an integer or out of range");

   std::string raw;
   bool vex = false;
   if (!kvGet(listValuesKey(argv[1]), raw, vex)) return makeErr("ERR lrange failed");
   if (!vex) return makeArray({});

   std::vector<std::string> vals;
   if (!decodeList(raw, vals)) return makeErr("ERR lrange decode failed");

   // 处理负索引与边界（与 Redis 语义接近）
   int64_t n = static_cast<int64_t>(vals.size());
   if (start < 0) start += n;
   if (stop < 0) stop += n;
   if (start < 0) start = 0;
   if (stop >= n) stop = n - 1;
   if (n == 0 || start > stop || start >= n) return makeArray({});

   std::vector<RespValue> out;
   for (int64_t i = start; i <= stop; i++) out.push_back(makeBulk(vals[i]));
   return makeArray(out);
}
```

#### 增强方案（建议）

- 改为分块链表结构：`l:chunk:{key}:{id}` + 元数据头尾指针，避免整表重写。

---

## 13. 五大结构统一设计模式（当前代码中非常关键）

### 13.1 模式 A：类型先行

任何写入先调用：
- `ensureTypeForWrite(key, expectedType, created, backendError)`

这样可以统一处理：
- 新键初始化类型
- 旧键类型冲突
- 后端不可用

### 13.2 模式 B：逻辑键与物理键分离

- 用户看到 `key`
- 系统写入 `meta:key` + 各结构私有前缀键

优势：
- 删除逻辑统一（`delLogicalKey`）
- 类型判断简单
- 结构演进时可平滑迁移

### 13.3 模式 C：先校验、后操作、最后回写目录

适用于 `Hash/Set/ZSet/List`：
1. 校验参数与类型
2. 读取目录/主体
3. 内存内修改
4. 回写主体 + 回写目录

该模式简单直观，但会有“多步写一致性窗口”，适合当前实验型实现。

---

## 14. 面向生产的最小改造路线（建议）

1. **String**：加原子 `INCRBY` 后端 RPC
2. **Hash/Set/List**：由整表编码改成分块增量编码
3. **ZSet**：引入可排序索引（跳表/树）
4. **通用**：增加幂等写与版本号，降低并发覆盖
5. **测试**：为五类结构补并发与故障注入回归

