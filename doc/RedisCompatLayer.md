# Redis 协议兼容层设计（基于 ShardKV）

## 1. 目标

在 `redis-cli` 与 `shardkv` 之间增加一层 RESP 协议服务，命令执行由该服务翻译为 ShardKV KV API。

当前 ShardKV 已具备：
- `PUT/GET`
- `DEL`
- `PREFIX-SCAN`

## 2. 分层结构

- RESP Server
  - 监听 TCP，解析 RESP 数组命令。
- Command Router
  - 将命令路由到 `String/Hash/Set/ZSet/List` 模块。
- KvAdapter
  - 封装 `ShardKVClerk`，提供统一接口：
    - `Set/Get/Del/PrefixScan`
- ExpireManager
  - TTL 管理，策略：惰性检查 + 后台定期清理。

## 3. 关键键空间规范

- String
  - `str:{key}` -> value
  - `exp:{key}` -> unix_ms

- Hash
  - `h:meta:{key}` -> field 列表编码
  - `h:f:{key}:{field}` -> value
  - `exp:{key}`

- Set
  - `s:meta:{key}` -> member 列表编码
  - `s:m:{key}:{member}` -> "1"
  - `exp:{key}`

- ZSet
  - `z:meta:{key}` -> "1"
  - `z:ms:{key}:{member}` -> score
  - `z:ord:{key}:{score32}:{member}` -> "1"
  - `exp:{key}`

- List
  - `l:meta:{key}` -> head/tail/len/seq
  - `l:n:{key}:{id}` -> prev|next|value
  - `exp:{key}`

## 4. TTL 策略

- 惰性检查：每次读 key 时先查 `exp:{key}`。
- 后台线程：按时间桶扫描并清理到期 key。
- 删除动作：调用 `DEL` 删除主键与关联 TTL 键。

## 5. 基础数据结构 API 总结

### 5.1 String
- `SET key value [EX/PX]`
- `GET key`
- `DEL key`
- `EXPIRE key seconds`
- `TTL key`

### 5.2 Hash
- `HSET key field value`
- `HGET key field`
- `HDEL key field`
- `HEXISTS key field`
- `HGETALL key`
- `HSCAN key cursor [MATCH pattern] [COUNT n]`

### 5.3 Set
- `SADD key member...`
- `SREM key member...`
- `SISMEMBER key member`
- `SMEMBERS key`
- `SCARD key`
- `SSCAN key cursor [MATCH pattern] [COUNT n]`

### 5.4 ZSet（score 为 32 位对齐整型字符串）
- `ZADD key score member...`
- `ZSCORE key member`
- `ZRANK key member`
- `ZRANGE key start stop [WITHSCORES]`
- `ZREM key member...`
- `ZSCAN key cursor [MATCH pattern] [COUNT n]`

### 5.5 List
- `LPUSH key value...`
- `RPUSH key value...`
- `LPOP key`
- `RPOP key`
- `LLEN key`
- `LRANGE key start stop`

### 5.6 通用
- `PING`
- `ECHO message`
- `EXISTS key`
- `TYPE key`
- `DEL key...`
- `SCAN cursor [MATCH pattern] [COUNT n]`

## 6. 与 ShardKV 的映射

- 写类命令：优先使用 `PUT` 与 `DEL`。
- 前缀遍历类命令：使用 `PREFIX-SCAN`，结合 cursor/count 实现分页。
- 多 key 命令：逐 key 执行，失败重试，保证幂等。

## 7. 已实现状态（本轮）

- ShardKV RPC 扩展：`DEL`、`PREFIX-SCAN`
- ShardGroup/ShardManger 路径打通
- KVService 增加有序索引支持前缀扫描
- ShardKVClerk 增加 `del/prefixScan`
- 新增回归测试：`TestDel4B`、`TestPrefixScan4B`
