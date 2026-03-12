# 分布式系统（MIT6.5840） C++实现

## 项目依赖

本项目所依赖的第三方库

| 名称       | 说明                                | 版本   | 链接                                 |
| ---------- | ----------------------------------- | ------ | ------------------------------------ |
| thrift     | 一个轻量级，跨语言的RPC库           | 0.18.1 | https://github.com/apache/thrift     |
| fmt        | 提供字符串格式化功能的库            | 10.0.0 | https://github.com/fmtlib/fmt        |
| googletest | C++测试框架                         | 1.13.0 | https://github.com/google/googletest |
| glog       | C++日志库，实现了应用级别的日志功能 | 0.6.0  | https://github.com/google/glog       |
| gflags     | C++命令行参数解析工具               | 2.2.2  | https://github.com/gflags/gflags     |

## 构建项目

本项目使用`make`来作为构建构建，下面分别给出了每个部分的编译
### MapReduce

执行如下命令构建MapReduce程序，并运行word count任务
```shell
make MapReduce

# 执行word count任务
cd test/mapreduce
bash test.sh
```

### Raft

执行如下命令构建Raft静态库，运行raft程序参见后续Test章节内容，以及KVRaft相关的内容。
```shell
make raft
```

### KVRaft

执行如下命令构建KVRaft静态库，以及KVRaft二进制文件
```shell
make kvraft
```

### shardkv

执行如下命令构建shardkv静态库
```shell
make shardkv
```

## Raft+Redis 测试

连接 `redis-cli` ，可以使用脚本一次启动后端（3个 ShardCtrler + 2个 ShardKV）以及 Redis 协议代理。

### 三步命令

```shell
# 1) 启动整套环境
./scripts/start_raft-redis.sh

# 2) 连接并验证
redis-cli -p 6381
PING
SET k1 v1
GET k1
HSET h1 f1 x
HGET h1 f1

# 3) 停止整套环境
./scripts/stop_raft-redis.sh
```

### 可选参数

```shell
# 自定义端口与 ShardCtrler 地址
REDIS_PORT=6390 CTRL_HOSTS=127.0.0.1:8101,127.0.0.1:8102,127.0.0.1:8103 ./scripts/start_raft-redis.sh
```


## 系统测试

Raft的整体逻辑较为容易理解，但是实现的时候就会发现细节超多，因此实现raft最为痛苦的地方在于debug，本项目使用gtest来编写测试用例，并通过运行如下的命令来运行测试用例：

```shell
# 运行raft测试用例
cd test/raft
make run-test

# 运行kvraft测试用例
cd test/kvraft
make run-test

# 运行shardkv测试用例
cd test/shardkv
make run-test
```

运行指定的测试用例

```shell
make run_test cmd_args="--gtest_list_tests"
make run_test cmd_args="--gtest_filter=*testCaseName"
```
