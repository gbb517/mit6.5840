#ifndef REDIS_PROXY_SERVER_H
#define REDIS_PROXY_SERVER_H

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <muduo/net/Buffer.h>
#include <muduo/net/Callbacks.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>

#include <rpc/kvraft/KVRaft_types.h>
#include <shardkv/ShardKVClerk.h>

class RedisProxyServer
{
public:
    RedisProxyServer(std::vector<Host> ctrlerHosts, int port);
    ~RedisProxyServer();

    void run();
    void stop();

    struct RespValue
    {
        enum class Type
        {
            SIMPLE_STRING,
            BULK_STRING,
            INTEGER,
            ARRAY,
            NIL,
            ERROR,
        };

        Type type = Type::NIL;
        std::string str;
        int64_t integer = 0;
        std::vector<RespValue> array;
    };

private:
    void onConnection(const muduo::net::TcpConnectionPtr &conn);
    void onMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buf, muduo::Timestamp);

    RespValue execute(const RespValue &request);
    RespValue executeCommand(const std::vector<std::string> &argv);

    RespValue cmdPing(const std::vector<std::string> &argv);
    RespValue cmdSet(const std::vector<std::string> &argv);
    RespValue cmdGet(const std::vector<std::string> &argv);
    RespValue cmdIncr(const std::vector<std::string> &argv);
    RespValue cmdDel(const std::vector<std::string> &argv);
    RespValue cmdScan(const std::vector<std::string> &argv);
    RespValue cmdExists(const std::vector<std::string> &argv);
    RespValue cmdType(const std::vector<std::string> &argv);

    RespValue cmdHSet(const std::vector<std::string> &argv);
    RespValue cmdHGet(const std::vector<std::string> &argv);
    RespValue cmdHDel(const std::vector<std::string> &argv);
    RespValue cmdHExists(const std::vector<std::string> &argv);
    RespValue cmdHGetAll(const std::vector<std::string> &argv);

    RespValue cmdSAdd(const std::vector<std::string> &argv);
    RespValue cmdSRem(const std::vector<std::string> &argv);
    RespValue cmdSIsMember(const std::vector<std::string> &argv);
    RespValue cmdSMembers(const std::vector<std::string> &argv);
    RespValue cmdSCard(const std::vector<std::string> &argv);

    RespValue cmdZAdd(const std::vector<std::string> &argv);
    RespValue cmdZScore(const std::vector<std::string> &argv);
    RespValue cmdZRank(const std::vector<std::string> &argv);
    RespValue cmdZRange(const std::vector<std::string> &argv);
    RespValue cmdZRem(const std::vector<std::string> &argv);

    RespValue cmdLPush(const std::vector<std::string> &argv);
    RespValue cmdRPush(const std::vector<std::string> &argv);
    RespValue cmdLPop(const std::vector<std::string> &argv);
    RespValue cmdRPop(const std::vector<std::string> &argv);
    RespValue cmdLLen(const std::vector<std::string> &argv);
    RespValue cmdLRange(const std::vector<std::string> &argv);

    std::string encodeResp(const RespValue &value) const;
    std::string upper(std::string s) const;

    bool kvSet(const std::string &key, const std::string &value);
    bool kvGet(const std::string &key, std::string &value, bool &exists);
    int kvDel(const std::string &key);
    bool kvPrefixScan(const std::string &prefix, std::string cursor, int count,
                      std::vector<std::pair<std::string, std::string>> &items,
                      std::string &nextCursor, bool &done);

    bool getTypeMeta(const std::string &key, std::string &type, bool &exists);
    bool setTypeMeta(const std::string &key, const std::string &type);
    bool ensureTypeForWrite(const std::string &key, const std::string &type, bool &created, bool &backendError);
    int delLogicalKey(const std::string &key);

    std::string metaKey(const std::string &key) const;
    std::string strKey(const std::string &key) const;
    std::string hashFieldsKey(const std::string &key) const;
    std::string hashFieldKey(const std::string &key, const std::string &field) const;
    std::string setMembersKey(const std::string &key) const;
    std::string setMemberKey(const std::string &key, const std::string &member) const;
    std::string zsetMembersKey(const std::string &key) const;
    std::string zsetOrderKey(const std::string &key) const;
    std::string zsetMemberScoreKey(const std::string &key, const std::string &member) const;
    std::string listValuesKey(const std::string &key) const;

    std::string encodeList(const std::vector<std::string> &items) const;
    bool decodeList(const std::string &raw, std::vector<std::string> &items) const;
    std::string encodeZOrder(const std::vector<std::pair<int64_t, std::string>> &items) const;
    bool decodeZOrder(const std::string &raw, std::vector<std::pair<int64_t, std::string>> &items) const;
    bool parseInt64(const std::string &s, int64_t &out) const;
    std::string zEntryJoin(int64_t score, const std::string &member) const;
    bool zEntrySplit(const std::string &entry, int64_t &score, std::string &member) const;
    bool canDial(const Host &host, int timeoutMs) const;
    bool backendUnavailableFast();
    void markBackendDownFor(std::chrono::milliseconds duration);
    int64_t nowMs() const;

    ShardId key2shard(const std::string &key, int shardNum) const;
    bool queryLatestConfig(Config &cfg);

private:
    std::vector<Host> ctrlerHosts_;
    ShardKVClerk clerk_;
    std::mutex clerkMu_;
    int port_;
    std::atomic<bool> stopped_{false};
    std::atomic<int64_t> backendDownUntilMs_{0};
    std::unique_ptr<muduo::net::EventLoop> loop_;
    std::unique_ptr<muduo::net::TcpServer> server_;
};

#endif