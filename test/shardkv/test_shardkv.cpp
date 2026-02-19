#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <climits>
#include <cerrno>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <raft/RaftConfig.h>
#include <rpc/kvraft/KVRaft_types.h>
#include <rpc/kvraft/ShardCtrler.h>
#include <shardkv/ShardCtrler.h>
#include <shardkv/ShardCtrlerClerk.h>
#include <shardkv/ShardKV.h>
#include <shardkv/ShardKVClerk.h>
#include <thrift/TOutput.h>
#include <tools/ProcessManager.hpp>

using std::array;
using std::string;
using std::unordered_map;
using std::vector;

static Host makeHost(const string &ip, int port)
{
    Host h;
    h.ip = ip;
    h.port = port;
    return h;
}

class ShardCtrlerTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ports_ = {7001, 7002, 7003, 7004, 7005, 7006, 7007, 7008};
        logDir_ = fmt::format("../../logs/{}", testing::UnitTest::GetInstance()->current_test_info()->name());
        testLogDir_ = fmt::format("{}/test_shardkv", logDir_);

        auto cleanCmd = fmt::format("rm -rf {}", logDir_);
        std::system(cleanCmd.c_str());

        if (mkdir(logDir_.c_str(), S_IRWXU) && errno != EEXIST)
        {
            LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", logDir_, strerror(errno));
        }
        if (mkdir(testLogDir_.c_str(), S_IRWXU) && errno != EEXIST)
        {
            LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", testLogDir_, strerror(errno));
        }

        FLAGS_log_dir = testLogDir_;
        google::InitGoogleLogging(testLogDir_.c_str());
        apache::thrift::GlobalOutput.setOutputFunction([](const char *msg)
                                                       { LOG(WARNING) << msg; });
    }

    void TearDown() override
    {
        google::ShutdownGoogleLogging();
    }

    void initCtrlers(int num)
    {
        ASSERT_GE(static_cast<int>(ports_.size()), num);

        hosts_ = vector<Host>(num);
        for (int i = 0; i < num; i++)
        {
            hosts_[i].ip = "127.0.0.1";
            hosts_[i].port = ports_[i];
        }

        for (int i = 0; i < num; i++)
        {
            auto peers = hosts_;
            Host me = hosts_[i];
            peers.erase(peers.begin() + i);

            string dirName = fmt::format("{}/ShardCtrler{}", logDir_, i + 1);
            if (mkdir(dirName.c_str(), S_IRWXU) && errno != EEXIST)
            {
                LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", dirName, strerror(errno));
            }

            using TProcessPtr = std::shared_ptr<apache::thrift::TProcessor>;
            ctrs_.emplace_back(me, dirName, [peers, me, dirName, this]() -> TProcessPtr
                               {
                auto handler = std::make_shared<ShardCtrler>(peers, me, dirName, SHARD_NUM_);
                TProcessPtr processor(new ShardCtrlerProcessor(handler));
                return processor; });
        }

        for (int i = 0; i < num; i++)
        {
            ctrs_[i].start();
        }
        std::this_thread::sleep_for(MAX_ELECTION_TIMEOUT);
    }

    Config getConfig(int configNum, ShardctrlerClerk &clerk)
    {
        QueryReply qrep;
        QueryArgs args;
        args.configNum = configNum;

        for (int attempt = 0; attempt < 120; attempt++)
        {
            clerk.query(qrep, args);
            if (qrep.code == ErrorCode::SUCCEED)
            {
                return qrep.config;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        ADD_FAILURE() << "query config failed after retries, configNum=" << configNum
                      << ", last code=" << to_string(qrep.code);
        throw std::runtime_error("query config failed after retries");
    }

    unordered_map<GID, vector<Host>> getGroupsInfo()
    {
        unordered_map<GID, vector<Host>> gid2hosts;
        gid2hosts[1] = {makeHost("x", 8001), makeHost("y", 8002), makeHost("z", 8003)};
        gid2hosts[2] = {makeHost("a", 8101), makeHost("b", 8102), makeHost("c", 8103)};
        gid2hosts[3] = {makeHost("j", 8201), makeHost("k", 8202), makeHost("l", 8203)};
        return gid2hosts;
    }

    vector<Host> makeHosts(int gid)
    {
        return {
            makeHost(fmt::format("{}a", gid), 9000 + gid),
            makeHost(fmt::format("{}b", gid), 10000 + gid),
            makeHost(fmt::format("{}c", gid), 11000 + gid),
        };
    }

    void joinOne(ShardctrlerClerk &clerk, GID gid, const vector<Host> &hosts)
    {
        JoinReply jrep;
        JoinArgs args;
        args.servers[gid] = hosts;
        clerk.join(jrep, args);
        ASSERT_EQ(jrep.code, ErrorCode::SUCCEED);
    }

    void leaveOne(ShardctrlerClerk &clerk, GID gid)
    {
        LeaveReply lrep;
        LeaveArgs args;
        args.gids = {gid};
        clerk.leave(lrep, args);
        ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);
    }

    void restartCtrler(int id)
    {
        ctrs_[id].start();
        std::this_thread::sleep_for(MAX_ELECTION_TIMEOUT);
    }

    void check(const unordered_map<GID, vector<Host>> &groups, ShardctrlerClerk &clerk)
    {
        auto config = getConfig(LATEST_CONFIG_NUM, clerk);
        EXPECT_EQ(groups.size(), config.groupHosts.size());

        for (const auto &it : groups)
        {
            auto p = config.groupHosts.find(it.first);
            ASSERT_NE(p, config.groupHosts.end());
            const auto &hosts = it.second;
            const auto &got = p->second;
            ASSERT_EQ(hosts.size(), got.size());
            for (int i = 0; i < static_cast<int>(hosts.size()); i++)
            {
                EXPECT_EQ(hosts[i].ip, got[i].ip);
                EXPECT_EQ(hosts[i].port, got[i].port);
            }
        }

        if (!groups.empty())
        {
            for (GID gid : config.shard2gid)
            {
                EXPECT_NE(gid, INVALID_GID);
                EXPECT_TRUE(config.groupHosts.find(gid) != config.groupHosts.end());
            }
        }

        int maxS = -1;
        int minS = INT_MAX;
        for (const auto &it : config.gid2shards)
        {
            int shardCnt = static_cast<int>(it.second.size());
            maxS = std::max(maxS, shardCnt);
            minS = std::min(minS, shardCnt);
        }

        if (!groups.empty())
        {
            EXPECT_LE(maxS - minS, 1);
        }
    }

    void checkSameConfig(const Config &c1, const Config &c2)
    {
        EXPECT_EQ(c1.configNum, c2.configNum);
        EXPECT_EQ(c1.shard2gid, c2.shard2gid);
        EXPECT_EQ(c1.groupHosts.size(), c2.groupHosts.size());

        for (const auto &it : c1.groupHosts)
        {
            auto p = c2.groupHosts.find(it.first);
            ASSERT_NE(p, c2.groupHosts.end());
            const auto &hosts1 = it.second;
            const auto &hosts2 = p->second;
            ASSERT_EQ(hosts1.size(), hosts2.size());
            for (int i = 0; i < static_cast<int>(hosts1.size()); i++)
            {
                EXPECT_EQ(hosts1[i].ip, hosts2[i].ip);
                EXPECT_EQ(hosts1[i].port, hosts2[i].port);
            }
        }
    }

protected:
    vector<int> ports_;
    string logDir_;
    string testLogDir_;
    vector<ProcessManager> ctrs_;
    vector<Host> hosts_;
    int SHARD_NUM_ = 10;
};

class ShardKVTest : public testing::Test
{
protected:
    void SetUp() override
    {
        ports_ = {7101, 7102, 7103, 7104, 7105, 7106, 7107, 7108};
        logDir_ = fmt::format("../../logs/{}", testing::UnitTest::GetInstance()->current_test_info()->name());

        auto cleanCmd = fmt::format("rm -rf {}", logDir_);
        std::system(cleanCmd.c_str());

        if (mkdir(logDir_.c_str(), S_IRWXU) && errno != EEXIST)
        {
            LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", logDir_, strerror(errno));
        }

        FLAGS_log_dir = logDir_;
        google::InitGoogleLogging(FLAGS_log_dir.c_str());
        apache::thrift::GlobalOutput.setOutputFunction([](const char *msg)
                                                       { LOG(WARNING) << msg; });
    }

    void TearDown() override
    {
        google::ShutdownGoogleLogging();
    }

    void initCtrlers(int num)
    {
        ctrlHosts_.assign(num, Host{});
        for (int i = 0; i < num; i++)
        {
            ctrlHosts_[i] = makeHost("127.0.0.1", ports_[i] + 1000);
        }

        for (int i = 0; i < num; i++)
        {
            auto peers = ctrlHosts_;
            Host me = ctrlHosts_[i];
            peers.erase(peers.begin() + i);
            string dirName = fmt::format("{}/ShardCtrler{}", logDir_, i + 1);
            if (mkdir(dirName.c_str(), S_IRWXU) && errno != EEXIST)
            {
                LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", dirName, strerror(errno));
            }

            using TProcessPtr = std::shared_ptr<apache::thrift::TProcessor>;
            ctrls_.emplace_back(me, dirName, [peers, me, dirName]() -> TProcessPtr
                                {
                auto handler = std::make_shared<ShardCtrler>(peers, me, dirName, 10);
                return TProcessPtr(new ShardCtrlerProcessor(handler)); });
        }

        for (auto &ctrl : ctrls_)
        {
            ctrl.start();
        }
        std::this_thread::sleep_for(MAX_ELECTION_TIMEOUT);
    }

    void initShardKVs(int num)
    {
        kvHosts_.assign(num, Host{});
        for (int i = 0; i < num; i++)
        {
            kvHosts_[i] = makeHost("127.0.0.1", ports_[i]);
        }

        for (int i = 0; i < num; i++)
        {
            Host me = kvHosts_[i];
            string dirName = fmt::format("{}/ShardKV{}", logDir_, i + 1);
            if (mkdir(dirName.c_str(), S_IRWXU) && errno != EEXIST)
            {
                LOG(WARNING) << fmt::format("mkdir \"{}\" failed: {}", dirName, strerror(errno));
            }

            auto ctrlerHosts = ctrlHosts_;
            using TProcessPtr = std::shared_ptr<apache::thrift::TProcessor>;
            kvs_.emplace_back(me, dirName, [ctrlerHosts, me]() mutable -> TProcessPtr
                              {
                auto handler = std::make_shared<ShardKV>(ctrlerHosts, me);
                return TProcessPtr(new ShardKVRaftProcessor(handler)); });
        }

        for (auto &kv : kvs_)
        {
            kv.start();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    void joinGroup(ShardctrlerClerk &mck, GID gid, const vector<Host> &hosts)
    {
        JoinReply jrep;
        JoinArgs jargs;
        jargs.servers[gid] = hosts;
        mck.join(jrep, jargs);
        ASSERT_EQ(jrep.code, ErrorCode::SUCCEED);
    }

    Config latestConfig(ShardctrlerClerk &mck)
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        mck.query(qrep, qargs);
        EXPECT_EQ(qrep.code, ErrorCode::SUCCEED);
        return qrep.config;
    }

    bool putRetry(ShardKVClerk &ck, const string &key, const string &value, PutOp::type op, int retries = 20)
    {
        PutAppendParams pp;
        pp.key = key;
        pp.value = value;
        pp.op = op;

        PutAppendReply pr;
        for (int i = 0; i < retries; i++)
        {
            ck.putAppend(pr, pp);
            if (pr.code == ErrorCode::SUCCEED)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    bool getRetry(ShardKVClerk &ck, const string &key, GetReply &gr, int retries = 20)
    {
        GetParams gp;
        gp.key = key;
        for (int i = 0; i < retries; i++)
        {
            ck.get(gr, gp);
            if (gr.code == ErrorCode::SUCCEED || gr.code == ErrorCode::ERR_NO_KEY)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void stopKV(int idx)
    {
        kvs_[idx].killProcess();
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
    }

    void startKV(int idx)
    {
        kvs_[idx].start();
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
    }

    void leaveGroup(ShardctrlerClerk &mck, GID gid)
    {
        LeaveReply lrep;
        LeaveArgs largs;
        largs.gids = {gid};
        mck.leave(lrep, largs);
        ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);
    }

protected:
    vector<int> ports_;
    string logDir_;
    vector<ProcessManager> ctrls_;
    vector<ProcessManager> kvs_;
    vector<Host> ctrlHosts_;
    vector<Host> kvHosts_;
};

TEST_F(ShardCtrlerTest, BasicTest4A)
{
    initCtrlers(3);
    auto gid2hosts = getGroupsInfo();
    ShardctrlerClerk ck(hosts_);

    vector<Config> cfa;
    cfa.push_back(getConfig(LATEST_CONFIG_NUM, ck));
    check(unordered_map<GID, vector<Host>>{}, ck);

    joinOne(ck, 1, gid2hosts[1]);
    check(unordered_map<GID, vector<Host>>{{1, gid2hosts[1]}}, ck);
    cfa.push_back(getConfig(LATEST_CONFIG_NUM, ck));

    joinOne(ck, 2, gid2hosts[2]);
    check(unordered_map<GID, vector<Host>>{{1, gid2hosts[1]}, {2, gid2hosts[2]}}, ck);
    cfa.push_back(getConfig(LATEST_CONFIG_NUM, ck));

    auto cfx = getConfig(LATEST_CONFIG_NUM, ck);
    ASSERT_EQ(cfx.groupHosts[1].size(), 3);
    ASSERT_EQ(cfx.groupHosts[2].size(), 3);
    EXPECT_EQ(cfx.groupHosts[1][0].ip, "x");
    EXPECT_EQ(cfx.groupHosts[1][1].ip, "y");
    EXPECT_EQ(cfx.groupHosts[1][2].ip, "z");
    EXPECT_EQ(cfx.groupHosts[2][0].ip, "a");
    EXPECT_EQ(cfx.groupHosts[2][1].ip, "b");
    EXPECT_EQ(cfx.groupHosts[2][2].ip, "c");

    leaveOne(ck, 1);
    check(unordered_map<GID, vector<Host>>{{2, gid2hosts[2]}}, ck);
    cfa.push_back(getConfig(LATEST_CONFIG_NUM, ck));

    leaveOne(ck, 2);
    cfa.push_back(getConfig(LATEST_CONFIG_NUM, ck));

    for (int s = 0; s < 3; s++)
    {
        ctrs_[s].killProcess();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        for (const auto &c : cfa)
        {
            auto got = getConfig(c.configNum, ck);
            checkSameConfig(c, got);
        }
        restartCtrler(s);
    }

    joinOne(ck, 503, makeHosts(503));
    joinOne(ck, 504, makeHosts(504));
    for (int sid = 0; sid < SHARD_NUM_; sid++)
    {
        auto cf = getConfig(LATEST_CONFIG_NUM, ck);
        GID target = sid < SHARD_NUM_ / 2 ? 503 : 504;
        MoveReply mrep;
        MoveArgs args;
        args.shard = sid;
        args.gid = target;
        ck.move(mrep, args);
        ASSERT_EQ(mrep.code, ErrorCode::SUCCEED);

        if (cf.shard2gid[sid] != target)
        {
            auto cf1 = getConfig(LATEST_CONFIG_NUM, ck);
            EXPECT_GT(cf1.configNum, cf.configNum);
        }
    }

    auto moved = getConfig(LATEST_CONFIG_NUM, ck);
    for (int sid = 0; sid < SHARD_NUM_; sid++)
    {
        if (sid < SHARD_NUM_ / 2)
        {
            EXPECT_EQ(moved.shard2gid[sid], 503);
        }
        else
        {
            EXPECT_EQ(moved.shard2gid[sid], 504);
        }
    }
    leaveOne(ck, 503);
    leaveOne(ck, 504);

    const int npara = 10;
    vector<GID> gids(npara);
    unordered_map<GID, vector<Host>> expected;
    vector<std::thread> workers;
    std::atomic<int> ok{0};

    for (int i = 0; i < npara; i++)
    {
        gids[i] = 100 + i;
        expected[gids[i]] = {makeHost(fmt::format("s{}b", gids[i]), 7000 + gids[i])};
        workers.emplace_back([this, gid = gids[i], &ok]()
                             {
            ShardctrlerClerk c(hosts_);

            auto joinRetry = [&c](const JoinArgs& args) {
                JoinReply rep;
                for (int t = 0; t < 8; t++)
                {
                    c.join(rep, args);
                    if (rep.code == ErrorCode::SUCCEED)
                    {
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                return false;
            };

            auto leaveRetry = [&c](const LeaveArgs& args) {
                LeaveReply rep;
                for (int t = 0; t < 8; t++)
                {
                    c.leave(rep, args);
                    if (rep.code == ErrorCode::SUCCEED)
                    {
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                return false;
            };

            JoinArgs a1;
            a1.servers[gid + 1000] = {makeHost(fmt::format("s{}a", gid), 6000 + gid)};
            JoinArgs a2;
            a2.servers[gid] = {makeHost(fmt::format("s{}b", gid), 7000 + gid)};
            LeaveArgs la;
            la.gids = {gid + 1000};

            bool ok1 = joinRetry(a1);
            bool ok2 = joinRetry(a2);
            bool ok3 = leaveRetry(la);

            if (ok1 && ok2 && ok3)
            {
                ok++;
            } });
    }

    for (auto &th : workers)
    {
        th.join();
    }
    EXPECT_EQ(ok.load(), npara);
    check(expected, ck);

    auto c1 = getConfig(LATEST_CONFIG_NUM, ck);
    for (int i = 0; i < 5; i++)
    {
        joinOne(ck, 200 + i, makeHosts(200 + i));
    }
    auto c2 = getConfig(LATEST_CONFIG_NUM, ck);
    for (GID gid : gids)
    {
        for (int sid = 0; sid < SHARD_NUM_; sid++)
        {
            if (c2.shard2gid[sid] == gid)
            {
                EXPECT_EQ(c1.shard2gid[sid], gid);
            }
        }
    }

    for (int i = 0; i < 5; i++)
    {
        leaveOne(ck, 200 + i);
    }
    auto c3 = getConfig(LATEST_CONFIG_NUM, ck);
    for (GID gid : gids)
    {
        for (int sid = 0; sid < SHARD_NUM_; sid++)
        {
            if (c2.shard2gid[sid] == gid)
            {
                EXPECT_EQ(c3.shard2gid[sid], gid);
            }
        }
    }
}

TEST_F(ShardCtrlerTest, TestMulti4A)
{
    initCtrlers(3);
    ShardctrlerClerk ck(hosts_);

    auto h1 = makeHosts(1);
    auto h2 = makeHosts(2);
    auto h3 = makeHosts(3);

    check(unordered_map<GID, vector<Host>>{}, ck);

    JoinReply jr;
    JoinArgs ja;
    ja.servers[1] = h1;
    ja.servers[2] = h2;
    ck.join(jr, ja);
    ASSERT_EQ(jr.code, ErrorCode::SUCCEED);
    check(unordered_map<GID, vector<Host>>{{1, h1}, {2, h2}}, ck);

    joinOne(ck, 3, h3);
    check(unordered_map<GID, vector<Host>>{{1, h1}, {2, h2}, {3, h3}}, ck);

    LeaveReply lrep;
    LeaveArgs largs;
    largs.gids = {1, 3};
    ck.leave(lrep, largs);
    ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);
    check(unordered_map<GID, vector<Host>>{{2, h2}}, ck);

    leaveOne(ck, 2);

    const int npara = 10;
    vector<GID> gids(npara);
    unordered_map<GID, vector<Host>> groups;
    vector<std::thread> threads;
    threads.reserve(npara);
    std::atomic<int> ok{0};

    for (int i = 0; i < npara; i++)
    {
        gids[i] = 1000 + i;
        groups[gids[i]] = {makeHost(fmt::format("{}a", gids[i]), 10000 + gids[i])};
        threads.emplace_back([this, gid = gids[i], &ok]()
                             {
            ShardctrlerClerk c(hosts_);

            auto joinRetry = [&c](const JoinArgs& args) {
                JoinReply rep;
                for (int t = 0; t < 8; t++)
                {
                    c.join(rep, args);
                    if (rep.code == ErrorCode::SUCCEED)
                    {
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                return false;
            };

            auto leaveRetry = [&c](const LeaveArgs& args) {
                LeaveReply rep;
                for (int t = 0; t < 8; t++)
                {
                    c.leave(rep, args);
                    if (rep.code == ErrorCode::SUCCEED)
                    {
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                return false;
            };

            JoinArgs args;
            args.servers[gid] = {makeHost(fmt::format("{}a", gid), 10000 + gid)};
            args.servers[gid + 1000] = {makeHost(fmt::format("{}a", gid + 1000), 11000 + gid)};
            args.servers[gid + 2000] = {makeHost(fmt::format("{}a", gid + 2000), 12000 + gid)};

            LeaveArgs la;
            la.gids = {gid + 1000, gid + 2000};

            bool ok1 = joinRetry(args);
            bool ok2 = leaveRetry(la);

            if (ok1 && ok2)
            {
                ok++;
            } });
    }

    for (auto &th : threads)
    {
        th.join();
    }
    EXPECT_EQ(ok.load(), npara);
    check(groups, ck);

    auto c1 = getConfig(LATEST_CONFIG_NUM, ck);
    JoinArgs mj;
    for (int i = 0; i < 5; i++)
    {
        GID gid = npara + 1 + i;
        mj.servers[gid] = {makeHost(fmt::format("{}a", gid), 13000 + gid), makeHost(fmt::format("{}b", gid), 14000 + gid)};
    }
    ck.join(jr, mj);
    ASSERT_EQ(jr.code, ErrorCode::SUCCEED);

    auto c2 = getConfig(LATEST_CONFIG_NUM, ck);
    for (GID gid : gids)
    {
        for (int sid = 0; sid < SHARD_NUM_; sid++)
        {
            if (c2.shard2gid[sid] == gid)
            {
                EXPECT_EQ(c1.shard2gid[sid], gid);
            }
        }
    }

    LeaveArgs ml;
    for (int i = 0; i < 5; i++)
    {
        ml.gids.push_back(npara + 1 + i);
    }
    ck.leave(lrep, ml);
    ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);

    auto c3 = getConfig(LATEST_CONFIG_NUM, ck);
    for (GID gid : gids)
    {
        for (int sid = 0; sid < SHARD_NUM_; sid++)
        {
            if (c2.shard2gid[sid] == gid)
            {
                EXPECT_EQ(c3.shard2gid[sid], gid);
            }
        }
    }
}

TEST_F(ShardCtrlerTest, MinimalTransfer4A)
{
    initCtrlers(3);
    ShardctrlerClerk ck(hosts_);

    for (int gid = 1; gid <= 5; gid++)
    {
        joinOne(ck, gid, makeHosts(gid));
    }
    auto c1 = getConfig(LATEST_CONFIG_NUM, ck);

    joinOne(ck, 6, makeHosts(6));
    auto c2 = getConfig(LATEST_CONFIG_NUM, ck);

    int moved = 0;
    for (int sid = 0; sid < SHARD_NUM_; sid++)
    {
        if (c1.shard2gid[sid] != c2.shard2gid[sid])
        {
            moved++;
        }
    }

    EXPECT_GE(moved, 1);
    EXPECT_LE(moved, 2);
}

TEST_F(ShardKVTest, TestStaticShards4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});
    joinGroup(mck, 101, {kvHosts_[1]});

    ShardKVClerk ck(ctrlHosts_);

    vector<string> keys;
    vector<string> vals;
    for (int i = 0; i < 10; i++)
    {
        keys.push_back(fmt::format("k{}", i));
        vals.push_back(fmt::format("v{}", i));

        PutAppendParams pp;
        pp.key = keys.back();
        pp.value = vals.back();
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);
    }

    for (int i = 0; i < 10; i++)
    {
        GetParams gp;
        gp.key = keys[i];
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, vals[i]);
    }
}

TEST_F(ShardKVTest, TestJoinLeave4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    ShardKVClerk ck(ctrlHosts_);

    for (int i = 0; i < 10; i++)
    {
        PutAppendParams pp;
        pp.key = fmt::format("base{}", i);
        pp.value = "v";
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = pp.key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "v");
    }

    joinGroup(mck, 101, {kvHosts_[1]});

    for (int i = 0; i < 10; i++)
    {
        string key = fmt::format("new{}", i);
        PutAppendParams pp;
        pp.key = key;
        pp.value = "x";
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        pp.op = PutOp::APPEND;
        pp.value = "y";
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "xy");
    }

    LeaveReply lrep;
    LeaveArgs largs;
    largs.gids = {100};
    mck.leave(lrep, largs);
    ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);

    for (int i = 0; i < 10; i++)
    {
        string key = fmt::format("after{}", i);
        PutAppendParams pp;
        pp.key = key;
        pp.value = "m";
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "m");
    }
}

TEST_F(ShardKVTest, TestSnapshot4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);

    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 30; i++)
    {
        string key = fmt::format("snap-a-{}", i);
        string val = fmt::format("v{}{}{}", i, string(40, 'x'), string(40, 'y'));

        PutAppendParams pp;
        pp.key = key;
        pp.value = val;
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, val);
    }

    joinGroup(mck, 101, {kvHosts_[1]});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < 20; i++)
    {
        string key = fmt::format("snap-b-{}", i);

        PutAppendParams pp;
        pp.key = key;
        pp.value = "base";
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        pp.op = PutOp::APPEND;
        pp.value = fmt::format("-{}", i);
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, fmt::format("base-{}", i));
    }

    LeaveReply lrep;
    LeaveArgs largs;
    largs.gids = {101};
    mck.leave(lrep, largs);
    ASSERT_EQ(lrep.code, ErrorCode::SUCCEED);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < 20; i++)
    {
        string key = fmt::format("snap-c-{}", i);

        PutAppendParams pp;
        pp.key = key;
        pp.value = "m";
        pp.op = PutOp::PUT;
        PutAppendReply pr;
        ck.putAppend(pr, pp);
        ASSERT_EQ(pr.code, ErrorCode::SUCCEED);

        GetParams gp;
        gp.key = key;
        GetReply gr;
        ck.get(gr, gp);
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "m");
    }
}

TEST_F(ShardKVTest, TestMissChange4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);

    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 12; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("mc-base-{}", i), "v", PutOp::PUT));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("mc-base-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
    }

    stopKV(1);
    joinGroup(mck, 101, {kvHosts_[1]});
    leaveGroup(mck, 101);
    joinGroup(mck, 102, {kvHosts_[1]});
    leaveGroup(mck, 102);

    for (int i = 0; i < 12; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("mc-during-{}", i), "x", PutOp::PUT));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("mc-during-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "x");
    }

    startKV(1);
    joinGroup(mck, 103, {kvHosts_[1]});
    leaveGroup(mck, 103);

    for (int i = 0; i < 12; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("mc-after-{}", i), "y", PutOp::PUT));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("mc-after-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "y");
    }
}

TEST_F(ShardKVTest, TestConcurrent1_4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    const int n = 8;
    std::atomic<bool> done{false};
    vector<std::atomic<int>> cnt(n);
    for (auto &x : cnt)
    {
        x.store(0);
    }

    vector<std::thread> workers;
    for (int i = 0; i < n; i++)
    {
        string key = fmt::format("c1-{}", i);
        ASSERT_TRUE(putRetry(ck, key, "s", PutOp::PUT));
        workers.emplace_back([this, &done, &cnt, i, key]()
                             {
            ShardKVClerk local(ctrlHosts_);
            while (!done.load())
            {
                if (putRetry(local, key, "x", PutOp::APPEND, 3))
                {
                    cnt[i]++;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            } });
    }

    stopKV(1);
    for (int t = 0; t < 3; t++)
    {
        joinGroup(mck, 101, {kvHosts_[1]});
        leaveGroup(mck, 101);
    }
    startKV(1);

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    done = true;
    for (auto &th : workers)
    {
        th.join();
    }

    for (int i = 0; i < n; i++)
    {
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("c1-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(static_cast<int>(gr.value.size()), 1 + cnt[i].load());
    }
}

TEST_F(ShardKVTest, TestConcurrent2_4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    const int n = 6;
    std::atomic<bool> done{false};
    vector<std::thread> workers;
    for (int i = 0; i < n; i++)
    {
        string key = fmt::format("c2-{}", i);
        ASSERT_TRUE(putRetry(ck, key, "b", PutOp::PUT));
        workers.emplace_back([this, &done, key]()
                             {
            ShardKVClerk local(ctrlHosts_);
            while (!done.load())
            {
                putRetry(local, key, "a", PutOp::APPEND, 3);
                std::this_thread::sleep_for(std::chrono::milliseconds(8));
            } });
    }

    stopKV(1);
    for (int i = 0; i < 2; i++)
    {
        joinGroup(mck, 101, {kvHosts_[1]});
        leaveGroup(mck, 101);
    }
    startKV(1);

    stopKV(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    startKV(0);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    done = true;
    for (auto &th : workers)
    {
        th.join();
    }

    for (int i = 0; i < n; i++)
    {
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("c2-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_FALSE(gr.value.empty());
    }
}

TEST_F(ShardKVTest, TestConcurrent3_4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    const int n = 5;
    std::atomic<bool> done{false};
    vector<std::thread> workers;
    for (int i = 0; i < n; i++)
    {
        string key = fmt::format("c3-{}", i);
        ASSERT_TRUE(putRetry(ck, key, "z", PutOp::PUT));
        workers.emplace_back([this, &done, key]()
                             {
            ShardKVClerk local(ctrlHosts_);
            while (!done.load())
            {
                putRetry(local, key, "w", PutOp::APPEND, 2);
            } });
    }

    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(1200))
    {
        joinGroup(mck, 101, {kvHosts_[1]});
        leaveGroup(mck, 101);
        stopKV(0);
        startKV(0);
    }

    done = true;
    for (auto &th : workers)
    {
        th.join();
    }

    for (int i = 0; i < n; i++)
    {
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("c3-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
    }
}

TEST_F(ShardKVTest, TestUnreliable1_4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 15; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("u1-{}", i), "v", PutOp::PUT));
    }

    for (int round = 0; round < 4; round++)
    {
        stopKV(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        startKV(0);
        for (int i = 0; i < 8; i++)
        {
            ASSERT_TRUE(putRetry(ck, fmt::format("u1-r{}-{}", round, i), "x", PutOp::PUT));
            GetReply gr;
            ASSERT_TRUE(getRetry(ck, fmt::format("u1-r{}-{}", round, i), gr));
            ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        }
    }
}

TEST_F(ShardKVTest, TestUnreliable2_4B)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 25; i++)
    {
        string key = fmt::format("u2-{}", i);
        ASSERT_TRUE(putRetry(ck, key, "a", PutOp::PUT));
        ASSERT_TRUE(putRetry(ck, key, "b", PutOp::APPEND));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, key, gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(gr.value, "ab");

        if (i % 6 == 0)
        {
            stopKV(0);
            startKV(0);
        }
    }
}

TEST_F(ShardKVTest, TestChallenge1Delete)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 40; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("ch1-{}", i), string(200, 'p'), PutOp::PUT));
    }

    stopKV(1);
    joinGroup(mck, 101, {kvHosts_[1]});
    leaveGroup(mck, 101);
    startKV(1);

    for (int i = 0; i < 10; i++)
    {
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("ch1-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
        EXPECT_EQ(static_cast<int>(gr.value.size()), 200);
    }
}

TEST_F(ShardKVTest, TestChallenge2Unaffected)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});

    for (int i = 0; i < 12; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("ch2u-{}", i), "0", PutOp::PUT));
    }

    stopKV(1);
    joinGroup(mck, 101, {kvHosts_[1]});

    for (int i = 0; i < 12; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("ch2u-{}", i), "1", PutOp::APPEND));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("ch2u-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
    }

    leaveGroup(mck, 101);
    startKV(1);
}

TEST_F(ShardKVTest, TestChallenge2Partial)
{
    initCtrlers(3);
    initShardKVs(2);

    ShardctrlerClerk mck(ctrlHosts_);
    ShardKVClerk ck(ctrlHosts_);
    joinGroup(mck, 100, {kvHosts_[0]});
    joinGroup(mck, 101, {kvHosts_[1]});

    for (int i = 0; i < 15; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("ch2p-{}", i), "a", PutOp::PUT));
    }

    stopKV(1);
    leaveGroup(mck, 101);

    for (int i = 0; i < 15; i++)
    {
        ASSERT_TRUE(putRetry(ck, fmt::format("ch2p-{}", i), "b", PutOp::APPEND));
        GetReply gr;
        ASSERT_TRUE(getRetry(ck, fmt::format("ch2p-{}", i), gr));
        ASSERT_EQ(gr.code, ErrorCode::SUCCEED);
    }

    startKV(1);
}
