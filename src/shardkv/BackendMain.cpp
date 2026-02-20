#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thrift/TOutput.h>

#include <raft/RaftConfig.h>
#include <rpc/kvraft/ShardCtrler.h>
#include <rpc/kvraft/ShardKVRaft.h>
#include <shardkv/ShardCtrler.h>
#include <shardkv/ShardCtrlerClerk.h>
#include <shardkv/ShardKV.h>
#include <tools/ProcessManager.hpp>

DEFINE_string(cluster_log_dir, "./logs/backend", "log root dir for one-click backend launcher");
DEFINE_string(ctrl_ports, "8101,8102,8103", "ShardCtrler listen ports, comma separated");
DEFINE_string(kv_ports, "8201,8202", "ShardKV listen ports, comma separated");
DEFINE_string(group_gids, "100,101", "gid list mapped 1:1 with kv_ports");
DEFINE_int32(shard_num, 10, "number of shards for shard controller");

static std::atomic<bool> g_stop{false};

static void onSignal(int)
{
    g_stop.store(true);
}

static bool ensureDir(const std::string &path)
{
    if (path.empty())
        return false;

    std::string cur;
    cur.reserve(path.size());

    for (size_t i = 0; i < path.size(); i++)
    {
        char c = path[i];
        cur.push_back(c);

        if (c != '/' && i + 1 != path.size())
        {
            continue;
        }

        if (cur == "/")
        {
            continue;
        }

        if (mkdir(cur.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0 && errno != EEXIST)
        {
            return false;
        }
    }

    if (mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0 && errno != EEXIST)
    {
        return false;
    }

    return true;
}

static std::vector<int> parseIntList(const std::string &text)
{
    std::vector<int> out;
    size_t start = 0;
    while (start < text.size())
    {
        size_t comma = text.find(',', start);
        std::string token = text.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!token.empty())
        {
            out.push_back(std::stoi(token));
        }
        if (comma == std::string::npos)
        {
            break;
        }
        start = comma + 1;
    }
    return out;
}

static Host makeHost(const std::string &ip, int port)
{
    Host h;
    h.ip = ip;
    h.port = static_cast<int16_t>(port);
    return h;
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!ensureDir(FLAGS_cluster_log_dir))
    {
        std::cerr << "failed to create cluster_log_dir: " << FLAGS_cluster_log_dir << ", errno=" << errno << "(" << strerror(errno) << ")" << std::endl;
        return 1;
    }

    FLAGS_log_dir = FLAGS_cluster_log_dir;
    FLAGS_logbuflevel = -1;
    google::InitGoogleLogging(argv[0]);
    apache::thrift::GlobalOutput.setOutputFunction([](const char *msg)
                                                   { LOG(WARNING) << msg; });

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    auto ctrlPorts = parseIntList(FLAGS_ctrl_ports);
    auto kvPorts = parseIntList(FLAGS_kv_ports);
    auto gids = parseIntList(FLAGS_group_gids);

    if (ctrlPorts.size() < 3)
    {
        LOG(ERROR) << "need at least 3 controller ports";
        return 1;
    }
    if (kvPorts.empty())
    {
        LOG(ERROR) << "need at least 1 kv port";
        return 1;
    }
    if (kvPorts.size() != gids.size())
    {
        LOG(ERROR) << "kv_ports size must match group_gids size";
        return 1;
    }

    std::vector<Host> ctrlHosts;
    ctrlHosts.reserve(ctrlPorts.size());
    for (int p : ctrlPorts)
    {
        ctrlHosts.push_back(makeHost("127.0.0.1", p));
    }

    std::vector<Host> kvHosts;
    kvHosts.reserve(kvPorts.size());
    for (int p : kvPorts)
    {
        kvHosts.push_back(makeHost("127.0.0.1", p));
    }

    std::vector<ProcessManager> ctrls;
    std::vector<ProcessManager> kvs;

    for (size_t i = 0; i < ctrlHosts.size(); i++)
    {
        auto peers = ctrlHosts;
        Host me = ctrlHosts[i];
        peers.erase(peers.begin() + static_cast<long>(i));
        std::string dirName = fmt::format("{}/ShardCtrler{}", FLAGS_cluster_log_dir, i + 1);
        if (!ensureDir(dirName))
        {
            LOG(ERROR) << "create dir failed: " << dirName;
            return 1;
        }

        using TProcessPtr = std::shared_ptr<apache::thrift::TProcessor>;
        ctrls.emplace_back(me, dirName, [peers, me, dirName]() -> TProcessPtr
                           {
            auto handler = std::make_shared<ShardCtrler>(peers, me, dirName, FLAGS_shard_num);
            return TProcessPtr(new ShardCtrlerProcessor(handler)); });
    }

    for (auto &ctrl : ctrls)
    {
        ctrl.start();
    }
    std::this_thread::sleep_for(MAX_ELECTION_TIMEOUT + std::chrono::milliseconds(200));

    ShardctrlerClerk clerk(ctrlHosts);
    {
        bool ready = false;
        for (int t = 0; t < 80; t++)
        {
            QueryReply qrep;
            QueryArgs qargs;
            qargs.configNum = LATEST_CONFIG_NUM;
            clerk.query(qrep, qargs);
            if (qrep.code == ErrorCode::SUCCEED)
            {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!ready)
        {
            LOG(ERROR) << "shard controller not ready in time";
            return 1;
        }
    }

    for (size_t i = 0; i < kvHosts.size(); i++)
    {
        Host me = kvHosts[i];
        std::string dirName = fmt::format("{}/ShardKV{}", FLAGS_cluster_log_dir, i + 1);
        if (!ensureDir(dirName))
        {
            LOG(ERROR) << "create dir failed: " << dirName;
            return 1;
        }

        auto ctrlerHosts = ctrlHosts;
        using TProcessPtr = std::shared_ptr<apache::thrift::TProcessor>;
        kvs.emplace_back(me, dirName, [ctrlerHosts, me]() mutable -> TProcessPtr
                         {
            auto handler = std::make_shared<ShardKV>(ctrlerHosts, me);
            return TProcessPtr(new ShardKVRaftProcessor(handler)); });
    }

    for (auto &kv : kvs)
    {
        kv.start();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    for (size_t i = 0; i < kvHosts.size(); i++)
    {
        JoinArgs args;
        args.servers[gids[i]] = {kvHosts[i]};

        bool ok = false;
        for (int t = 0; t < 80; t++)
        {
            JoinReply rep;
            clerk.join(rep, args);
            if (rep.code == ErrorCode::SUCCEED)
            {
                ok = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!ok)
        {
            LOG(ERROR) << "join group failed, gid=" << gids[i] << ", host=" << kvHosts[i].ip << ":" << kvHosts[i].port;
            return 1;
        }
    }

    LOG(INFO) << "One-click backend started.";
    LOG(INFO) << "ShardCtrler hosts: " << FLAGS_ctrl_ports;
    LOG(INFO) << "ShardKV hosts: " << FLAGS_kv_ports;
    LOG(INFO) << "Use redisproxy --ctrler_hosts=" << FLAGS_ctrl_ports;

    while (!g_stop.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    LOG(INFO) << "Stopping backend launcher...";
    google::ShutdownGoogleLogging();
    return 0;
}
