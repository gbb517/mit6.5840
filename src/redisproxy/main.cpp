#include <redisproxy/RedisProxyServer.h>

#include <csignal>
#include <cerrno>
#include <iostream>
#include <memory>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(ctrler_hosts, "127.0.0.1:8101,127.0.0.1:8102,127.0.0.1:8103", "ShardCtrler hosts, comma separated ip:port");
DEFINE_int32(redis_port, 6379, "Redis compatible server port");
DEFINE_string(redisproxy_log_dir, "./logs", "glog output directory");

static std::unique_ptr<RedisProxyServer> g_server;

static std::vector<Host> parseHosts(const std::string &s)
{
    std::vector<Host> hosts;
    size_t start = 0;
    while (start < s.size())
    {
        size_t comma = s.find(',', start);
        std::string token = s.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        size_t colon = token.find(':');
        if (colon != std::string::npos)
        {
            Host h;
            h.ip = token.substr(0, colon);
            h.port = static_cast<int16_t>(std::stoi(token.substr(colon + 1)));
            hosts.push_back(h);
        }
        if (comma == std::string::npos)
            break;
        start = comma + 1;
    }
    return hosts;
}

static void handleSignal(int)
{
    if (g_server)
    {
        g_server->stop();
    }
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (mkdir(FLAGS_redisproxy_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0 && errno != EEXIST)
    {
        std::cerr << "failed to create log directory: " << FLAGS_redisproxy_log_dir << std::endl;
        return 1;
    }

    FLAGS_log_dir = FLAGS_redisproxy_log_dir;
    google::InitGoogleLogging(argv[0]);

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    auto hosts = parseHosts(FLAGS_ctrler_hosts);
    if (hosts.empty())
    {
        LOG(ERROR) << "no valid ctrler hosts";
        return 1;
    }

    g_server.reset(new RedisProxyServer(hosts, FLAGS_redis_port));
    g_server->run();

    google::ShutdownGoogleLogging();
    return 0;
}
