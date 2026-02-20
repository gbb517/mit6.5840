#include <redisproxy/RedisProxyServer.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <limits>
#include <set>
#include <thread>
#include <unordered_set>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <glog/logging.h>
#include <shardkv/ShardCtrler.h>

namespace
{
    RedisProxyServer::RespValue makeSimple(const std::string &s)
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::SIMPLE_STRING;
        r.str = s;
        return r;
    }

    RedisProxyServer::RespValue makeBulk(const std::string &s)
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::BULK_STRING;
        r.str = s;
        return r;
    }

    RedisProxyServer::RespValue makeInt(int64_t x)
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::INTEGER;
        r.integer = x;
        return r;
    }

    RedisProxyServer::RespValue makeErr(const std::string &e)
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::ERROR;
        r.str = e;
        return r;
    }

    RedisProxyServer::RespValue makeNil()
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::NIL;
        return r;
    }

    RedisProxyServer::RespValue makeArray(const std::vector<RedisProxyServer::RespValue> &arr)
    {
        RedisProxyServer::RespValue r;
        r.type = RedisProxyServer::RespValue::Type::ARRAY;
        r.array = arr;
        return r;
    }
} // namespace

RedisProxyServer::RedisProxyServer(std::vector<Host> ctrlerHosts, int port)
    : ctrlerHosts_(std::move(ctrlerHosts)), clerk_(ctrlerHosts_), port_(port)
{
}

RedisProxyServer::~RedisProxyServer()
{
    stop();
}

void RedisProxyServer::run()
{
    serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd_ < 0)
    {
        LOG(FATAL) << "socket failed: " << strerror(errno);
    }

    int opt = 1;
    setsockopt(serverFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverFd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        LOG(FATAL) << "bind failed: " << strerror(errno);
    }
    if (listen(serverFd_, 128) < 0)
    {
        LOG(FATAL) << "listen failed: " << strerror(errno);
    }

    LOG(INFO) << "RedisProxyServer listening on port " << port_;
    while (!stopped_.load())
    {
        int clientFd = accept(serverFd_, nullptr, nullptr);
        if (clientFd < 0)
        {
            if (stopped_.load())
            {
                break;
            }
            continue;
        }

        std::thread worker([this, clientFd]()
                           { handleClient(clientFd); });
        worker.detach();
    }
}

void RedisProxyServer::stop()
{
    stopped_ = true;
    if (serverFd_ >= 0)
    {
        close(serverFd_);
        serverFd_ = -1;
    }
}

void RedisProxyServer::handleClient(int clientFd)
{
    while (!stopped_.load())
    {
        RespValue req;
        if (!readRespValue(clientFd, req))
        {
            break;
        }
        auto resp = execute(req);
        auto out = encodeResp(resp);
        if (send(clientFd, out.data(), out.size(), 0) < 0)
        {
            break;
        }
    }
    close(clientFd);
}

bool RedisProxyServer::readLine(int fd, std::string &line)
{
    line.clear();
    char ch;
    while (true)
    {
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n <= 0)
        {
            return false;
        }
        if (ch == '\r')
        {
            ssize_t m = recv(fd, &ch, 1, 0);
            if (m <= 0 || ch != '\n')
            {
                return false;
            }
            return true;
        }
        line.push_back(ch);
    }
}

bool RedisProxyServer::readNBytes(int fd, int n, std::string &out)
{
    out.clear();
    out.resize(n);
    int got = 0;
    while (got < n)
    {
        ssize_t m = recv(fd, &out[got], n - got, 0);
        if (m <= 0)
        {
            return false;
        }
        got += static_cast<int>(m);
    }
    return true;
}

bool RedisProxyServer::readRespValue(int fd, RespValue &out)
{
    char prefix;
    ssize_t n = recv(fd, &prefix, 1, 0);
    if (n <= 0)
    {
        return false;
    }

    std::string line;
    switch (prefix)
    {
    case '*':
    {
        if (!readLine(fd, line))
            return false;
        int count = std::stoi(line);
        out.type = RespValue::Type::ARRAY;
        out.array.clear();
        for (int i = 0; i < count; i++)
        {
            RespValue elem;
            if (!readRespValue(fd, elem))
                return false;
            out.array.push_back(std::move(elem));
        }
        return true;
    }
    case '$':
    {
        if (!readLine(fd, line))
            return false;
        int len = std::stoi(line);
        if (len < 0)
        {
            out.type = RespValue::Type::NIL;
            out.str.clear();
            return true;
        }
        std::string data;
        if (!readNBytes(fd, len, data))
            return false;
        char crlf[2];
        if (recv(fd, crlf, 2, 0) != 2 || crlf[0] != '\r' || crlf[1] != '\n')
            return false;
        out.type = RespValue::Type::BULK_STRING;
        out.str = std::move(data);
        return true;
    }
    case '+':
        if (!readLine(fd, line))
            return false;
        out.type = RespValue::Type::SIMPLE_STRING;
        out.str = std::move(line);
        return true;
    case ':':
        if (!readLine(fd, line))
            return false;
        out.type = RespValue::Type::INTEGER;
        out.integer = std::stoll(line);
        return true;
    default:
        return false;
    }
}

RedisProxyServer::RespValue RedisProxyServer::execute(const RespValue &request)
{
    if (request.type != RespValue::Type::ARRAY || request.array.empty())
    {
        return makeErr("ERR invalid command");
    }

    std::vector<std::string> argv;
    argv.reserve(request.array.size());
    for (const auto &arg : request.array)
    {
        if (arg.type != RespValue::Type::BULK_STRING && arg.type != RespValue::Type::SIMPLE_STRING)
        {
            return makeErr("ERR invalid argument");
        }
        argv.push_back(arg.str);
    }
    return executeCommand(argv);
}

RedisProxyServer::RespValue RedisProxyServer::executeCommand(const std::vector<std::string> &argv)
{
    if (argv.empty())
    {
        return makeErr("ERR empty command");
    }

    auto cmd = upper(argv[0]);

    if (cmd == "PING")
        return cmdPing(argv);
    if (cmd == "SET")
        return cmdSet(argv);
    if (cmd == "GET")
        return cmdGet(argv);
    if (cmd == "INCR")
        return cmdIncr(argv);
    if (cmd == "DEL")
        return cmdDel(argv);
    if (cmd == "SCAN")
        return cmdScan(argv);
    if (cmd == "EXISTS")
        return cmdExists(argv);
    if (cmd == "TYPE")
        return cmdType(argv);

    if (cmd == "HSET")
        return cmdHSet(argv);
    if (cmd == "HGET")
        return cmdHGet(argv);
    if (cmd == "HDEL")
        return cmdHDel(argv);
    if (cmd == "HEXISTS")
        return cmdHExists(argv);
    if (cmd == "HGETALL")
        return cmdHGetAll(argv);

    if (cmd == "SADD")
        return cmdSAdd(argv);
    if (cmd == "SREM")
        return cmdSRem(argv);
    if (cmd == "SISMEMBER")
        return cmdSIsMember(argv);
    if (cmd == "SMEMBERS")
        return cmdSMembers(argv);
    if (cmd == "SCARD")
        return cmdSCard(argv);

    if (cmd == "ZADD")
        return cmdZAdd(argv);
    if (cmd == "ZSCORE")
        return cmdZScore(argv);
    if (cmd == "ZRANK")
        return cmdZRank(argv);
    if (cmd == "ZRANGE")
        return cmdZRange(argv);
    if (cmd == "ZREM")
        return cmdZRem(argv);

    if (cmd == "LPUSH")
        return cmdLPush(argv);
    if (cmd == "RPUSH")
        return cmdRPush(argv);
    if (cmd == "LPOP")
        return cmdLPop(argv);
    if (cmd == "RPOP")
        return cmdRPop(argv);
    if (cmd == "LLEN")
        return cmdLLen(argv);
    if (cmd == "LRANGE")
        return cmdLRange(argv);

    return makeErr("ERR unknown command");
}

RedisProxyServer::RespValue RedisProxyServer::cmdPing(const std::vector<std::string> &argv)
{
    if (argv.size() == 1)
        return makeSimple("PONG");
    if (argv.size() == 2)
        return makeBulk(argv[1]);
    return makeErr("ERR wrong number of arguments for 'ping'");
}

RedisProxyServer::RespValue RedisProxyServer::cmdSet(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'set'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "string", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    if (!kvSet(strKey(argv[1]), argv[2]))
        return makeErr("ERR set failed");

    return makeSimple("OK");
}

RedisProxyServer::RespValue RedisProxyServer::cmdGet(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'get'");

    std::string t;
    bool typeExists = false;
    if (!getTypeMeta(argv[1], t, typeExists))
        return makeErr("ERR get type failed");
    if (!typeExists)
        return makeNil();
    if (t != "string")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string value;
    bool exists = false;
    if (!kvGet(strKey(argv[1]), value, exists))
        return makeErr("ERR get failed");
    if (!exists)
        return makeNil();
    return makeBulk(value);
}

RedisProxyServer::RespValue RedisProxyServer::cmdIncr(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'incr'");

    const auto &key = argv[1];
    std::string t;
    bool typeExists = false;
    if (!getTypeMeta(key, t, typeExists))
        return makeErr("ERR backend unavailable");

    if (!typeExists)
    {
        if (!setTypeMeta(key, "string"))
            return makeErr("ERR backend unavailable");
        if (!kvSet(strKey(key), "1"))
            return makeErr("ERR backend unavailable");
        return makeInt(1);
    }

    if (t != "string")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string value;
    bool exists = false;
    if (!kvGet(strKey(key), value, exists))
        return makeErr("ERR backend unavailable");

    int64_t current = 0;
    if (exists)
    {
        if (!parseInt64(value, current))
            return makeErr("ERR value is not an integer or out of range");
    }

    if (current == std::numeric_limits<int64_t>::max())
        return makeErr("ERR value is not an integer or out of range");

    int64_t next = current + 1;
    if (!kvSet(strKey(key), std::to_string(next)))
        return makeErr("ERR backend unavailable");

    return makeInt(next);
}

RedisProxyServer::RespValue RedisProxyServer::cmdDel(const std::vector<std::string> &argv)
{
    if (argv.size() < 2)
        return makeErr("ERR wrong number of arguments for 'del'");

    int deleted = 0;
    for (size_t i = 1; i < argv.size(); i++)
    {
        deleted += delLogicalKey(argv[i]);
    }
    return makeInt(deleted);
}

RedisProxyServer::RespValue RedisProxyServer::cmdScan(const std::vector<std::string> &argv)
{
    if (argv.size() < 2)
        return makeErr("ERR wrong number of arguments for 'scan'");

    std::string cursorArg = argv[1];
    std::string cursorInternal = cursorArg == "0" ? "" : metaKey(cursorArg);
    std::string matchPrefix;
    int count = 10;

    for (size_t i = 2; i + 1 < argv.size(); i += 2)
    {
        auto key = upper(argv[i]);
        if (key == "MATCH")
        {
            matchPrefix = argv[i + 1];
            auto starPos = matchPrefix.find('*');
            if (starPos != std::string::npos)
                matchPrefix = matchPrefix.substr(0, starPos);
        }
        else if (key == "COUNT")
        {
            int64_t v;
            if (!parseInt64(argv[i + 1], v) || v <= 0)
                return makeErr("ERR value is not an integer or out of range");
            count = static_cast<int>(v);
        }
    }

    std::vector<std::pair<std::string, std::string>> items;
    std::string nextCursor;
    bool done = true;
    if (!kvPrefixScan(metaKey(matchPrefix), cursorInternal, count, items, nextCursor, done))
        return makeErr("ERR scan failed");

    std::vector<RespValue> keyArr;
    for (const auto &it : items)
    {
        if (it.first.rfind("meta:", 0) == 0)
            keyArr.push_back(makeBulk(it.first.substr(5)));
    }

    std::string outCursor = "0";
    if (!done && nextCursor.rfind("meta:", 0) == 0)
        outCursor = nextCursor.substr(5);

    return makeArray({makeBulk(outCursor), makeArray(keyArr)});
}

RedisProxyServer::RespValue RedisProxyServer::cmdExists(const std::vector<std::string> &argv)
{
    if (argv.size() < 2)
        return makeErr("ERR wrong number of arguments for 'exists'");

    int existsCnt = 0;
    for (size_t i = 1; i < argv.size(); i++)
    {
        std::string t;
        bool ex = false;
        if (!getTypeMeta(argv[i], t, ex))
            return makeErr("ERR exists failed");
        if (ex)
            existsCnt++;
    }
    return makeInt(existsCnt);
}

RedisProxyServer::RespValue RedisProxyServer::cmdType(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'type'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR type failed");
    if (!ex)
        return makeSimple("none");
    return makeSimple(t);
}

RedisProxyServer::RespValue RedisProxyServer::cmdHSet(const std::vector<std::string> &argv)
{
    if (argv.size() < 4 || (argv.size() % 2) != 0)
        return makeErr("ERR wrong number of arguments for 'hset'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "hash", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    std::vector<std::string> fields;
    std::string raw;
    bool exists = false;
    if (!kvGet(hashFieldsKey(argv[1]), raw, exists))
        return makeErr("ERR hset failed");
    if (exists && !decodeList(raw, fields))
        return makeErr("ERR hset decode failed");

    std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
    int added = 0;
    for (size_t i = 2; i < argv.size(); i += 2)
    {
        const auto &field = argv[i];
        const auto &val = argv[i + 1];
        if (fieldSet.insert(field).second)
        {
            fields.push_back(field);
            added++;
        }
        if (!kvSet(hashFieldKey(argv[1], field), val))
            return makeErr("ERR hset write failed");
    }

    if (!kvSet(hashFieldsKey(argv[1]), encodeList(fields)))
        return makeErr("ERR hset meta failed");

    return makeInt(added);
}

RedisProxyServer::RespValue RedisProxyServer::cmdHGet(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'hget'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR hget failed");
    if (!ex)
        return makeNil();
    if (t != "hash")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string val;
    bool fex = false;
    if (!kvGet(hashFieldKey(argv[1], argv[2]), val, fex))
        return makeErr("ERR hget failed");
    if (!fex)
        return makeNil();
    return makeBulk(val);
}

RedisProxyServer::RespValue RedisProxyServer::cmdHDel(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'hdel'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR hdel failed");
    if (!ex)
        return makeInt(0);
    if (t != "hash")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::vector<std::string> fields;
    std::string raw;
    bool metaEx = false;
    if (!kvGet(hashFieldsKey(argv[1]), raw, metaEx))
        return makeErr("ERR hdel failed");
    if (metaEx && !decodeList(raw, fields))
        return makeErr("ERR hdel decode failed");

    std::unordered_set<std::string> toDel;
    for (size_t i = 2; i < argv.size(); i++)
        toDel.insert(argv[i]);

    int deleted = 0;
    std::vector<std::string> keep;
    for (const auto &f : fields)
    {
        if (toDel.find(f) != toDel.end())
        {
            deleted += kvDel(hashFieldKey(argv[1], f));
        }
        else
        {
            keep.push_back(f);
        }
    }

    if (!kvSet(hashFieldsKey(argv[1]), encodeList(keep)))
        return makeErr("ERR hdel meta failed");

    return makeInt(deleted);
}

RedisProxyServer::RespValue RedisProxyServer::cmdHExists(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'hexists'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR hexists failed");
    if (!ex)
        return makeInt(0);
    if (t != "hash")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string val;
    bool fex = false;
    if (!kvGet(hashFieldKey(argv[1], argv[2]), val, fex))
        return makeErr("ERR hexists failed");
    return makeInt(fex ? 1 : 0);
}

RedisProxyServer::RespValue RedisProxyServer::cmdHGetAll(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'hgetall'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR hgetall failed");
    if (!ex)
        return makeArray({});
    if (t != "hash")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool m = false;
    if (!kvGet(hashFieldsKey(argv[1]), raw, m))
        return makeErr("ERR hgetall failed");

    std::vector<std::string> fields;
    if (m && !decodeList(raw, fields))
        return makeErr("ERR hgetall decode failed");

    std::vector<RespValue> out;
    for (const auto &f : fields)
    {
        std::string val;
        bool fex = false;
        if (!kvGet(hashFieldKey(argv[1], f), val, fex))
            return makeErr("ERR hgetall field failed");
        if (!fex)
            continue;
        out.push_back(makeBulk(f));
        out.push_back(makeBulk(val));
    }
    return makeArray(out);
}

RedisProxyServer::RespValue RedisProxyServer::cmdSAdd(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'sadd'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "set", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    std::string raw;
    bool ex = false;
    if (!kvGet(setMembersKey(argv[1]), raw, ex))
        return makeErr("ERR sadd failed");

    std::vector<std::string> members;
    if (ex && !decodeList(raw, members))
        return makeErr("ERR sadd decode failed");
    std::unordered_set<std::string> memberSet(members.begin(), members.end());

    int added = 0;
    for (size_t i = 2; i < argv.size(); i++)
    {
        if (memberSet.insert(argv[i]).second)
        {
            members.push_back(argv[i]);
            added++;
        }
        if (!kvSet(setMemberKey(argv[1], argv[i]), "1"))
            return makeErr("ERR sadd write failed");
    }

    if (!kvSet(setMembersKey(argv[1]), encodeList(members)))
        return makeErr("ERR sadd meta failed");

    return makeInt(added);
}

RedisProxyServer::RespValue RedisProxyServer::cmdSRem(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'srem'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR srem failed");
    if (!ex)
        return makeInt(0);
    if (t != "set")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool mex = false;
    if (!kvGet(setMembersKey(argv[1]), raw, mex))
        return makeErr("ERR srem failed");

    std::vector<std::string> members;
    if (mex && !decodeList(raw, members))
        return makeErr("ERR srem decode failed");

    std::unordered_set<std::string> removeSet;
    for (size_t i = 2; i < argv.size(); i++)
        removeSet.insert(argv[i]);

    int removed = 0;
    std::vector<std::string> keep;
    for (const auto &m : members)
    {
        if (removeSet.find(m) != removeSet.end())
        {
            removed += kvDel(setMemberKey(argv[1], m));
        }
        else
        {
            keep.push_back(m);
        }
    }

    if (!kvSet(setMembersKey(argv[1]), encodeList(keep)))
        return makeErr("ERR srem meta failed");

    return makeInt(removed);
}

RedisProxyServer::RespValue RedisProxyServer::cmdSIsMember(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'sismember'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR sismember failed");
    if (!ex)
        return makeInt(0);
    if (t != "set")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string val;
    bool m = false;
    if (!kvGet(setMemberKey(argv[1], argv[2]), val, m))
        return makeErr("ERR sismember failed");
    return makeInt(m ? 1 : 0);
}

RedisProxyServer::RespValue RedisProxyServer::cmdSMembers(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'smembers'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR smembers failed");
    if (!ex)
        return makeArray({});
    if (t != "set")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool mex = false;
    if (!kvGet(setMembersKey(argv[1]), raw, mex))
        return makeErr("ERR smembers failed");

    std::vector<std::string> members;
    if (mex && !decodeList(raw, members))
        return makeErr("ERR smembers decode failed");

    std::vector<RespValue> out;
    for (const auto &m : members)
        out.push_back(makeBulk(m));
    return makeArray(out);
}

RedisProxyServer::RespValue RedisProxyServer::cmdSCard(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'scard'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR scard failed");
    if (!ex)
        return makeInt(0);
    if (t != "set")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool mex = false;
    if (!kvGet(setMembersKey(argv[1]), raw, mex))
        return makeErr("ERR scard failed");

    std::vector<std::string> members;
    if (mex && !decodeList(raw, members))
        return makeErr("ERR scard decode failed");
    return makeInt(static_cast<int>(members.size()));
}

RedisProxyServer::RespValue RedisProxyServer::cmdZAdd(const std::vector<std::string> &argv)
{
    if (argv.size() < 4 || (argv.size() % 2) != 0)
        return makeErr("ERR wrong number of arguments for 'zadd'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "zset", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    std::vector<std::string> members;
    std::string mraw;
    bool mex = false;
    if (!kvGet(zsetMembersKey(argv[1]), mraw, mex))
        return makeErr("ERR zadd failed");
    if (mex && !decodeList(mraw, members))
        return makeErr("ERR zadd members decode failed");

    std::vector<std::pair<int64_t, std::string>> order;
    std::string oraw;
    bool oex = false;
    if (!kvGet(zsetOrderKey(argv[1]), oraw, oex))
        return makeErr("ERR zadd failed");
    if (oex && !decodeZOrder(oraw, order))
        return makeErr("ERR zadd order decode failed");

    std::unordered_set<std::string> memberSet(members.begin(), members.end());
    int added = 0;

    for (size_t i = 2; i < argv.size(); i += 2)
    {
        int64_t score;
        if (!parseInt64(argv[i], score))
            return makeErr("ERR value is not an integer or out of range");
        const auto &member = argv[i + 1];

        if (memberSet.insert(member).second)
        {
            members.push_back(member);
            added++;
        }

        order.erase(std::remove_if(order.begin(), order.end(), [&member](const std::pair<int64_t, std::string> &it)
                                   { return it.second == member; }),
                    order.end());
        order.emplace_back(score, member);

        if (!kvSet(zsetMemberScoreKey(argv[1], member), std::to_string(score)))
            return makeErr("ERR zadd write failed");
    }

    std::sort(order.begin(), order.end(), [](const std::pair<int64_t, std::string> &a, const std::pair<int64_t, std::string> &b)
              {
                  if (a.first != b.first)
                      return a.first < b.first;
                  return a.second < b.second; });

    if (!kvSet(zsetMembersKey(argv[1]), encodeList(members)))
        return makeErr("ERR zadd members failed");
    if (!kvSet(zsetOrderKey(argv[1]), encodeZOrder(order)))
        return makeErr("ERR zadd order failed");

    return makeInt(added);
}

RedisProxyServer::RespValue RedisProxyServer::cmdZScore(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'zscore'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR zscore failed");
    if (!ex)
        return makeNil();
    if (t != "zset")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string score;
    bool sex = false;
    if (!kvGet(zsetMemberScoreKey(argv[1], argv[2]), score, sex))
        return makeErr("ERR zscore failed");
    if (!sex)
        return makeNil();
    return makeBulk(score);
}

RedisProxyServer::RespValue RedisProxyServer::cmdZRank(const std::vector<std::string> &argv)
{
    if (argv.size() != 3)
        return makeErr("ERR wrong number of arguments for 'zrank'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR zrank failed");
    if (!ex)
        return makeNil();
    if (t != "zset")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool oex = false;
    if (!kvGet(zsetOrderKey(argv[1]), raw, oex))
        return makeErr("ERR zrank failed");
    if (!oex)
        return makeNil();

    std::vector<std::pair<int64_t, std::string>> order;
    if (!decodeZOrder(raw, order))
        return makeErr("ERR zrank decode failed");

    for (size_t i = 0; i < order.size(); i++)
    {
        if (order[i].second == argv[2])
            return makeInt(static_cast<int64_t>(i));
    }
    return makeNil();
}

RedisProxyServer::RespValue RedisProxyServer::cmdZRange(const std::vector<std::string> &argv)
{
    if (argv.size() < 4)
        return makeErr("ERR wrong number of arguments for 'zrange'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR zrange failed");
    if (!ex)
        return makeArray({});
    if (t != "zset")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    int64_t start, stop;
    if (!parseInt64(argv[2], start) || !parseInt64(argv[3], stop))
        return makeErr("ERR value is not an integer or out of range");

    bool withScores = argv.size() >= 5 && upper(argv[4]) == "WITHSCORES";

    std::string raw;
    bool oex = false;
    if (!kvGet(zsetOrderKey(argv[1]), raw, oex))
        return makeErr("ERR zrange failed");
    if (!oex)
        return makeArray({});

    std::vector<std::pair<int64_t, std::string>> order;
    if (!decodeZOrder(raw, order))
        return makeErr("ERR zrange decode failed");

    int64_t n = static_cast<int64_t>(order.size());
    if (start < 0)
        start += n;
    if (stop < 0)
        stop += n;
    if (start < 0)
        start = 0;
    if (stop >= n)
        stop = n - 1;

    if (n == 0 || start > stop || start >= n)
        return makeArray({});

    std::vector<RespValue> out;
    for (int64_t i = start; i <= stop; i++)
    {
        out.push_back(makeBulk(order[i].second));
        if (withScores)
            out.push_back(makeBulk(std::to_string(order[i].first)));
    }
    return makeArray(out);
}

RedisProxyServer::RespValue RedisProxyServer::cmdZRem(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'zrem'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR zrem failed");
    if (!ex)
        return makeInt(0);
    if (t != "zset")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::unordered_set<std::string> rmSet;
    for (size_t i = 2; i < argv.size(); i++)
        rmSet.insert(argv[i]);

    std::string mraw;
    bool mex = false;
    if (!kvGet(zsetMembersKey(argv[1]), mraw, mex))
        return makeErr("ERR zrem failed");

    std::vector<std::string> members;
    if (mex && !decodeList(mraw, members))
        return makeErr("ERR zrem members decode failed");

    std::string oraw;
    bool oex = false;
    if (!kvGet(zsetOrderKey(argv[1]), oraw, oex))
        return makeErr("ERR zrem failed");

    std::vector<std::pair<int64_t, std::string>> order;
    if (oex && !decodeZOrder(oraw, order))
        return makeErr("ERR zrem order decode failed");

    int removed = 0;
    std::vector<std::string> keepMembers;
    for (const auto &m : members)
    {
        if (rmSet.find(m) != rmSet.end())
        {
            removed += kvDel(zsetMemberScoreKey(argv[1], m));
        }
        else
        {
            keepMembers.push_back(m);
        }
    }

    order.erase(std::remove_if(order.begin(), order.end(), [&rmSet](const std::pair<int64_t, std::string> &it)
                               { return rmSet.find(it.second) != rmSet.end(); }),
                order.end());

    if (!kvSet(zsetMembersKey(argv[1]), encodeList(keepMembers)))
        return makeErr("ERR zrem members failed");
    if (!kvSet(zsetOrderKey(argv[1]), encodeZOrder(order)))
        return makeErr("ERR zrem order failed");

    return makeInt(removed);
}

RedisProxyServer::RespValue RedisProxyServer::cmdLPush(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'lpush'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "list", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    std::string raw;
    bool ex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, ex))
        return makeErr("ERR lpush failed");

    std::vector<std::string> vals;
    if (ex && !decodeList(raw, vals))
        return makeErr("ERR lpush decode failed");

    for (size_t i = 2; i < argv.size(); i++)
    {
        vals.insert(vals.begin(), argv[i]);
    }

    if (!kvSet(listValuesKey(argv[1]), encodeList(vals)))
        return makeErr("ERR lpush write failed");

    return makeInt(static_cast<int64_t>(vals.size()));
}

RedisProxyServer::RespValue RedisProxyServer::cmdRPush(const std::vector<std::string> &argv)
{
    if (argv.size() < 3)
        return makeErr("ERR wrong number of arguments for 'rpush'");

    bool created = false;
    bool backendError = false;
    if (!ensureTypeForWrite(argv[1], "list", created, backendError))
    {
        if (backendError)
            return makeErr("ERR backend unavailable");
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    std::string raw;
    bool ex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, ex))
        return makeErr("ERR rpush failed");

    std::vector<std::string> vals;
    if (ex && !decodeList(raw, vals))
        return makeErr("ERR rpush decode failed");

    for (size_t i = 2; i < argv.size(); i++)
    {
        vals.push_back(argv[i]);
    }

    if (!kvSet(listValuesKey(argv[1]), encodeList(vals)))
        return makeErr("ERR rpush write failed");

    return makeInt(static_cast<int64_t>(vals.size()));
}

RedisProxyServer::RespValue RedisProxyServer::cmdLPop(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'lpop'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR lpop failed");
    if (!ex)
        return makeNil();
    if (t != "list")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool vex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, vex))
        return makeErr("ERR lpop failed");
    if (!vex)
        return makeNil();

    std::vector<std::string> vals;
    if (!decodeList(raw, vals))
        return makeErr("ERR lpop decode failed");
    if (vals.empty())
        return makeNil();

    std::string front = vals.front();
    vals.erase(vals.begin());
    if (!kvSet(listValuesKey(argv[1]), encodeList(vals)))
        return makeErr("ERR lpop write failed");

    return makeBulk(front);
}

RedisProxyServer::RespValue RedisProxyServer::cmdRPop(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'rpop'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR rpop failed");
    if (!ex)
        return makeNil();
    if (t != "list")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool vex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, vex))
        return makeErr("ERR rpop failed");
    if (!vex)
        return makeNil();

    std::vector<std::string> vals;
    if (!decodeList(raw, vals))
        return makeErr("ERR rpop decode failed");
    if (vals.empty())
        return makeNil();

    std::string back = vals.back();
    vals.pop_back();
    if (!kvSet(listValuesKey(argv[1]), encodeList(vals)))
        return makeErr("ERR rpop write failed");

    return makeBulk(back);
}

RedisProxyServer::RespValue RedisProxyServer::cmdLLen(const std::vector<std::string> &argv)
{
    if (argv.size() != 2)
        return makeErr("ERR wrong number of arguments for 'llen'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR llen failed");
    if (!ex)
        return makeInt(0);
    if (t != "list")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    std::string raw;
    bool vex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, vex))
        return makeErr("ERR llen failed");

    std::vector<std::string> vals;
    if (vex && !decodeList(raw, vals))
        return makeErr("ERR llen decode failed");

    return makeInt(static_cast<int64_t>(vals.size()));
}

RedisProxyServer::RespValue RedisProxyServer::cmdLRange(const std::vector<std::string> &argv)
{
    if (argv.size() != 4)
        return makeErr("ERR wrong number of arguments for 'lrange'");

    std::string t;
    bool ex = false;
    if (!getTypeMeta(argv[1], t, ex))
        return makeErr("ERR lrange failed");
    if (!ex)
        return makeArray({});
    if (t != "list")
        return makeErr("WRONGTYPE Operation against a key holding the wrong kind of value");

    int64_t start, stop;
    if (!parseInt64(argv[2], start) || !parseInt64(argv[3], stop))
        return makeErr("ERR value is not an integer or out of range");

    std::string raw;
    bool vex = false;
    if (!kvGet(listValuesKey(argv[1]), raw, vex))
        return makeErr("ERR lrange failed");
    if (!vex)
        return makeArray({});

    std::vector<std::string> vals;
    if (!decodeList(raw, vals))
        return makeErr("ERR lrange decode failed");

    int64_t n = static_cast<int64_t>(vals.size());
    if (start < 0)
        start += n;
    if (stop < 0)
        stop += n;
    if (start < 0)
        start = 0;
    if (stop >= n)
        stop = n - 1;

    if (n == 0 || start > stop || start >= n)
        return makeArray({});

    std::vector<RespValue> out;
    for (int64_t i = start; i <= stop; i++)
        out.push_back(makeBulk(vals[i]));
    return makeArray(out);
}

std::string RedisProxyServer::encodeResp(const RespValue &value) const
{
    switch (value.type)
    {
    case RespValue::Type::SIMPLE_STRING:
        return "+" + value.str + "\r\n";
    case RespValue::Type::ERROR:
        return "-" + value.str + "\r\n";
    case RespValue::Type::INTEGER:
        return ":" + std::to_string(value.integer) + "\r\n";
    case RespValue::Type::BULK_STRING:
        return "$" + std::to_string(value.str.size()) + "\r\n" + value.str + "\r\n";
    case RespValue::Type::NIL:
        return "$-1\r\n";
    case RespValue::Type::ARRAY:
    {
        std::string out = "*" + std::to_string(value.array.size()) + "\r\n";
        for (const auto &elem : value.array)
            out += encodeResp(elem);
        return out;
    }
    }
    return "-ERR internal\r\n";
}

std::string RedisProxyServer::upper(std::string s) const
{
    std::transform(s.begin(), s.end(), s.begin(), [](char ch)
                   { return static_cast<char>(std::toupper(static_cast<unsigned char>(ch))); });
    return s;
}

bool RedisProxyServer::kvSet(const std::string &key, const std::string &value)
{
    if (backendUnavailableFast())
        return false;

    PutAppendParams pp;
    pp.key = key;
    pp.value = value;
    pp.op = PutOp::PUT;
    PutAppendReply rep;
    clerk_.putAppend(rep, pp);
    if (rep.code != ErrorCode::SUCCEED)
    {
        markBackendDownFor(std::chrono::milliseconds(200));
    }
    return rep.code == ErrorCode::SUCCEED;
}

bool RedisProxyServer::kvGet(const std::string &key, std::string &value, bool &exists)
{
    if (backendUnavailableFast())
        return false;

    GetParams gp;
    gp.key = key;
    GetReply rep;
    clerk_.get(rep, gp);
    if (rep.code == ErrorCode::SUCCEED)
    {
        value = rep.value;
        exists = true;
        return true;
    }
    if (rep.code == ErrorCode::ERR_NO_KEY)
    {
        exists = false;
        return true;
    }
    markBackendDownFor(std::chrono::milliseconds(200));
    return false;
}

int RedisProxyServer::kvDel(const std::string &key)
{
    if (backendUnavailableFast())
        return 0;

    DeleteParams dp;
    dp.key = key;
    DeleteReply dr;
    clerk_.del(dr, dp);
    if (dr.code == ErrorCode::SUCCEED)
        return dr.deleted;
    markBackendDownFor(std::chrono::milliseconds(200));
    return 0;
}

bool RedisProxyServer::kvPrefixScan(const std::string &prefix, std::string cursor, int count,
                                    std::vector<std::pair<std::string, std::string>> &items,
                                    std::string &nextCursor, bool &done)
{
    if (backendUnavailableFast())
        return false;

    PrefixScanParams sp;
    sp.prefix = prefix;
    sp.cursor = std::move(cursor);
    sp.count = count;

    PrefixScanReply sr;
    clerk_.prefixScan(sr, sp);
    if (sr.code != ErrorCode::SUCCEED)
    {
        markBackendDownFor(std::chrono::milliseconds(200));
        return false;
    }

    items.clear();
    for (const auto &kv : sr.kvs)
        items.emplace_back(kv.first, kv.second);

    nextCursor = sr.nextCursor;
    done = sr.done;
    return true;
}

int64_t RedisProxyServer::nowMs() const
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void RedisProxyServer::markBackendDownFor(std::chrono::milliseconds duration)
{
    const int64_t deadline = nowMs() + duration.count();
    int64_t cur = backendDownUntilMs_.load(std::memory_order_relaxed);
    while (cur < deadline &&
           !backendDownUntilMs_.compare_exchange_weak(cur, deadline, std::memory_order_relaxed))
    {
    }
}

bool RedisProxyServer::canDial(const Host &host, int timeoutMs) const
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return false;

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        close(fd);
        return false;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        close(fd);
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(host.port);
    if (inet_pton(AF_INET, host.ip.c_str(), &addr.sin_addr) != 1)
    {
        close(fd);
        return false;
    }

    int rc = connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (rc == 0)
    {
        close(fd);
        return true;
    }

    if (errno != EINPROGRESS)
    {
        close(fd);
        return false;
    }

    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);

    timeval tv{};
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;

    rc = select(fd + 1, nullptr, &wfds, nullptr, &tv);
    if (rc <= 0)
    {
        close(fd);
        return false;
    }

    int soError = 0;
    socklen_t len = sizeof(soError);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len) < 0)
    {
        close(fd);
        return false;
    }

    close(fd);
    return soError == 0;
}

bool RedisProxyServer::backendUnavailableFast()
{
    const int64_t now = nowMs();
    if (now < backendDownUntilMs_.load(std::memory_order_relaxed))
        return true;

    constexpr int kProbeTimeoutMs = 50;
    for (const auto &host : ctrlerHosts_)
    {
        if (canDial(host, kProbeTimeoutMs))
            return false;
    }

    markBackendDownFor(std::chrono::milliseconds(300));
    return true;
}

bool RedisProxyServer::getTypeMeta(const std::string &key, std::string &type, bool &exists)
{
    return kvGet(metaKey(key), type, exists);
}

bool RedisProxyServer::setTypeMeta(const std::string &key, const std::string &type)
{
    return kvSet(metaKey(key), type);
}

bool RedisProxyServer::ensureTypeForWrite(const std::string &key, const std::string &type, bool &created, bool &backendError)
{
    backendError = false;
    std::string oldType;
    bool exists = false;
    if (!getTypeMeta(key, oldType, exists))
    {
        backendError = true;
        return false;
    }

    if (!exists)
    {
        created = true;
        bool ok = setTypeMeta(key, type);
        if (!ok)
            backendError = true;
        return ok;
    }

    created = false;
    return oldType == type;
}

int RedisProxyServer::delLogicalKey(const std::string &key)
{
    std::string t;
    bool ex = false;
    if (!getTypeMeta(key, t, ex))
        return 0;

    if (!ex)
        return 0;

    if (t == "string")
    {
        kvDel(strKey(key));
    }
    else if (t == "hash")
    {
        std::string raw;
        bool mex = false;
        if (kvGet(hashFieldsKey(key), raw, mex) && mex)
        {
            std::vector<std::string> fields;
            if (decodeList(raw, fields))
            {
                for (const auto &f : fields)
                    kvDel(hashFieldKey(key, f));
            }
        }
        kvDel(hashFieldsKey(key));
    }
    else if (t == "set")
    {
        std::string raw;
        bool mex = false;
        if (kvGet(setMembersKey(key), raw, mex) && mex)
        {
            std::vector<std::string> members;
            if (decodeList(raw, members))
            {
                for (const auto &m : members)
                    kvDel(setMemberKey(key, m));
            }
        }
        kvDel(setMembersKey(key));
    }
    else if (t == "zset")
    {
        std::string raw;
        bool mex = false;
        if (kvGet(zsetMembersKey(key), raw, mex) && mex)
        {
            std::vector<std::string> members;
            if (decodeList(raw, members))
            {
                for (const auto &m : members)
                    kvDel(zsetMemberScoreKey(key, m));
            }
        }
        kvDel(zsetMembersKey(key));
        kvDel(zsetOrderKey(key));
    }
    else if (t == "list")
    {
        kvDel(listValuesKey(key));
    }

    kvDel(metaKey(key));
    return 1;
}

std::string RedisProxyServer::metaKey(const std::string &key) const { return "meta:" + key; }
std::string RedisProxyServer::strKey(const std::string &key) const { return "str:" + key; }
std::string RedisProxyServer::hashFieldsKey(const std::string &key) const { return "h:fields:" + key; }
std::string RedisProxyServer::hashFieldKey(const std::string &key, const std::string &field) const { return "h:f:" + key + ":" + field; }
std::string RedisProxyServer::setMembersKey(const std::string &key) const { return "s:members:" + key; }
std::string RedisProxyServer::setMemberKey(const std::string &key, const std::string &member) const { return "s:m:" + key + ":" + member; }
std::string RedisProxyServer::zsetMembersKey(const std::string &key) const { return "z:members:" + key; }
std::string RedisProxyServer::zsetOrderKey(const std::string &key) const { return "z:order:" + key; }
std::string RedisProxyServer::zsetMemberScoreKey(const std::string &key, const std::string &member) const { return "z:ms:" + key + ":" + member; }
std::string RedisProxyServer::listValuesKey(const std::string &key) const { return "l:vals:" + key; }

std::string RedisProxyServer::encodeList(const std::vector<std::string> &items) const
{
    std::string out;
    for (const auto &item : items)
    {
        out += std::to_string(item.size());
        out.push_back(':');
        out += item;
    }
    return out;
}

bool RedisProxyServer::decodeList(const std::string &raw, std::vector<std::string> &items) const
{
    items.clear();
    size_t pos = 0;
    while (pos < raw.size())
    {
        size_t colon = raw.find(':', pos);
        if (colon == std::string::npos)
            return false;

        int64_t len;
        if (!parseInt64(raw.substr(pos, colon - pos), len) || len < 0)
            return false;

        size_t start = colon + 1;
        size_t end = start + static_cast<size_t>(len);
        if (end > raw.size())
            return false;

        items.push_back(raw.substr(start, static_cast<size_t>(len)));
        pos = end;
    }
    return true;
}

std::string RedisProxyServer::zEntryJoin(int64_t score, const std::string &member) const
{
    return std::to_string(score) + "\x1f" + member;
}

bool RedisProxyServer::zEntrySplit(const std::string &entry, int64_t &score, std::string &member) const
{
    auto sep = entry.find('\x1f');
    if (sep == std::string::npos)
        return false;
    if (!parseInt64(entry.substr(0, sep), score))
        return false;
    member = entry.substr(sep + 1);
    return true;
}

std::string RedisProxyServer::encodeZOrder(const std::vector<std::pair<int64_t, std::string>> &items) const
{
    std::vector<std::string> rows;
    rows.reserve(items.size());
    for (const auto &it : items)
        rows.push_back(zEntryJoin(it.first, it.second));
    return encodeList(rows);
}

bool RedisProxyServer::decodeZOrder(const std::string &raw, std::vector<std::pair<int64_t, std::string>> &items) const
{
    items.clear();
    std::vector<std::string> rows;
    if (!decodeList(raw, rows))
        return false;

    for (const auto &row : rows)
    {
        int64_t score;
        std::string member;
        if (!zEntrySplit(row, score, member))
            return false;
        items.emplace_back(score, member);
    }
    return true;
}

bool RedisProxyServer::parseInt64(const std::string &s, int64_t &out) const
{
    try
    {
        size_t idx = 0;
        long long val = std::stoll(s, &idx, 10);
        if (idx != s.size())
            return false;
        out = static_cast<int64_t>(val);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

ShardId RedisProxyServer::key2shard(const std::string &key, int shardNum) const
{
    if (shardNum <= 0 || key.empty())
        return 0;
    return static_cast<unsigned char>(key[0]) % shardNum;
}

bool RedisProxyServer::queryLatestConfig(Config &cfg)
{
    ShardctrlerClerk c(ctrlerHosts_);
    QueryReply qrep;
    QueryArgs args;
    args.configNum = LATEST_CONFIG_NUM;
    c.query(qrep, args);
    if (qrep.code != ErrorCode::SUCCEED)
        return false;
    cfg = std::move(qrep.config);
    return true;
}
