#include <string>
#include <unordered_set>
#include <fstream>

#include <shardkv/CtrlerArgs.hpp>
#include <shardkv/ShardCtrler.h>
#include <tools/Timer.hpp>
#include <tools/ToString.hpp>

#include <glog/logging.h>
#include <glog/stl_logging.h>

struct GInfo
{
    GID gid;
    int shardCnt;

    GInfo() = default;
    GInfo(GID id, int cnt)
        : gid(id), shardCnt(cnt)
    {
    }

    // 比较分片数量
    bool operator<(const GInfo &g) const
    {
        return shardCnt < g.shardCnt;
    }

public:
    struct Cmp
    {
        bool operator()(const GInfo &lhs, const GInfo &rhs)
        {
            if (lhs.shardCnt != rhs.shardCnt)
            {
                return lhs.shardCnt > rhs.shardCnt;
            }
            return lhs.gid > rhs.gid;
        }
    };
};

static std::ostream &operator<<(std::ostream &out, const GInfo &info)
{
    out << fmt::format("({}, {})", info.gid, info.shardCnt);
    return out;
}

ShardCtrler::ShardCtrler(std::vector<Host> peers, Host me, std::string persisterDir, int shards)
    : raft_(std::make_unique<RaftHandler>(peers, me, persisterDir, this)),
      lastApplyIndex_(0),
      lastApplyTerm_(0)
{
    Config init;
    init.configNum = 0;
    init.shard2gid = std::vector<GID>(shards, INVALID_GID); // 未分配
    configs_.push_back(std::move(init));

    auto snapshotPath = raft_->getPersister()->getLatestSnapshotPath();
    if (!snapshotPath.empty())
    {
        applySnapShot(snapshotPath);
    }
}

void ShardCtrler::join(JoinReply &_return, const JoinArgs &jargs)
{
    CtrlerArgs args(jargs);
    auto reply = sendArgsToRaft(args);
    _return.code = reply.code;
    _return.wrongLeader = reply.wrongLeader;
}

void ShardCtrler::leave(LeaveReply &_return, const LeaveArgs &largs)
{
    CtrlerArgs args(largs);
    auto reply = sendArgsToRaft(args);
    _return.code = reply.code;
    _return.wrongLeader = reply.wrongLeader;
}

void ShardCtrler::move(MoveReply &_return, const MoveArgs &margs)
{
    CtrlerArgs args(margs);
    auto reply = sendArgsToRaft(args);
    _return.code = reply.code;
    _return.wrongLeader = reply.wrongLeader;
}

void ShardCtrler::query(QueryReply &_return, const QueryArgs &qargs)
{
    CtrlerArgs args(qargs);
    auto reply = sendArgsToRaft(args);
    _return.code = reply.code;
    _return.wrongLeader = reply.wrongLeader;
    _return.config = std::move(reply.config);
}

void ShardCtrler::apply(ApplyMsg msg)
{
    Timer t(fmt::format("Apply cmd {}", msg.command), fmt::format("Finsh cmd {}", msg.command));
    std::lock_guard<std::mutex> guard(lock_);

    if (msg.commandIndex <= lastApplyIndex_)
    {
        return;
    }

    // 字符串命令->操作参数
    CtrlerArgs args = CtrlerArgs::deserialize(msg.command);
    Reply rep;
    switch (args.op())
    {
    case ShardCtrlerOP::JOIN:
    {
        JoinArgs join;
        args.copyTo(join);
        rep = handleJoin(join, msg);
    }
    break;

    case ShardCtrlerOP::LEAVE:
    {
        LeaveArgs leave;
        args.copyTo(leave);
        rep = handleLeave(leave, msg);
    }
    break;

    case ShardCtrlerOP::MOVE:
    {
        MoveArgs move;
        args.copyTo(move);
        rep = handleMove(move, msg);
    }
    break;

    case ShardCtrlerOP::QUERY:
    {
        QueryArgs query;
        args.copyTo(query);
        rep = handleQuery(query, msg);
    }
    break;

    default:
        LOG(FATAL) << "Unkonw args type!";
    }

    if (waits_.find(msg.commandIndex) != waits_.end())
    {
        waits_[msg.commandIndex].set_value(std::move(rep));
        waits_.erase(msg.commandIndex);
    }

    if (msg.commandIndex < lastApplyIndex_)
    {
        return;
    }
    lastApplyIndex_ = msg.commandIndex;
    lastApplyTerm_ = msg.commandTerm;
}

void ShardCtrler::startSnapShot(std::string filePath, std::function<void(LogId, TermId)> callback)
{
    LogId lastIndex;
    TermId lastTerm;
    {
        std::lock_guard<std::mutex> guard(lock_);
        lastIndex = lastApplyIndex_;
        lastTerm = lastApplyTerm_;

        std::ofstream ofs(filePath);
        ofs << lastTerm << ' ' << lastIndex << ' ' << configs_.size() << '\n';
        for (const auto &cfg : configs_)
        {
            ofs << cfg.configNum << ' ' << cfg.shard2gid.size() << '\n';
            for (GID gid : cfg.shard2gid)
            {
                ofs << gid << ' ';
            }
            ofs << '\n';

            ofs << cfg.gid2shards.size() << '\n';
            for (const auto &it : cfg.gid2shards)
            {
                ofs << it.first << ' ' << it.second.size();
                for (ShardId sid : it.second)
                {
                    ofs << ' ' << sid;
                }
                ofs << '\n';
            }

            ofs << cfg.groupHosts.size() << '\n';
            for (const auto &it : cfg.groupHosts)
            {
                ofs << it.first << ' ' << it.second.size();
                for (const auto &host : it.second)
                {
                    ofs << ' ' << host.ip << ' ' << host.port;
                }
                ofs << '\n';
            }
        }
    }
    callback(lastIndex, lastTerm);
}

void ShardCtrler::applySnapShot(std::string filePath)
{
    std::lock_guard<std::mutex> guard(lock_);
    std::ifstream ifs(filePath);
    if (!ifs.is_open())
    {
        LOG(ERROR) << "Failed to open snapshot file: " << filePath;
        return;
    }

    size_t cfgCount = 0;
    ifs >> lastApplyTerm_ >> lastApplyIndex_ >> cfgCount;
    configs_.clear();

    for (size_t i = 0; i < cfgCount; i++)
    {
        Config cfg;
        size_t shardCount = 0;
        ifs >> cfg.configNum >> shardCount;
        cfg.shard2gid.resize(shardCount);
        for (size_t j = 0; j < shardCount; j++)
        {
            ifs >> cfg.shard2gid[j];
        }

        size_t gid2ShardCount = 0;
        ifs >> gid2ShardCount;
        for (size_t j = 0; j < gid2ShardCount; j++)
        {
            GID gid;
            size_t sidCount = 0;
            ifs >> gid >> sidCount;
            std::set<ShardId> sids;
            for (size_t k = 0; k < sidCount; k++)
            {
                ShardId sid;
                ifs >> sid;
                sids.insert(sid);
            }
            cfg.gid2shards[gid] = std::move(sids);
        }

        size_t groupCount = 0;
        ifs >> groupCount;
        for (size_t j = 0; j < groupCount; j++)
        {
            GID gid;
            size_t hostCount = 0;
            ifs >> gid >> hostCount;
            std::vector<Host> hosts(hostCount);
            for (size_t k = 0; k < hostCount; k++)
            {
                ifs >> hosts[k].ip >> hosts[k].port;
            }
            cfg.groupHosts[gid] = std::move(hosts);
        }

        configs_.push_back(std::move(cfg));
    }

    if (configs_.empty())
    {
        Config init;
        init.configNum = 0;
        configs_.push_back(std::move(init));
    }

    for (auto it = waits_.begin(); it != waits_.end();)
    {
        if (it->first <= lastApplyIndex_)
        {
            Reply rep;
            rep.op = ShardCtrlerOP::QUERY;
            rep.code = ErrorCode::ERR_WRONG_LEADER;
            rep.wrongLeader = true;
            it->second.set_value(rep);
            it = waits_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

ShardCtrler::Reply ShardCtrler::handleJoin(const JoinArgs &join, const ApplyMsg &msg)
{
    // 根据最新配置进行操作
    auto newConfig = configs_.back();
    newConfig.configNum++;

    auto &gid2shards = newConfig.gid2shards; // gid2shards[gid]  = shardid
    auto &shard2gid = newConfig.shard2gid;   // shard2gid[shardid] = gid

    bool noGroup = gid2shards.empty();
    // 根据新的配置[新的gid配置]构造数据--添加新gid的对应分片配置
    for (auto &it : join.servers)
    {
        GID gid = it.first;
        // 待分配shard id
        newConfig.gid2shards[gid] = std::set<ShardId>();
        // 记录传入的服务器配置
        newConfig.groupHosts[gid] = it.second;
    }

    // 配置没有历史分片内容，给每个gid进行分配分片
    if (noGroup)
    {
        int shardNum = shard2gid.size();
        ShardId sid = 0;
        while (sid < shardNum)
        {
            // 轮询分配分片id
            for (auto &it : gid2shards)
            {
                if (sid >= shardNum)
                    break;

                it.second.insert(sid);
                shard2gid[sid] = it.first;
                sid++;
            }
        }
    }

    // 负载均衡
    // 历史分片配置
    std::vector<GInfo> info(gid2shards.size());
    {
        uint i = 0;
        for (auto it : newConfig.gid2shards)
        {
            info[i].gid = it.first;
            info[i].shardCnt = it.second.size();
            i++;
        }
    }
    sort(info.begin(), info.end());

    // adjust the balance util the difference bwtween max load and min load is 1
    while (true)
    {
        int diff = info.front().shardCnt - info.back().shardCnt;
        if (abs(diff) <= 1)
            break;

        GID maxg = info.back().gid;
        GID ming = info.front().gid;

        // gid2shards[maxg] -> gid2shards[ming]
        auto sid = *gid2shards[maxg].begin();
        gid2shards[maxg].erase(sid);
        info.front().shardCnt++;
        gid2shards[ming].insert(sid);
        shard2gid[sid] = ming;
        info.back().shardCnt--;

        // 重新排出最大最小
        uint i = 0;
        while ((i + 1) < info.size() && info[i + 1] < info[i])
            std::swap(info[i], info[i + 1]);

        i = info.size() - 1;
        while (i > 0 && info[i] < info[i - 1])
            std::swap(info[i - 1], info[i]);
    }

    configs_.push_back(std::move(newConfig));
    LOG(INFO) << "After join, new config: " << to_string(newConfig) << ", groupInfo: " << info;

    return defaultReply(ShardCtrlerOP::JOIN);
}

ShardCtrler::Reply ShardCtrler::handleLeave(const LeaveArgs &leave, const ApplyMsg &msg)
{
    auto newConfig = configs_.back();
    newConfig.configNum++;

    // 获取配置内容
    auto &gid2shards = newConfig.gid2shards;
    auto &shard2gid = newConfig.shard2gid;
    auto &groupHosts = newConfig.groupHosts;

    std::unordered_set<ShardId> shards; // 存入需要重新分配的分片
    for (GID gid : leave.gids)
    {
        for (ShardId id : gid2shards[gid])
        {
            shards.insert(id);
        }
        gid2shards.erase(gid);
        groupHosts.erase(gid);
    }

    // 全部删除干净，设置为空
    if (gid2shards.empty())
    {
        for (auto sid : shards)
        {
            shard2gid[sid] = INVALID_GID;
        }
    }
    else
    {
        /*
         * Add shards to existing groups as load-balanced as possible
         */
        // 每次给最小的加入分片
        std::priority_queue<GInfo, std::vector<GInfo>, GInfo::Cmp> pq;
        for (auto &it : gid2shards)
        {
            pq.emplace(it.first, it.second.size());
        }

        for (auto sid : shards)
        {
            auto gi = pq.top();
            GID gid = gi.gid;
            int cnt = gi.shardCnt;

            pq.pop();
            gid2shards[gid].insert(sid);
            shard2gid[sid] = gid;
            pq.emplace(gid, cnt + 1);
        }
    }

    configs_.push_back(std::move(newConfig));

    return defaultReply(ShardCtrlerOP::LEAVE);
}

ShardCtrler::Reply ShardCtrler::handleMove(const MoveArgs &move, const ApplyMsg &msg)
{
    Config newConfig = configs_.back();
    ShardId sid = move.shard;
    GID gid = move.gid, oldGid = newConfig.shard2gid[sid];

    newConfig.configNum++;
    newConfig.shard2gid[sid] = gid;
    newConfig.gid2shards[oldGid].erase(sid);
    newConfig.gid2shards[gid].insert(sid);

    configs_.push_back(std::move(newConfig));

    return defaultReply(ShardCtrlerOP::MOVE);
}

ShardCtrler::Reply ShardCtrler::handleQuery(const QueryArgs &query, const ApplyMsg &msg)
{
    Reply rep = defaultReply(ShardCtrlerOP::QUERY);

    if (waits_.find(msg.commandIndex) == waits_.end())
        return rep;

    const int cn = query.configNum;

    // if (cn != LATEST_CONFIG_NUM && cn >= static_cast<int>(configs_.size()))
    // {
    //     rep.code = ErrorCode::ERR_NO_SUCH_SHARD_CONFIG;
    // }
    // else
    {
        rep.code = ErrorCode::SUCCEED;

        if (cn == LATEST_CONFIG_NUM || cn >= static_cast<int>(configs_.size()))
        {
            rep.config = configs_.back();
            LOG(INFO) << "Get Latest config " << rep.config;
        }
        else
        {
            rep.config = configs_[cn];
            LOG(INFO) << "Get config at " << cn << ": " << rep.config;
        }
    }
    return rep;
}

ShardCtrler::Reply ShardCtrler::sendArgsToRaft(const CtrlerArgs &args)
{
    std::string cmd = CtrlerArgs::serialize(args); // 转换为字符串存入日志
    StartResult rs;
    raft_->start(rs, cmd);

    Reply re;
    re.wrongLeader = !rs.isLeader;
    re.code = ErrorCode::SUCCEED;

    if (re.wrongLeader)
    {
        re.code = ErrorCode::ERR_WRONG_LEADER;
        return re;
    }

    auto logId = rs.expectedLogIndex;
    std::future<Reply> f;

    {
        std::lock_guard<std::mutex> guard(lock_);
        f = waits_[logId].get_future();
    }

    f.wait();
    return f.get();
}