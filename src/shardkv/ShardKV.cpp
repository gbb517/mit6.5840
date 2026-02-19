#include <shardkv/ShardKV.h>

#include <common.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <sys/stat.h>
#include <tuple>

#include <fmt/format.h>
#include <thrift/Thrift.h>
#include <tools/ClientManager.hpp>

ShardKV::ShardKV(std::vector<Host> &ctrlerHosts, Host me)
    : ctrlerHosts_(ctrlerHosts), ctrlerClerk_(ctrlerHosts), me_(std::move(me))
{
    QueryReply qrep;
    QueryArgs qargs;
    qargs.configNum = LATEST_CONFIG_NUM;
    ctrlerClerk_.query(qrep, qargs);
    if (qrep.code == ErrorCode::SUCCEED)
    {
        Config oldCfg;
        {
            std::lock_guard<std::mutex> guard(lock_);
            oldCfg = currentConfig_;
            currentConfig_ = qrep.config;
        }
        refreshOwnership(oldCfg, qrep.config);
    }

    poller_ = std::thread([this]()
                          { pollConfigLoop(); });
    poller_.detach();
}

void ShardKV::putAppend(PutAppendReply &_return, const PutAppendParams &params)
{
    if (params.gid < 0 || params.sid < 0)
    {
        _return.code = ErrorCode::ERR_INVALID_SHARD;
        return;
    }

    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        if (ownedShards_.find(params.sid) == ownedShards_.end() || myGids_.find(params.gid) == myGids_.end())
        {
            _return.code = ErrorCode::ERR_NO_SHARD;
            return;
        }

        if (!currentConfig_.shard2gid.empty() && params.sid < static_cast<int>(currentConfig_.shard2gid.size()) &&
            currentConfig_.shard2gid[params.sid] != params.gid)
        {
            _return.code = ErrorCode::ERR_NO_SHARD;
            return;
        }

        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            return;
        }
        group = it->second;
    }

    group->putAppend(_return, params);
}

void ShardKV::get(GetReply &_return, const GetParams &params)
{
    if (params.gid < 0 || params.sid < 0)
    {
        _return.code = ErrorCode::ERR_INVALID_SHARD;
        return;
    }

    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        if (ownedShards_.find(params.sid) == ownedShards_.end() || myGids_.find(params.gid) == myGids_.end())
        {
            _return.code = ErrorCode::ERR_NO_SHARD;
            return;
        }

        if (!currentConfig_.shard2gid.empty() && params.sid < static_cast<int>(currentConfig_.shard2gid.size()) &&
            currentConfig_.shard2gid[params.sid] != params.gid)
        {
            _return.code = ErrorCode::ERR_NO_SHARD;
            return;
        }

        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            return;
        }
        group = it->second;
    }

    group->get(_return, params);
}

void ShardKV::requestVote(RequestVoteResult &_return, const RequestVoteParams &params)
{
    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            _return.voteGranted = false;
            return;
        }
        group = it->second;
    }
    group->requestVote(_return, params);
}

void ShardKV::appendEntries(AppendEntriesResult &_return, const AppendEntriesParams &params)
{
    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            _return.success = false;
            return;
        }
        group = it->second;
    }
    group->appendEntries(_return, params);
}

void ShardKV::getState(RaftState &_return)
{
    (void)_return;
    LOG(ERROR) << "Disable RPC invoke for ShardKV::getState without gid context";
}

void ShardKV::start(StartResult &_return, const std::string &command)
{
    (void)command;
    _return.code = ErrorCode::ERR_NOT_SUPPORT_OPERATOR;
    _return.isLeader = false;
}

TermId ShardKV::installSnapshot(const InstallSnapshotParams &params)
{
    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            return INVALID_TERM_ID;
        }
        group = it->second;
    }
    return group->installSnapshot(params);
}

void ShardKV::pullShardParams(PullShardReply &reply, const PullShardParams &params)
{
    if (params.gid < 0 || params.id < 0)
    {
        reply.code = ErrorCode::ERR_INVALID_SHARD;
        return;
    }

    std::shared_ptr<ShardGroup> group;
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = groups_.find(params.gid);
        if (it == groups_.end())
        {
            reply.code = ErrorCode::ERR_NO_SUCH_GROUP;
            return;
        }
        group = it->second;
    }

    group->pullShardParams(reply, params);
}

void ShardKV::pollConfigLoop()
{
    while (!stopped_.load())
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        ctrlerClerk_.query(qrep, qargs);
        if (qrep.code == ErrorCode::SUCCEED)
        {
            Config oldCfg;
            bool changed = false;
            {
                std::lock_guard<std::mutex> guard(lock_);
                if (qrep.config.configNum > currentConfig_.configNum)
                {
                    oldCfg = currentConfig_;
                    currentConfig_ = qrep.config;
                    changed = true;
                }
            }
            if (changed)
            {
                refreshOwnership(oldCfg, qrep.config);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void ShardKV::ensureLocalGroups(const Config &cfg)
{
    auto rootDir = basePersisterDir();
    if (mkdir(rootDir.c_str(), S_IRWXU) && errno != EEXIST)
    {
        LOG(WARNING) << fmt::format("mkdir '{}' failed", rootDir);
    }

    for (GID gid : myGids_)
    {
        if (groups_.find(gid) != groups_.end())
        {
            continue;
        }

        auto it = cfg.groupHosts.find(gid);
        if (it == cfg.groupHosts.end())
        {
            continue;
        }

        auto peers = it->second;
        peers.erase(std::remove_if(peers.begin(), peers.end(), [this](const Host &h)
                                   { return h.ip == me_.ip && h.port == me_.port; }),
                    peers.end());

        auto gidDir = fmt::format("{}/gid_{}", rootDir, gid);
        if (mkdir(gidDir.c_str(), S_IRWXU) && errno != EEXIST)
        {
            LOG(WARNING) << fmt::format("mkdir '{}' failed", gidDir);
        }

        groups_[gid] = std::make_shared<ShardGroup>(peers, me_, gidDir, gid);
    }
}

bool ShardKV::tryPullShardData(const Config &oldCfg, GID fromGid, ShardId sid, std::map<std::string, std::string> &data)
{
    auto srcIt = oldCfg.groupHosts.find(fromGid);
    if (srcIt == oldCfg.groupHosts.end() || srcIt->second.empty())
    {
        return false;
    }

    PullShardParams p;
    p.id = sid;
    p.gid = fromGid;
    p.configNum = oldCfg.configNum;

    auto hosts = srcIt->second;
    ClientManager<ShardKVRaftClient> cm(hosts.size(), KV_PRC_TIMEOUT);
    for (int i = 0; i < static_cast<int>(hosts.size()); i++)
    {
        try
        {
            PullShardReply rep;
            auto *client = cm.getClient(i, hosts[i]);
            client->pullShardParams(rep, p);
            if (rep.code == ErrorCode::SUCCEED)
            {
                data = std::move(rep.kvs);
                return true;
            }
        }
        catch (apache::thrift::TException &)
        {
            cm.setInvalid(i);
        }
    }
    return false;
}

void ShardKV::refreshOwnership(const Config &oldCfg, const Config &newCfg)
{
    std::set<GID> newMyGids;
    std::set<ShardId> newOwned;
    std::vector<std::pair<GID, ShardId>> stopTasks;
    std::vector<std::tuple<GID, ShardId, GID>> pullTasks;

    for (const auto &it : newCfg.groupHosts)
    {
        for (const auto &h : it.second)
        {
            if (h.ip == me_.ip && h.port == me_.port)
            {
                newMyGids.insert(it.first);
                break;
            }
        }
    }

    {
        std::lock_guard<std::mutex> guard(lock_);
        auto oldOwned = ownedShards_;
        myGids_ = newMyGids;
        // 更新group配置
        ensureLocalGroups(newCfg);

        for (int sid = 0; sid < static_cast<int>(newCfg.shard2gid.size()); sid++)
        {
            GID gid = newCfg.shard2gid[sid];
            if (myGids_.find(gid) == myGids_.end())
            {
                continue;
            }

            newOwned.insert(sid);
            // sid 存在，说明能够服务
            auto git = groups_.find(gid);
            if (git != groups_.end())
            {
                git->second->ensureShardServing(sid);
            }

            GID fromGid = INVALID_GID;
            if (sid < static_cast<int>(oldCfg.shard2gid.size()))
            {
                fromGid = oldCfg.shard2gid[sid];
            }
            if (fromGid != INVALID_GID && fromGid != gid)
            {
                // 请求拉取任务
                pullTasks.emplace_back(gid, sid, fromGid);
            }
        }

        for (ShardId sid : oldOwned)
        {
            if (newOwned.find(sid) != newOwned.end())
            {
                continue;
            }
            if (sid < static_cast<int>(oldCfg.shard2gid.size()))
            {
                GID oldGid = oldCfg.shard2gid[sid];
                if (myGids_.find(oldGid) != myGids_.end())
                {
                    stopTasks.emplace_back(oldGid, sid);
                }
            }
        }

        ownedShards_ = newOwned;
    }

    for (const auto &task : stopTasks)
    {
        std::shared_ptr<ShardGroup> g;
        {
            std::lock_guard<std::mutex> guard(lock_);
            auto it = groups_.find(task.first);
            if (it == groups_.end())
            {
                continue;
            }
            g = it->second;
        }
        g->stopShard(task.second);
    }

    for (const auto &task : pullTasks)
    {
        GID toGid = std::get<0>(task);
        ShardId sid = std::get<1>(task);
        GID fromGid = std::get<2>(task);

        std::map<std::string, std::string> data;
        if (!tryPullShardData(oldCfg, fromGid, sid, data))
        {
            continue;
        }

        std::shared_ptr<ShardGroup> g;
        {
            std::lock_guard<std::mutex> guard(lock_);
            auto it = groups_.find(toGid);
            if (it == groups_.end())
            {
                continue;
            }
            g = it->second;
        }
        // 加载数据到本地
        g->importShardData(sid, data);
    }
}

std::string ShardKV::basePersisterDir() const
{
    return fmt::format("/tmp/mit6.824-shardkv-{}", me_.port);
}
