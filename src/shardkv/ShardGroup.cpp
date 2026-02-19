#include <future>
#include <fstream>

#include <shardkv/ShardGroup.h>

ShardGroup::ShardGroup(std::vector<Host> &peers, Host me, std::string persisterDir, GID gid)
    : raft_(std::make_unique<RaftHandler>(peers, me, persisterDir, &shardManger_, gid)), gid_(gid)
{
    standalone_ = peers.empty();
    auto snapshotPath = raft_->getPersister()->getLatestSnapshotPath();
    if (!snapshotPath.empty())
    {
        shardManger_.applySnapShot(snapshotPath);
    }
}

void ShardManger::apply(ApplyMsg msg)
{
    auto args = KVArgs::deserialize(msg.command);
    ShardReply rep;

    switch (args.op())
    {
    case KVArgsOP::GET:
    {
        GetReply reply;
        GetParams gp;
        args.copyTo(gp);
        handleGet(reply, gp);
        rep = ShardReply(reply);
    }
    break;

    case KVArgsOP::PUT:
    {
        PutAppendReply reply;
        PutAppendParams pp;
        args.copyTo(pp);
        handlePutAppend(reply, pp);
        rep = ShardReply(reply);
    }
    break;

    default:
        LOG(FATAL) << "Unexpected op: " << static_cast<int>(args.op());
    }

    std::lock_guard<std::mutex> guard(lock_);
    lastApplyIndex_ = msg.commandIndex;
    lastApplyTerm_ = msg.commandTerm;
    LogId id = msg.commandIndex;
    if (waits_.find(id) != waits_.end())
    {
        waits_[id].set_value(rep);
        waits_.erase(id);
    }
}

std::future<ShardReply> ShardManger::getFuture(LogId id)
{
    std::lock_guard<std::mutex> guard(lock_);
    return waits_[id].get_future();
}

void ShardManger::handlePutAppend(PutAppendReply &_return, const PutAppendParams &params)
{
    ShardId sid = params.sid;
    if (checkShard(sid, _return.code) != ErrorCode::SUCCEED)
        return;

    auto &shard = shards_[sid];
    _return = shard.kv.putAppend(params);
}

void ShardManger::handleGet(GetReply &_return, const GetParams &params)
{
    ShardId sid = params.sid;
    if (checkShard(sid, _return.code) != ErrorCode::SUCCEED)
        return;

    auto &shard = shards_[sid];
    _return = shard.kv.get(params);
}

ErrorCode::type ShardManger::checkShard(ShardId sid, ErrorCode::type &code)
{
    if (shards_.find(sid) == shards_.end())
    {
        Shard shard;
        shard.kv = KVService(sid);
        shard.status = ShardStatus::SERVERING;
        shards_[sid] = std::move(shard);
    }

    auto &shard = shards_[sid];
    switch (shard.status)
    {
    case ShardStatus::PULLING:
    case ShardStatus::PUSHING:
        code = ErrorCode::ERR_SHARD_MIGRATING;
        break;
    case ShardStatus::STOP:
        code = ErrorCode::ERR_SHARD_STOP;
        break;
    case ShardStatus::SERVERING:
        code = ErrorCode::SUCCEED;
        break;
    default:
        LOG(FATAL) << "Unexpected shard status: " << shard.status;
        break;
    }
    return code;
}

void ShardManger::startSnapShot(std::string filePath, std::function<void(LogId, TermId)> callback)
{
    LogId lastIndex;
    TermId lastTerm;
    {
        std::lock_guard<std::mutex> guard(lock_);
        lastIndex = lastApplyIndex_;
        lastTerm = lastApplyTerm_;

        std::ofstream ofs(filePath);
        ofs << lastTerm << ' ' << lastIndex << ' ' << shards_.size() << '\n';
        for (const auto &it : shards_)
        {
            const auto sid = it.first;
            const auto &shard = it.second;
            auto data = shard.kv.snapshotData();
            ofs << sid << ' ' << static_cast<int>(shard.status) << ' ' << data.size() << '\n';
            for (const auto &kv : data)
            {
                ofs << kv.first << ' ' << kv.second << '\n';
            }
        }
    }
    callback(lastIndex, lastTerm);
}

void ShardManger::applySnapShot(std::string filePath)
{
    std::lock_guard<std::mutex> guard(lock_);
    std::ifstream ifs(filePath);
    if (!ifs.is_open())
    {
        LOG(ERROR) << "Failed to open snapshot file: " << filePath;
        return;
    }

    size_t shardCount = 0;
    ifs >> lastApplyTerm_ >> lastApplyIndex_ >> shardCount;
    shards_.clear();

    for (size_t i = 0; i < shardCount; i++)
    {
        ShardId sid;
        int statusInt;
        size_t kvCount;
        ifs >> sid >> statusInt >> kvCount;

        std::unordered_map<std::string, std::string> data;
        for (size_t j = 0; j < kvCount; j++)
        {
            std::string key;
            std::string val;
            ifs >> key >> val;
            data[key] = val;
        }

        Shard shard;
        shard.kv = KVService(sid);
        shard.kv.loadData(data);
        shard.status = static_cast<ShardStatus::type>(statusInt);
        shards_[sid] = std::move(shard);
    }
}

void ShardGroup::pullShardParams(PullShardReply &_return, const PullShardParams &params)
{
    if (params.gid != gid_)
    {
        _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
        return;
    }
    if (!exportShardData(params.id, _return.kvs, _return.code))
    {
        return;
    }
    _return.code = ErrorCode::SUCCEED;
}

void ShardManger::ensureShardServing(ShardId sid)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (shards_.find(sid) == shards_.end())
    {
        Shard shard;
        shard.kv = KVService(sid);
        shard.status = ShardStatus::SERVERING;
        shards_[sid] = std::move(shard);
        return;
    }
    auto &shard = shards_[sid];
    shard.status = ShardStatus::SERVERING;
}

bool ShardManger::exportShardData(ShardId sid, std::map<std::string, std::string> &data, ErrorCode::type &code)
{
    std::lock_guard<std::mutex> guard(lock_);
    auto it = shards_.find(sid);
    if (it == shards_.end())
    {
        code = ErrorCode::ERR_NO_SHARD;
        return false;
    }
    if (it->second.status == ShardStatus::STOP)
    {
        code = ErrorCode::ERR_SHARD_STOP;
        return false;
    }
    data.clear();
    auto um = it->second.kv.snapshotData();
    for (const auto &kv : um)
    {
        data[kv.first] = kv.second;
    }
    code = ErrorCode::SUCCEED;
    return true;
}

void ShardManger::importShardData(ShardId sid, const std::map<std::string, std::string> &data)
{
    std::lock_guard<std::mutex> guard(lock_);
    std::unordered_map<std::string, std::string> um;
    for (const auto &kv : data)
    {
        um[kv.first] = kv.second;
    }
    auto &shard = shards_[sid];
    shard.kv = KVService(sid);
    shard.kv.loadData(um);
    shard.status = ShardStatus::SERVERING;
}

void ShardManger::stopShard(ShardId sid)
{
    std::lock_guard<std::mutex> guard(lock_);
    auto it = shards_.find(sid);
    if (it == shards_.end())
    {
        return;
    }
    it->second.status = ShardStatus::STOP;
}

void ShardGroup::ensureShardServing(ShardId sid)
{
    shardManger_.ensureShardServing(sid);
}

bool ShardGroup::exportShardData(ShardId sid, std::map<std::string, std::string> &data, ErrorCode::type &code)
{
    return shardManger_.exportShardData(sid, data, code);
}

void ShardGroup::importShardData(ShardId sid, const std::map<std::string, std::string> &data)
{
    shardManger_.importShardData(sid, data);
}

void ShardGroup::stopShard(ShardId sid)
{
    shardManger_.stopShard(sid);
}

void ShardGroup::putAppend(PutAppendReply &_return, const PutAppendParams &params)
{
    KVArgs args(params);
    std::future<ShardReply> f;
    bool isLeader = sendArgsToRaft(f, args);

    if (isLeader)
    {
        f.wait();
        auto rep = f.get();
        rep.copyTo(_return);
    }
    else
    {
        _return.code = ErrorCode::ERR_WRONG_LEADER;
    }
}

void ShardGroup::get(GetReply &_return, const GetParams &params)
{
    KVArgs args(params);
    std::future<ShardReply> f;
    bool isLeader = sendArgsToRaft(f, args);

    if (isLeader)
    {
        f.wait();
        auto rep = f.get();
        rep.copyTo(_return);
    }
    else
    {
        _return.code = ErrorCode::ERR_WRONG_LEADER;
    }
}

bool ShardGroup::sendArgsToRaft(std::future<ShardReply> &f, const KVArgs &args)
{
    std::string cmd = KVArgs::serialize(args);
    if (standalone_)
    {
        LogId id = localLogId_.fetch_add(1);
        f = shardManger_.getFuture(id);

        ApplyMsg msg;
        msg.command = std::move(cmd);
        msg.commandIndex = id;
        msg.commandTerm = 1;
        shardManger_.apply(std::move(msg));
        return true;
    }

    StartResult rs;
    raft_->start(rs, cmd);
    if (rs.isLeader)
    {
        auto logId = rs.expectedLogIndex;
        f = shardManger_.getFuture(logId);
    }

    return rs.isLeader;
}