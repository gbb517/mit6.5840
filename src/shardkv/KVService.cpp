#include <fmt/format.h>
#include <glog/logging.h>

#include <shardkv/KVService.h>

KVService::KVService(ShardId sid)
    : sid_(sid)
{
}

PutAppendReply KVService::putAppend(const PutAppendParams &params)
{
    LOG_IF(FATAL, params.sid != sid_) << fmt::format("Params should be sent to shard {}, current shardId: {}", params.sid, sid_);
    auto key = params.key;
    auto val = params.value;
    switch (params.op)
    {
    case PutOp::PUT:
        um_[key] = val;
        ordered_[key] = val;
        break;
    case PutOp::APPEND:
        um_[key] += val;
        ordered_[key] = um_[key];
        break;
    default:
        LOG(FATAL) << "Unexpected op type: " << to_string(params.op);
    }

    PutAppendReply rep;
    rep.code = ErrorCode::SUCCEED;
    return rep;
}

GetReply KVService::get(const GetParams &params)
{
    LOG_IF(FATAL, params.sid != sid_) << fmt::format("Params should be sent to shard {}, current shardId: {}", params.sid, sid_);
    GetReply rep;
    if (um_.find(params.key) == um_.end())
    {
        rep.code = ErrorCode::ERR_NO_KEY;
        return rep;
    }

    rep.value = um_[params.key];
    rep.code = ErrorCode::SUCCEED;
    return rep;
}

DeleteReply KVService::del(const DeleteParams &params)
{
    LOG_IF(FATAL, params.sid != sid_) << fmt::format("Params should be sent to shard {}, current shardId: {}", params.sid, sid_);
    DeleteReply rep;
    rep.code = ErrorCode::SUCCEED;
    rep.deleted = 0;

    auto it = um_.find(params.key);
    if (it == um_.end())
    {
        return rep;
    }

    um_.erase(it);
    ordered_.erase(params.key);
    rep.deleted = 1;
    return rep;
}

static bool startsWith(const std::string &value, const std::string &prefix)
{
    return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
}

PrefixScanReply KVService::prefixScan(const PrefixScanParams &params)
{
    LOG_IF(FATAL, params.sid != sid_) << fmt::format("Params should be sent to shard {}, current shardId: {}", params.sid, sid_);
    PrefixScanReply rep;
    rep.code = ErrorCode::SUCCEED;
    rep.done = true;
    rep.nextCursor.clear();

    int limit = params.count <= 0 ? 50 : params.count;
    auto it = params.cursor.empty() ? ordered_.lower_bound(params.prefix) : ordered_.upper_bound(params.cursor);

    int emitted = 0;
    while (it != ordered_.end() && emitted < limit)
    {
        if (!startsWith(it->first, params.prefix))
        {
            break;
        }
        rep.kvs[it->first] = it->second;
        rep.nextCursor = it->first;
        ++it;
        emitted++;
    }

    if (it != ordered_.end() && startsWith(it->first, params.prefix))
    {
        rep.done = false;
    }
    return rep;
}

std::unordered_map<std::string, std::string> KVService::snapshotData() const
{
    return um_;
}

void KVService::loadData(const std::unordered_map<std::string, std::string> &data)
{
    um_ = data;
    ordered_.clear();
    for (const auto &kv : um_)
    {
        ordered_[kv.first] = kv.second;
    }
}