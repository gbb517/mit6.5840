#ifndef KVSERVICE_H
#define KVSERVICE_H

#include <string>
#include <map>
#include <unordered_map>

#include <rpc/kvraft/KVRaft_types.h>

// 单个分片的所有 KV 操作逻辑
class KVService
{
public:
    explicit KVService(ShardId sid_);

    PutAppendReply putAppend(const PutAppendParams &params);

    GetReply get(const GetParams &params);

    DeleteReply del(const DeleteParams &params);

    PrefixScanReply prefixScan(const PrefixScanParams &params);

    std::unordered_map<std::string, std::string> snapshotData() const;

    void loadData(const std::unordered_map<std::string, std::string> &data);

private:
    ShardId sid_;
    std::unordered_map<std::string, std::string> um_;
    std::map<std::string, std::string> ordered_;
};

#endif