#ifndef SHARDKVCLIENT_H
#define SHARDKVCLIENT_H

#include <rpc/kvraft/KVRaft.h>
#include <rpc/kvraft/ShardKVRaft.h>
#include <tools/ClientManager.hpp>
#include <shardkv/ShardCtrlerClerk.h>

class ShardKVClerk
{
public:
    ShardKVClerk(std::vector<Host> &ctrlerHosts);

    void putAppend(PutAppendReply &_return, const PutAppendParams &params);

    void get(GetReply &_return, const GetParams &params);

    void del(DeleteReply &_return, const DeleteParams &params);

    void prefixScan(PrefixScanReply &_return, const PrefixScanParams &params);

private:
    ShardId key2shard(const std::string &key, int shardNum) const;
    ShardctrlerClerk ctrlerClerk_;
};

#endif