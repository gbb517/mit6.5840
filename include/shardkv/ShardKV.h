#ifndef SHARDKV_H
#define SHARDKV_H

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <mutex>
#include <unordered_map>

#include <glog/logging.h>

#include <raft/RaftConfig.h>
#include <raft/StateMachine.h>
#include <rpc/kvraft/ShardKVRaft.h>
#include <shardkv/ShardGroup.h>
#include <shardkv/ShardCtrler.h>
#include <shardkv/ShardCtrlerClerk.h>

class ShardKV : virtual public ShardKVRaftIf
{
public:
    explicit ShardKV(std::vector<Host> &ctrlerHosts, Host me);

    /*
     * methods for KVRaftIf
     */
    void putAppend(PutAppendReply &_return, const PutAppendParams &params) override;
    void get(GetReply &_return, const GetParams &params) override;
    void del(DeleteReply &_return, const DeleteParams &params) override;
    void prefixScan(PrefixScanReply &_return, const PrefixScanParams &params) override;

    /*
     * methods for RaftIf
     */
    void requestVote(RequestVoteResult &_return, const RequestVoteParams &params) override;
    void appendEntries(AppendEntriesResult &_return, const AppendEntriesParams &params) override;
    void getState(RaftState &_return) override;
    void start(StartResult &_return, const std::string &command) override;
    TermId installSnapshot(const InstallSnapshotParams &params) override;

    void pullShardParams(PullShardReply &reply, const PullShardParams &params) override;

private:
    void pollConfigLoop();
    void refreshOwnership(const Config &oldCfg, const Config &newCfg);
    void ensureLocalGroups(const Config &cfg);
    bool tryPullShardData(const Config &oldCfg, GID fromGid, ShardId sid, std::map<std::string, std::string> &data);
    bool tryAckShardTransfer(const Config &oldCfg, GID fromGid, ShardId sid, int32_t configNum);
    std::string basePersisterDir() const;

private:
    std::unordered_map<GID, std::shared_ptr<ShardGroup>> groups_;
    std::vector<Host> ctrlerHosts_;
    ShardctrlerClerk ctrlerClerk_;
    Config currentConfig_;
    std::set<ShardId> ownedShards_;
    std::set<GID> myGids_;
    Host me_;
    std::thread poller_;
    std::atomic<bool> stopped_{false};
    std::mutex lock_;
};

#endif