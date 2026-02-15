
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <thrift/TToString.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <raft/RaftConfig.h>
#include <raft/raft.h>
#include <tools/ClientManager.hpp>
#include <tools/Timer.hpp>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using std::string;
using std::vector;
using time_point = std::chrono::steady_clock::time_point;

RaftHandler::RaftHandler(vector<Host> &peers, Host me, string persisterDir, StateMachineIf *stateMachine, GID gid)
    : currentTerm_(0), votedFor_(NULL_HOST), commitIndex_(0), lastApplied_(0),
      nextIndex_(vector<int32_t>(peers.size())),
      matchIndex_(vector<int32_t>(peers.size())),
      state_(ServerState::FOLLOWER), lastSeenLeader_(NOW()),
      peers_(peers), me_(std::move(me)), inElection_(false),
      inSnapshot_(false), persister_(persisterDir),
      cmForHB_(ClientManager<RaftClient>(peers.size(), HEART_BEATS_INTERVAL)),
      cmForRV_(ClientManager<RaftClient>(peers.size(), RPC_TIMEOUT)),
      cmForAE_(ClientManager<RaftClient>(peers.size(), RPC_TIMEOUT)),
      stateMachine_(stateMachine), snapshotIndex_(0), snapshotTerm_(0), gid_(gid), isExit_(false)
{
    switchToFollow();

    for (uint i = 0; i < peers_.size(); i++)
    {
        std::thread ae([this, i]()
                       { this->async_replicateLogTo(i, peers_[i]); });
        ae.detach();
    }

    std::thread applier([this]()
                        { this->async_applyMsg(); });
    applier.detach();

    std::thread snapshot([this]()
                         { this->async_startSnapShot(); });
    snapshot.detach();

    persister_.loadRaftState(currentTerm_, votedFor_, logs_, snapshotIndex_, snapshotTerm_);
    LOG(INFO) << fmt::format("Load raft state: (term: {}, votedFor_: {}, logs: {}",
                             currentTerm_, to_string(votedFor_), logsRange(logs_));
}

RaftHandler::~RaftHandler()
{
    Timer t("Start to exit!", "Exit successfully!");
    isExit_ = true;
    sendEntries_.notify_all();
    applyLogs_.notify_all();
    startSnapshot_.notify_all();
    // wait for other threads exit
    std::this_thread::sleep_for(MAX_ELECTION_TIMEOUT);
}

void RaftHandler::requestVote(RequestVoteResult &_return, const RequestVoteParams &params)
{
    std::lock_guard<std::mutex> guard(raftLock_);
    _return.voteGranted = false;

    if (isExit_)
    {
        _return.term = currentTerm_;
        return; // Raft already exited, reject all requests.
    }

    // 对方任期更高，投票
    if (params.term > currentTerm_)
    {
        if (state_ != ServerState::FOLLOWER)
            switchToFollow();
        currentTerm_ = params.term;
        votedFor_ = NULL_HOST;
        lastSeenLeader_ = NOW();
    }

    if (params.term < currentTerm_)
    {
        LOG(INFO) << fmt::format("Out of fashion vote request from {}, term: {}, currentTerm: {}",
                                 to_string(params.candidateId), params.term, currentTerm_);
        _return.term = currentTerm_;
        return;
    }

    // 已经投过票
    if (params.term == currentTerm_ && votedFor_ != NULL_HOST && votedFor_ != params.candidateId)
    {
        LOG(INFO) << fmt::format("Receive a vote request from {}, but already voted to {}, reject it.",
                                 to_string(params.candidateId), to_string(votedFor_));
        _return.term = currentTerm_;
        return;
    }

    /*
     * If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs
     * end with the same term, then whichever log is longer is more up-to-date.
     */
    // 检查候选人日志是否至少和本地一样新
    bool candidateLogUpToDate = (params.LastLogTerm > lastLogTerm()) ||
                                (params.LastLogTerm == lastLogTerm() && params.lastLogIndex >= lastLogIndex());
    // 不是最新，不给投票
    if (!candidateLogUpToDate)
    {
        LOG(INFO) << fmt::format("Receive a vote request from {} with outdate logs, reject it.", to_string(params.candidateId));
        return;
    }

    LOG(INFO) << "Vote for " << params.candidateId;
    votedFor_ = params.candidateId;
    _return.voteGranted = true;
    _return.term = currentTerm_;
    persister_.saveTermAndVote(currentTerm_, votedFor_);
}

void RaftHandler::appendEntries(AppendEntriesResult &_return, const AppendEntriesParams &params)
{
    std::lock_guard<std::mutex> guard(raftLock_);

    _return.success = false;
    _return.term = currentTerm_;

    if (isExit_)
    {
        return; // Raft already exited, reject all requests.
    }

    // 5.1 如果 Leader 的 Term < 自身 Term → Leader 已过期，拒绝 RPC
    if (params.term < currentTerm_)
    {
        LOG(INFO) << fmt::format("Out of fashion appendEntries from {}, term: {}, currentTerm: {}",
                                 to_string(params.leaderId), params.term, currentTerm_);
        return;
    }

    // 自身也是 leader，有问题（需要重新选举）
    if (params.term == currentTerm_ && state_ != ServerState::FOLLOWER)
    {
        LOG_IF(FATAL, state_ == ServerState::LEADER) << "Two leader in the same term!";
        switchToFollow();
    }

    if (params.term > currentTerm_)
    {
        LOG(INFO) << fmt::format("Received logs from higher term leader {}, term: {}, currentTerm: {}",
                                 to_string(params.leaderId), params.term, currentTerm_);
        // 更新自身 Term 和 voteFor（用于下次投票）
        currentTerm_ = params.term;
        votedFor_ = NULL_HOST;
        if (state_ != ServerState::FOLLOWER)
        {
            switchToFollow();
        }
    }
    lastSeenLeader_ = NOW();

    // Leader 要求同步的日志的“前置索引”（prevLogIndex）> 自身最后日志索引 → 日志太新，无法匹配，拒绝
    if (params.prevLogIndex > lastLogIndex())
    {
        LOG(INFO) << fmt::format("Expected older logs. prevLogIndex: {}, param.prevLogIndex: {}",
                                 lastLogIndex(), params.prevLogIndex);
        return;
    }

    // 如果 prevLogIndex 小于当前快照索引，说明该日志已经被压缩，无法通过日志数组校验
    if (params.prevLogIndex < snapshotIndex_)
    {
        // 对方提到的日志已经被做成快照了，无法校验。
        // 通常返回 false (让 Leader 发送 InstallSnapshot) 或者直接忽略
        LOG(INFO) << fmt::format("Expected snapshot. params.prevLogIndex < snapshotIndex_");
        return;
    }

    TermId preLogTerm = params.prevLogIndex == snapshotIndex_ ? snapshotTerm_ : getLogByLogIndex(params.prevLogIndex).term;
    // 如果恰好是快照的最后一条日志，使用快照保存的 Term
    // 只有确信 prevLogIndex > snapshotIndex_ 后，才能安全调用 getLogByLogIndex
    // 5.3 prevLogIndex 合法，但对应位置的 Term 与 Leader 不一致 → 回退日志（(说明之前接收了旧 Leader 的脏数据)）
    if (params.prevLogTerm != lastLogTerm())
    {

        LOG(INFO) << fmt::format("Expected log term {} at index {}, get {}, delete all logs after it!",
                                 params.prevLogTerm, lastLogIndex(), lastLogTerm());
        // 删除从 prevLogIndex 开始及之后的所有日志
        while (!logs_.empty() && logs_.back().index >= params.prevLogIndex)
            logs_.pop_back();
        // 返回false，让leader返回更小的prelogidex，因为之前的也可能冲突
        return;
    }

    {
        // 处理日志冲突（相同索引但 Term 不同 → 删除冲突日志）
        uint i;
        for (i = 0; i < params.entries.size(); i++)
        {
            auto &newEntry = params.entries[i];
            // 新日志索引超过自身最后索引 → 无冲突，跳出循环准备追加
            if (newEntry.index > lastLogIndex())
                break;
            // 找到冲突日志（同索引不同 Term）
            auto &entry = getLogByLogIndex(newEntry.index);
            if (newEntry.term != entry.term)
            {
                LOG(INFO) << fmt::format("Expected log term {} at index {}, get {}, delete all logs after it!",
                                         newEntry.term, entry.index, entry.term); // 删除冲突索引及以后的所有日志
                while (!logs_.empty() && logs_.back().index >= newEntry.index)
                    logs_.pop_back();
                break;
            }
        }

        // 追加所有未在本地的新日志
        for (; i < params.entries.size(); i++)
        {
            auto &entry = params.entries[i];
            logs_.push_back(entry);
        }
    }

    // 提交日志
    updateCommitIndex(std::min(params.leaderCommit, lastLogIndex()));

    if (params.entries.empty())
    {
        LOG_EVERY_N(INFO, HEART_BEATS_LOG_COUNT)
            << fmt::format("Received {} heart beats from leader {}, term: {}, currentTerm: {}",
                           HEART_BEATS_LOG_COUNT, to_string(params.leaderId), params.term, currentTerm_);
    }
    else
    {
        LOG(INFO) << "Received appendEntries request!";
    }
    _return.success = true;
}

void RaftHandler::getState(RaftState &_return)
{
    std::lock_guard<std::mutex> guard(raftLock_);
    _return.currentTerm = currentTerm_;
    _return.votedFor = votedFor_;
    _return.commitIndex = commitIndex_;
    _return.lastApplied = lastApplied_;
    _return.state = state_;
    _return.peers = peers_;
    _return.logs = std::vector<LogEntry>(logs_.begin(), logs_.end());
    LOG(INFO) << fmt::format("Get raft state: term = {}, votedFor = {}, commitIndex = {}, lastApplied = {}, state={}, logs size={}",
                             currentTerm_, to_string(votedFor_), commitIndex_, lastApplied_, to_string(state_), _return.logs.size());
}

void RaftHandler::start(StartResult &_return, const std::string &command)
{
    std::lock_guard<std::mutex> guard(raftLock_);
    _return.code = ErrorCode::SUCCEED;

    if (isExit_)
    {
        _return.code = ErrorCode::ERR_REQUEST_FAILD;
        return; // Raft already exited, reject all requests.
    }

    if (state_ != ServerState::LEADER)
    {
        _return.isLeader = false;
        _return.code = ErrorCode::ERR_WRONG_LEADER;
        return;
    }

    LogEntry log;
    log.index = lastLogIndex() + 1;
    log.command = command;
    log.term = currentTerm_;
    logs_.push_back(std::move(log));

    _return.expectedLogIndex = log.index;
    _return.term = currentTerm_;
    _return.isLeader = true;

    sendEntries_.notify_all();
    LOG(INFO) << "start to synchronization cmd: " << command;
}

TermId RaftHandler::installSnapshot(const InstallSnapshotParams &params)
{
    std::lock_guard<std::mutex> guard(raftLock_);
    static std::string tmpSnapshotPath;
    static int offset = 0;

    // auto &tmpSnapshotPath = this->tmpSnapshotPath_;
    // auto &offset = this->snapshotOffset_;

    lastSeenLeader_ = NOW();

    if (params.term < currentTerm_)
    {
        LOG(INFO) << fmt::format("Out of fashion installsnapshot from {}, term: {}, currentTerm: {}",
                                 to_string(params.leaderId), params.term, currentTerm_);
        return currentTerm_;
    }

    if (params.term > currentTerm_)
    {
        LOG(INFO) << fmt::format("Received snapshot from higher term leader {}, term: {}, currentTerm: {}",
                                 to_string(params.leaderId), params.term, currentTerm_);
        currentTerm_ = params.term;
        if (state_ != ServerState::FOLLOWER)
        {
            switchToFollow();
        }
    }

    if (offset > params.offset || offset < params.offset)
    {
        return currentTerm_;
    }

    if (offset == 0)
    {
        if (access(tmpSnapshotPath.c_str(), F_OK) == 0)
        {
            unlink(tmpSnapshotPath.c_str());
            LOG(INFO) << "Remove old tmp file: " << tmpSnapshotPath;
        }
        tmpSnapshotPath = persister_.getTmpSnapshotPath();
    }

    std::ofstream ofs(tmpSnapshotPath, std::ios::app | std::ios::binary);
    ofs.write(params.data.c_str(), params.data.size());
    LOG(INFO) << fmt::format("Write {} bytes into {}", params.data.size(), tmpSnapshotPath);

    if (params.done)
    {
        persister_.commitSnapshot(tmpSnapshotPath, params.lastIncludedTerm, params.lastIncludedIndex);
        auto ssPath = persister_.getLatestSnapshotPath();
        std::thread applySs([this, ssPath]()
                            { stateMachine_->applySnapShot(ssPath); });
        applySs.detach();
        offset = 0;
    }

    return currentTerm_;
}

void RaftHandler::switchToFollow()
{
    state_ = ServerState::FOLLOWER;
    LOG(INFO) << "Switch to follower!";
    std::thread cl([this]()
                   { this->async_checkLeaderStatus(); });
    cl.detach();
}

void RaftHandler::switchToCandidate()
{
    state_ = ServerState::CANDIDAE;
    LOG(INFO) << "Switch to Candidate!";
    std::thread se([this]()
                   { this->async_startElection(); });
    se.detach();
}

void RaftHandler::switchToLeader()
{
    state_ = ServerState::LEADER;
    LOG(INFO) << "Switch to Leader! Term: " << currentTerm_;
    std::thread hb([this]()
                   { this->async_sendHeartBeats(); });
    hb.detach();
    std::fill(nextIndex_.begin(), nextIndex_.end(), lastLogIndex() + 1);
    std::fill(matchIndex_.begin(), matchIndex_.end(), 0);
}

void RaftHandler::updateCommitIndex(LogId newIndex)
{
    if (newIndex > commitIndex_)
    {
        commitIndex_ = newIndex;
        applyLogs_.notify_one();
        if (commitIndex_ - snapshotIndex_ > MAX_LOGS_BEFORE_SNAPSHOT)
        {
            startSnapshot_.notify_one();
        }
        persister_.saveLogs(commitIndex_, logs_);
    }
}

LogEntry &RaftHandler::getLogByLogIndex(LogId logIndex)
{
    uint i = logIndex - (logs_.empty() ? snapshotIndex_ : logs_.front().index);
    LOG_IF(FATAL, i < 0 || i > logs_.size()) << fmt::format("Unexpected log index {}, cur logs: {}", logIndex, logsRange(logs_));
    auto &entry = logs_[i];
    LOG_IF(FATAL, logIndex != entry.index) << "Unexpected log entry: " << entry << " in index: " << logIndex;
    return entry;
}

AppendEntriesParams RaftHandler::buildAppendEntriesParamsFor(int peerIndex)
{
    AppendEntriesParams params;
    params.term = currentTerm_;
    params.leaderId = me_;

    // 本次待追加的日志条目
    LogId prev = nextIndex_[peerIndex] - 1;
    // 需要发送的日志条目还存在日志中
    if (!logs_.empty() && prev >= logs_.front().index)
    {
        auto &prevLog = getLogByLogIndex(prev);
        params.prevLogIndex = prevLog.index;
        params.prevLogTerm = prevLog.term;
    }
    else
    {
        // 日志已经存入快照中，发送当前快照
        params.prevLogIndex = snapshotIndex_;
        params.prevLogTerm = snapshotTerm_;
    }

    params.leaderCommit = commitIndex_;
    params.gid = gid_;
    return params;
}

void RaftHandler::handleReplicateResultFor(int peerIndex, LogId prevLogIndex, LogId matchedIndex, bool success)
{
    int i = peerIndex;
    if (success)
    {
        nextIndex_[i] = matchedIndex + 1;
        matchIndex_[i] = matchedIndex;
        /*
         * Update commitIndex_. This is a typical top k problem, since the size of matchIndex_ is tiny,
         * to simplify the code, we handle this problem by sorting matchIndex_.
         */
        auto matchI = matchIndex_;
        matchI.push_back(lastLogIndex());
        sort(matchI.begin(), matchI.end());
        LogId agreeIndex = matchI[matchI.size() / 2];
        if (agreeIndex != commitIndex_ && getLogByLogIndex(agreeIndex).term == currentTerm_)
        {
            updateCommitIndex(agreeIndex);
        }
    }
    else
    {
        nextIndex_[i] = std::min(nextIndex_[i], prevLogIndex);
    }
}

int RaftHandler::gatherLogsFor(int peerIndex, AppendEntriesParams &params)
{
    int target = lastLogIndex();
    if (nextIndex_[peerIndex] > target)
        return 0;

    int logsNum = std::min(target - nextIndex_[peerIndex] + 1, MAX_LOGS_PER_REQUEST);
    // 发送日志条目队列
    for (int j = 0; j < logsNum; j++)
    {
        params.entries.push_back(getLogByLogIndex(j + nextIndex_[peerIndex]));
    }

    return logsNum;
}

std::chrono::microseconds RaftHandler::getElectionTimeout()
{
    static std::random_device rd;
    static std::uniform_int_distribution<int> randomTime(
        MIN_ELECTION_TIMEOUT.count(),
        MAX_ELECTION_TIMEOUT.count());
    auto timeout = std::chrono::milliseconds(randomTime(rd));
    return timeout;
}

void RaftHandler::compactLogs()
{
    int oldSize = logs_.size();
    while (!logs_.empty() && logs_.front().index < snapshotIndex_)
    {
        logs_.pop_front();
    }
    LOG(INFO) << fmt::format("Remove {} logs, before: {}, now: {}", oldSize - logs_.size(), oldSize, logs_.size());
}

bool RaftHandler::async_sendLogsTo(int peerIndex, Host host, AppendEntriesParams &params, ClientManager<RaftClient> &cm)
{
    bool success = true;
    try
    {
        AppendEntriesResult rs;
        auto *client_ = cm.getClient(peerIndex, host);
        client_->appendEntries(rs, params);

        if (!params.entries.empty())
        {
            std::lock_guard<std::mutex> guard(raftLock_);
            LogId matchId = params.prevLogIndex + params.entries.size();
            handleReplicateResultFor(peerIndex, params.prevLogIndex, matchId, rs.success);
        }

        bool isHb = params.entries.empty(); // is heart beat;
        LOG_IF(INFO, !isHb) << fmt::format("Send {} logs to {}", params.entries.size(), to_string(host))
                            << fmt::format(", params: (prevLogIndex={}, prevLogTerm={}, commit={})",
                                           params.prevLogIndex, params.prevLogTerm, params.leaderCommit)
                            << fmt::format(", the result: (success: {}, term: {})", rs.success, rs.term);
    }
    catch (TException &tx)
    {
        cm.setInvalid(peerIndex);
        LOG(INFO) << fmt::format("Send logs to {} failed: {}", to_string(host), tx.what());
        success = false;
    }
    return success;
}

bool RaftHandler::async_sendSnapshotTo(int peerIndex, Host host)
{
    bool success = true;
    std::ifstream ifs;
    InstallSnapshotParams params;
    {
        std::lock_guard<std::mutex> guard(raftLock_);
        auto ss = persister_.getLatestSnapshotPath();
        ifs = std::ifstream(ss, std::ios::binary);
        LOG_IF(FATAL, !ifs.good()) << "Invalid snaphot: " << ss;

        params.leaderId = me_;
        params.term = currentTerm_;
        params.lastIncludedIndex = snapshotIndex_;
        params.lastIncludedTerm = snapshotTerm_;
        params.gid = gid_;
    }

    LOG(INFO) << fmt::format("Send snapshot to {}", to_string(host));

    constexpr int bufSize = 4096;
    char buf[bufSize];
    while (ifs.good())
    {
        ifs.read(buf, bufSize);
        params.data = std::string(buf);
        params.done = ifs.good();
        try
        {
            auto *client = cmForAE_.getClient(peerIndex, host);
            client->installSnapshot(params);
        }
        catch (TException &tx)
        {
            success = false;
            cmForAE_.setInvalid(peerIndex);
            break;
        }
        params.offset += bufSize;
        if (isExit_)
            return true; // Exit the loop if raft exited.
    }

    if (success)
    {
        std::lock_guard<std::mutex> guard(raftLock_);
        LogId matchId = params.lastIncludedIndex;
        handleReplicateResultFor(peerIndex, matchIndex_[peerIndex], matchId, true);
    }
    return true;
}

void RaftHandler::async_checkLeaderStatus() noexcept
{
    // 检查leader是否超时
    while (true)
    {
        std::this_thread::sleep_for(getElectionTimeout());
        if (isExit_)
            return; // Exit the loop if raft exited.
        {
            std::lock_guard<std::mutex> guard(raftLock_);
            switch (state_)
            {
            case ServerState::CANDIDAE:
            case ServerState::FOLLOWER:
            {
                if (NOW() - lastSeenLeader_ > MIN_ELECTION_TIMEOUT)
                {
                    LOG(INFO) << "Election timeout, start a election.";
                    switchToCandidate();
                }
            }
            break;
            case ServerState::LEADER:
                /*
                 * Leader does not need to check the status of leader,
                 * exit this thread
                 */
                LOG(INFO) << "Raft become the leader, exit the checkLeaderStatus thread!";
                // 直到成为leader会退出
                return;
            default:
                LOG(FATAL) << "Unexpected state!";
            }
        }
    }
}

void RaftHandler::async_startElection() noexcept
{
    bool expected = false;
    // 原子比较
    if (!inElection_.compare_exchange_strong(expected, true))
    {
        LOG(INFO) << "Current raft is in the election!";
        return;
    }

    vector<Host> peersForRV;
    RequestVoteParams params;
    {
        std::lock_guard<std::mutex> guard(raftLock_);
        if (state_ != ServerState::CANDIDAE)
        {
            LOG(INFO) << "Raft is not candidate now, exit the election";
            inElection_ = false;
            return;
        }

        // 开启选举
        LOG(INFO) << fmt::format("Start a new election! currentTerm: {}, nextTerm: {}", currentTerm_, currentTerm_ + 1);
        currentTerm_++;
        votedFor_ = me_;
        persister_.saveTermAndVote(currentTerm_, votedFor_);

        /*
         * reset lastSeenLeader_, so the async_checkLeaderStatus thread can start a new election
         * if no leader is selected in this term.
         */
        // 重置候选时间
        lastSeenLeader_ = NOW();

        // copy peers_ to a local variable to avoid aquire and release raftLock_ frequently
        peersForRV = peers_;

        // RPC 参数
        params.term = currentTerm_;
        params.candidateId = me_;
        params.lastLogIndex = lastLogIndex();
        params.LastLogTerm = lastLogTerm();
        params.gid = gid_;
    }

    std::atomic<int> voteCnt(1);
    std::atomic<int> finishCnt(0);
    std::mutex finish;
    std::condition_variable cv;

    vector<std::thread> threads(peersForRV.size());
    for (uint i = 0; i < peersForRV.size(); i++)
    {
        // 单独线程进行RPC调用并处理返回数据
        threads[i] = std::thread([i, this, &peersForRV, &params, &voteCnt, &finishCnt, &cv]()
                                 {
            RequestVoteResult rs;
            RaftClient* client;
            try {
                client = cmForRV_.getClient(i, peersForRV[i]);
                client->requestVote(rs, params);
            } catch (TException& tx) {
                LOG(ERROR) << fmt::format("Request {} for voting error: {}", to_string(peersForRV[i]), tx.what());
                cmForRV_.setInvalid(i);
            }

            if (rs.voteGranted)
                voteCnt++;
            finishCnt++;
            cv.notify_one(); });
    }

    int raftNum = peers_.size() + 1;
    std::unique_lock<std::mutex> lk(finish);
    // 等待投票超过半数/全部followes询问过
    cv.wait(lk, [raftNum, &finishCnt, &voteCnt]()
            { return voteCnt > (raftNum / 2) || (finishCnt == raftNum - 1); });

    LOG(INFO) << fmt::format("Raft nums: {}, get votes: {}", raftNum, voteCnt.load());
    if (voteCnt > raftNum / 2)
    {
        // 成功当选 Leader
        std::lock_guard<std::mutex> guard(raftLock_);
        if (state_ == ServerState::CANDIDAE)
            switchToLeader();
        else
            LOG(ERROR) << fmt::format("Raft is not candidate now!");
    }

    for (uint i = 0; i < threads.size(); i++)
        threads[i].join();
    inElection_ = false;
}

void RaftHandler::async_sendHeartBeats() noexcept
{
    /*
     * copy peers_ to a local variable, so we don't need to aquire and release raftLock_ frequently
     * to access peers_
     */
    vector<Host> peersForHB;
    {
        std::lock_guard<std::mutex> guard(raftLock_);
        peersForHB = peers_;
    }

    while (!isExit_)
    {
        vector<AppendEntriesParams> paramsList(peersForHB.size());
        {
            std::lock_guard<std::mutex> guard(raftLock_);
            if (state_ != ServerState::LEADER)
                return;
            for (uint i = 0; i < peersForHB.size(); i++)
            {
                paramsList[i] = buildAppendEntriesParamsFor(i);
            }
        }

        auto startNext = NOW() + HEART_BEATS_INTERVAL;
        vector<std::thread> threads(peersForHB.size());
        for (uint i = 0; i < peersForHB.size(); i++)
        {
            threads[i] = std::thread([i, &peersForHB, &paramsList, this]()
                                     { async_sendLogsTo(i, peersForHB[i], paramsList[i], cmForHB_); });
        }

        for (uint i = 0; i < peersForHB.size(); i++)
        {
            threads[i].join();
        }
        LOG_EVERY_N(INFO, HEART_BEATS_LOG_COUNT) << fmt::format("Broadcast {} heart beats", HEART_BEATS_LOG_COUNT);

        std::this_thread::sleep_until(startNext);
    }
}

void RaftHandler::async_replicateLogTo(int peerIndex, Host host) noexcept
{
    while (true)
    {
        {
            /*
             * wait for RaftHandler::start or destructor function to notify
             */
            std::unique_lock<std::mutex> logLock(raftLock_);
            sendEntries_.wait(logLock);
            if (isExit_)
                return; // Exit the loop if raft exited.
            if (state_ != ServerState::LEADER)
                continue;
        }

        LOG(INFO) << "async_replicateLogTo to " << to_string(host) << " is notified";
        LogId lastLogId, nextIndex, snapshotIndex;
        while (true)
        {
            {
                std::lock_guard<std::mutex> guard(raftLock_);
                if (state_ != ServerState::LEADER)
                    break;
                lastLogId = lastLogIndex();
                nextIndex = nextIndex_[peerIndex];
                snapshotIndex = snapshotIndex_;
            }

            bool success = true;
            if (nextIndex + MAX_LOGS_BEFORE_SNAPSHOT < snapshotIndex)
            {
                success = async_sendSnapshotTo(peerIndex, host);
            }
            // 存在日志需要发送
            else if (nextIndex <= lastLogId)
            {
                AppendEntriesParams params;
                int logsCount = 0;
                {
                    std::lock_guard<std::mutex> guard(raftLock_);
                    params = buildAppendEntriesParamsFor(peerIndex);
                    logsCount = gatherLogsFor(peerIndex, params);
                }
                // （logsCount == 0），说明出了问题（被快照截断了），强制检查快照
                if (logsCount == 0 && nextIndex <= lastLogId)
                {
                    continue;
                }
                success = async_sendLogsTo(peerIndex, host, params, cmForAE_);
            }
            else
            {
                break;
            }

            /*
             * To avoid too many failed attempts, we sleep for a while if replicate logs failed
             */
            if (!success)
            {
                std::this_thread::sleep_for(HEART_BEATS_INTERVAL);
            }
            if (isExit_)
                return; // Exit the loop if raft exited.
        }
    }
}

void RaftHandler::async_applyMsg() noexcept
{
    std::queue<LogEntry> logsForApply;
    while (true)
    {
        {
            std::unique_lock<std::mutex> lockLock(raftLock_);
            applyLogs_.wait(lockLock);
            LOG(INFO) << "async_applyMsg is notified";
            if (isExit_)
                return; // Exit the loop if raft exited.
        }

        while (true)
        {
            {
                std::lock_guard<std::mutex> guard(raftLock_);
                for (int i = lastApplied_ + 1; i <= std::min(lastApplied_ + 20, commitIndex_); i++)
                {
                    logsForApply.push(getLogByLogIndex(i)); // <--- 潜在崩溃点
                }
                // // 确保 i >= 日志起始位置
                // LogId startId = lastApplied_ + 1;
                // LogId endId = std::min(lastApplied_ + 20, commitIndex_);

                // // 获取当前日志及其物理起始索引
                // LogId logStartIndex = logs_.empty() ? snapshotIndex_ : logs_.front().index;
                // if (startId < logStartIndex)
                // {
                //     startId = logStartIndex;
                // }
                // we only copy 20 logs each time to avoid holding the lock for too long
                // for (int i = startId; i <= endId; i++)
                // {
                //     if (i > lastLogIndex())
                //         break;
                //     logsForApply.push(getLogByLogIndex(i));
                // }
            }

            if (logsForApply.empty())
                break;
            LOG(INFO) << fmt::format("Gather {} logs to apply!", logsForApply.size());

            const int N = logsForApply.size();
            while (!logsForApply.empty())
            {
                LogEntry log = std::move(logsForApply.front());
                logsForApply.pop();

                ApplyMsg msg;
                msg.command = log.command;
                msg.commandIndex = log.index;
                msg.commandTerm = log.term;
                stateMachine_->apply(msg);
            }

            {
                std::lock_guard<std::mutex> guard(raftLock_);
                lastApplied_ += N;
            }
            if (isExit_)
                return; // Exit the loop if raft exited.
        }
    }
}

void RaftHandler::async_startSnapShot() noexcept
{
    while (true)
    {
        {
            std::unique_lock<std::mutex> lock_(raftLock_);
            startSnapshot_.wait(lock_);
            if (isExit_)
                return; // Exit the loop if raft exited.
        }

        bool expected = false;
        if (!inSnapshot_.compare_exchange_strong(expected, true))
            continue;

        LOG(INFO) << fmt::format("Start snapshot, lastLogIndex: {}, logs: {}", lastLogIndex(), logsRange(logs_));

        string tmpSnapshotFile = persister_.getTmpSnapshotPath();
        std::function<void(int, int)> callback = [tmpSnapshotFile, this](LogId lastIncludeIndex, TermId lastIncludeTerm) -> void
        {
            {
                std::lock_guard<std::mutex> guard(raftLock_);
                persister_.commitSnapshot(tmpSnapshotFile, lastIncludeTerm, lastIncludeIndex);
                snapshotIndex_ = lastIncludeIndex;
                snapshotTerm_ = lastIncludeTerm;
                compactLogs();
            }
            inSnapshot_ = false;
        };

        stateMachine_->startSnapShot(tmpSnapshotFile, callback);
        if (isExit_)
            return; // Exit the loop if raft exited.
    }
}