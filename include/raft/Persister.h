#ifndef PERSISTER_H
#define PERSISTER_H

#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

#include <rpc/kvraft/KVRaft_types.h>

// 单个日志 Chunk 文件的最大字节数 4MB
const unsigned int LOG_CHUNK_BYTES = 4 * 1024 * 1024;

class RaftHandler;

struct Metadata
{
    TermId term;
    Host voteFor;
};

class Persister
{
public:
    Persister(std::string dirPath);

    ~Persister();

    void saveTermAndVote(TermId term, Host &host);

    void saveLogs(LogId commitIndex, std::deque<LogEntry> &logs);

    void loadRaftState(TermId &term, Host &votedFor, std::deque<LogEntry> &logs, TermId &lastIncTerm, LogId &lastIncIndex);

    void commitSnapshot(std::string tmpName, TermId lastIncTerm, LogId lastIncIndex);

    std::string getLatestSnapshotPath();

    std::string getTmpSnapshotPath();

private:
    void flushLogBuf();

    bool checkState(TermId &term, Host &voteFor, std::deque<LogEntry> &logs);

    uint estmateSize(LogEntry &log);

    int loadChunks();

    void applySnapshot(std::string snapshotPath);

    void compactLogs(LogId lastIncIndex);

    std::vector<std::string> filesIn(std::string &dir);

private:
    /*  数据拆分存储  */
    // 元数据文件路径
    std::string metaFilePath_;
    // 日志分段目录
    std::string logChunkDir_;
    // 快照目录
    std::string snapshotDir_;

    Metadata md_;
    // 日志块名称
    std::deque<std::string> chunkNames_;
    // 快照对应的最后日志索引和任期
    LogId lastIncludeIndex_;
    TermId lastIncludeTerm_;

    std::deque<LogEntry> logBuf_;
    uint estimateLogBufSize_;
    LogId lastInBufLogId_;
};

#endif