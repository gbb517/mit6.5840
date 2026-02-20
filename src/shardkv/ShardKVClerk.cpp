#include <shardkv/ShardKVClerk.h>

#include <common.h>

#include <shardkv/ShardCtrler.h>
#include <thread>
#include <thrift/Thrift.h>

ShardKVClerk::ShardKVClerk(std::vector<Host> &ctrlerHosts)
    : ctrlerClerk_(ctrlerHosts)
{
}

void ShardKVClerk::putAppend(PutAppendReply &_return, const PutAppendParams &params)
{
    _return.code = ErrorCode::ERR_REQUEST_FAILD;
    for (int attempt = 0; attempt < 40; attempt++)
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        ctrlerClerk_.query(qrep, qargs);
        if (qrep.code != ErrorCode::SUCCEED)
        {
            _return.code = qrep.code;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        const auto &config = qrep.config;
        if (config.shard2gid.empty())
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        ShardId sid = key2shard(params.key, config.shard2gid.size());
        PutAppendParams realParams = params;

        auto tryConfig = [&](const Config &cfg)
        {
            if (sid >= static_cast<int>(cfg.shard2gid.size()))
            {
                return false;
            }
            GID gid = cfg.shard2gid[sid];
            if (gid == INVALID_GID)
            {
                _return.code = ErrorCode::ERR_INVALID_SHARD;
                return false;
            }

            auto groupIt = cfg.groupHosts.find(gid);
            if (groupIt == cfg.groupHosts.end() || groupIt->second.empty())
            {
                _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
                return false;
            }

            realParams.gid = gid;
            realParams.sid = sid;
            auto hosts = groupIt->second;
            ClientManager<ShardKVRaftClient> cm(hosts.size(), KV_PRC_TIMEOUT);
            for (int i = 0; i < static_cast<int>(hosts.size()); i++)
            {
                try
                {
                    auto *client = cm.getClient(i, hosts[i]);
                    client->putAppend(_return, realParams);
                    if (_return.code == ErrorCode::SUCCEED)
                    {
                        return true;
                    }
                }
                catch (apache::thrift::TException &)
                {
                    cm.setInvalid(i);
                    _return.code = ErrorCode::ERR_REQUEST_FAILD;
                }
            }
            return false;
        };

        if (tryConfig(config))
        {
            return;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void ShardKVClerk::get(GetReply &_return, const GetParams &params)
{
    _return.code = ErrorCode::ERR_REQUEST_FAILD;
    for (int attempt = 0; attempt < 40; attempt++)
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        ctrlerClerk_.query(qrep, qargs);
        if (qrep.code != ErrorCode::SUCCEED)
        {
            _return.code = qrep.code;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        const auto &config = qrep.config;
        if (config.shard2gid.empty())
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        ShardId sid = key2shard(params.key, config.shard2gid.size());
        GetParams realParams = params;

        auto tryConfig = [&](const Config &cfg)
        {
            if (sid >= static_cast<int>(cfg.shard2gid.size()))
            {
                return false;
            }
            GID gid = cfg.shard2gid[sid];
            if (gid == INVALID_GID)
            {
                _return.code = ErrorCode::ERR_INVALID_SHARD;
                return false;
            }

            auto groupIt = cfg.groupHosts.find(gid);
            if (groupIt == cfg.groupHosts.end() || groupIt->second.empty())
            {
                _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
                return false;
            }

            realParams.gid = gid;
            realParams.sid = sid;
            auto hosts = groupIt->second;
            ClientManager<ShardKVRaftClient> cm(hosts.size(), KV_PRC_TIMEOUT);
            for (int i = 0; i < static_cast<int>(hosts.size()); i++)
            {
                try
                {
                    auto *client = cm.getClient(i, hosts[i]);
                    client->get(_return, realParams);
                    if (_return.code == ErrorCode::SUCCEED || _return.code == ErrorCode::ERR_NO_KEY)
                    {
                        return true;
                    }
                }
                catch (apache::thrift::TException &)
                {
                    cm.setInvalid(i);
                    _return.code = ErrorCode::ERR_REQUEST_FAILD;
                }
            }
            return false;
        };

        if (tryConfig(config))
        {
            return;
        }

        for (int back = config.configNum - 1; back >= 0; back--)
        {
            QueryReply oldRep;
            QueryArgs oldArgs;
            oldArgs.configNum = back;
            ctrlerClerk_.query(oldRep, oldArgs);
            if (oldRep.code != ErrorCode::SUCCEED)
            {
                continue;
            }
            if (tryConfig(oldRep.config))
            {
                return;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void ShardKVClerk::del(DeleteReply &_return, const DeleteParams &params)
{
    _return.code = ErrorCode::ERR_REQUEST_FAILD;
    _return.deleted = 0;

    for (int attempt = 0; attempt < 40; attempt++)
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        ctrlerClerk_.query(qrep, qargs);
        if (qrep.code != ErrorCode::SUCCEED)
        {
            _return.code = qrep.code;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        const auto &config = qrep.config;
        if (config.shard2gid.empty())
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        ShardId sid = key2shard(params.key, config.shard2gid.size());
        if (sid >= static_cast<int>(config.shard2gid.size()))
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            continue;
        }

        GID gid = config.shard2gid[sid];
        if (gid == INVALID_GID)
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            continue;
        }

        auto groupIt = config.groupHosts.find(gid);
        if (groupIt == config.groupHosts.end() || groupIt->second.empty())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            continue;
        }

        DeleteParams real = params;
        real.gid = gid;
        real.sid = sid;
        auto hosts = groupIt->second;
        ClientManager<ShardKVRaftClient> cm(hosts.size(), KV_PRC_TIMEOUT);
        for (int i = 0; i < static_cast<int>(hosts.size()); i++)
        {
            try
            {
                auto *client = cm.getClient(i, hosts[i]);
                client->del(_return, real);
                if (_return.code == ErrorCode::SUCCEED)
                {
                    return;
                }
            }
            catch (apache::thrift::TException &)
            {
                cm.setInvalid(i);
                _return.code = ErrorCode::ERR_REQUEST_FAILD;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void ShardKVClerk::prefixScan(PrefixScanReply &_return, const PrefixScanParams &params)
{
    _return.code = ErrorCode::ERR_REQUEST_FAILD;
    _return.done = true;
    _return.nextCursor.clear();

    for (int attempt = 0; attempt < 40; attempt++)
    {
        QueryReply qrep;
        QueryArgs qargs;
        qargs.configNum = LATEST_CONFIG_NUM;
        ctrlerClerk_.query(qrep, qargs);
        if (qrep.code != ErrorCode::SUCCEED)
        {
            _return.code = qrep.code;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        const auto &config = qrep.config;
        if (config.shard2gid.empty())
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            continue;
        }

        ShardId sid = key2shard(params.prefix, config.shard2gid.size());
        if (sid >= static_cast<int>(config.shard2gid.size()))
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            continue;
        }

        GID gid = config.shard2gid[sid];
        if (gid == INVALID_GID)
        {
            _return.code = ErrorCode::ERR_INVALID_SHARD;
            continue;
        }

        auto groupIt = config.groupHosts.find(gid);
        if (groupIt == config.groupHosts.end() || groupIt->second.empty())
        {
            _return.code = ErrorCode::ERR_NO_SUCH_GROUP;
            continue;
        }

        PrefixScanParams real = params;
        real.gid = gid;
        real.sid = sid;

        auto hosts = groupIt->second;
        ClientManager<ShardKVRaftClient> cm(hosts.size(), KV_PRC_TIMEOUT);
        for (int i = 0; i < static_cast<int>(hosts.size()); i++)
        {
            try
            {
                auto *client = cm.getClient(i, hosts[i]);
                client->prefixScan(_return, real);
                if (_return.code == ErrorCode::SUCCEED)
                {
                    return;
                }
            }
            catch (apache::thrift::TException &)
            {
                cm.setInvalid(i);
                _return.code = ErrorCode::ERR_REQUEST_FAILD;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

ShardId ShardKVClerk::key2shard(const std::string &key, int shardNum) const
{
    // 分片是按照余数放置的
    if (shardNum <= 0)
    {
        return 0;
    }
    if (key.empty())
    {
        return 0;
    }
    return static_cast<unsigned char>(key[0]) % shardNum;
}
