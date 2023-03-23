#!/usr/bin/env tarantool
-- Details: https:www.tarantool.io/en/doc/latest/reference/configuration/
box.cfg{
    work_dir = '/tmp/tarantool_data/3302',
    memtx_dir = '/tmp/tarantool_data/3302/memtx',
    wal_dir = '/tmp/tarantool_data/3302/wal',
    memtx_memory = 1048576000, -- 100Mb
    sql_cache_size = 104857600, -- 100Mb
    memtx_use_mvcc_engine = true,
    pid_file = "tarantool3302.pid",
    worker_pool_threads = 16,
    iproto_threads = 8,
    listen = 3302,
    checkpoint_interval = 0,
    replication_synchro_quorum = 3,
    replication_synchro_timeout = 10,
    election_mode = 'voter',
    replication = {
       'replicator:password@localhost:3301',
       'replicator:password@localhost:3302',
       'replicator:password@localhost:3303'
    },
    read_only = false,
}

box.once("schema", function()
   box.schema.user.create('replicator', {password = 'password'})
   box.schema.user.grant('replicator', 'replication')

   box.schema.user.passwd('pass')

   box.execute([[CREATE TABLE IF NOT EXISTS test_table  (
                		id VARCHAR(40) primary key ,
                		name VARCHAR(100) NOT NULL,
                		type int NOT NULL
                		);]])

   box.execute([[CREATE UNIQUE INDEX IF NOT EXISTS t_idx_1 ON test_table (name, type);]])
   box.space.TEST_TABLE:alter({is_sync = true})

   print('box.once executed')
end)

function get_cluster_members()
    a = {}
    a["uri"] = 'localhost:3301'
    a["type"] = 'writable'
    b = {}
    b["uri"] = 'localhost:3302'
    b["type"] = 'non-writable'
    c = {}
    c["uri"] = 'localhost:3303'
    c["type"] = 'non-writable'

    return { a, b, c }
end
