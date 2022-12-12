local nodes_load = require("config_load_nodes")

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
    memtx_use_mvcc_engine = os.getenv("TEST_TNT_MEMTX_USE_MVCC_ENGINE") == 'true' or nil,
}

-- Function to call for getting address list, part of tarantool/multi API.
local get_cluster_nodes = nodes_load.get_cluster_nodes
rawset(_G, 'get_cluster_nodes', get_cluster_nodes)

box.once("init", function()
    local s = box.schema.space.create('test', {
        id = 617,
        if_not_exists = true,
    })
    s:create_index('primary', {type = 'tree', parts = {1, 'string'}, if_not_exists = true})

    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')

    local sp = box.schema.space.create('SQL_TEST', {
        id = 621,
        if_not_exists = true,
        format = {
            {name = "NAME0", type = "unsigned"},
            {name = "NAME1", type = "string"},
            {name = "NAME2", type = "string"},
        }
    })
    sp:create_index('primary', {type = 'tree', parts = {1, 'uint'}, if_not_exists = true})
    sp:insert{1, "test", "test"}
    -- grants for sql tests
    box.schema.user.grant('test', 'create,read,write,drop,alter', 'space')
    box.schema.user.grant('test', 'create', 'sequence')
end)

local function simple_concat(a)
    return a .. a
end

rawset(_G, 'simple_concat', simple_concat)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
