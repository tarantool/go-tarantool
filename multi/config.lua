local nodes_load = require("config_load_nodes")

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

local major = tonumber(string.sub(_TARANTOOL, 1, 1))
local minor = tonumber(string.sub(_TARANTOOL, 3, 3))

local num_type = "unsigned"
local string_type = "string"
if major == 1 and minor == 6 then
    num_type = "NUM"
    string_type = "STR"
end

-- Function to call for getting address list, part of tarantool/multi API.
local get_cluster_nodes = nodes_load.get_cluster_nodes
rawset(_G, 'get_cluster_nodes', get_cluster_nodes)

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')

    local sp = box.schema.space.create('SQL_TEST', {
        id = 521,
        if_not_exists = true,
        format = {
            {name = "NAME0", type = num_type},
            {name = "NAME1", type = string_type},
            {name = "NAME2", type = string_type},
        }
    })
    sp:create_index('primary', {type = 'tree', parts = {1, num_type}, if_not_exists = true})
    sp:insert{1, "test", "test"}
    -- grants for sql tests
    if major >= 2 then
        box.schema.user.grant('test', 'create,read,write,drop,alter', 'space')
        box.schema.user.grant('test', 'create', 'sequence')
    end
end)

local function simple_incr(a)
    return a + 1
end

rawset(_G, 'simple_incr', simple_incr)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
