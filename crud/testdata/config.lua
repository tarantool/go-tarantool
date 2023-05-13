-- configure path so that you can run application
-- from outside the root directory
if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    -- Workaround for rocks loading in tarantool 1.10
    -- It can be removed in tarantool > 2.2
    -- By default, when you do require('mymodule'), tarantool looks into
    -- the current working directory and whatever is specified in
    -- package.path and package.cpath. If you run your app while in the
    -- root directory of that app, everything goes fine, but if you try to
    -- start your app with "tarantool myapp/init.lua", it will fail to load
    -- its modules, and modules from myapp/.rocks.
    local fio = require('fio')
    local app_dir = fio.abspath(fio.dirname(arg[0]))
    package.path = app_dir .. '/?.lua;' .. package.path
    package.path = app_dir .. '/?/init.lua;' .. package.path
    package.path = app_dir .. '/.rocks/share/tarantool/?.lua;' .. package.path
    package.path = app_dir .. '/.rocks/share/tarantool/?/init.lua;' .. package.path
    package.cpath = app_dir .. '/?.so;' .. package.cpath
    package.cpath = app_dir .. '/?.dylib;' .. package.cpath
    package.cpath = app_dir .. '/.rocks/lib/tarantool/?.so;' .. package.cpath
    package.cpath = app_dir .. '/.rocks/lib/tarantool/?.dylib;' .. package.cpath
end

local crud = require('crud')
local vshard = require('vshard')

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.user.grant(
    'guest',
    'read,write,execute',
    'universe'
)

local s = box.schema.space.create('test', {
    id = 617,
    if_not_exists = true,
    format = {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned', is_nullable = true},
        {name = 'name', type = 'string'},
    }
})
s:create_index('primary_index', {
    parts = {
        {field = 1, type = 'unsigned'},
    },
})
s:create_index('bucket_id', {
    parts = {
        {field = 2, type = 'unsigned'},
    },
    unique = false,
})

local function is_ready_false()
	return false
end

local function is_ready_true()
	return true
end

rawset(_G, 'is_ready', is_ready_false)

-- Setup vshard.
_G.vshard = vshard
box.once('guest', function()
    box.schema.user.grant('guest', 'super')
end)
local uri = 'guest@127.0.0.1:3013'
local cfg = {
    bucket_count = 300,
    sharding = {
        [box.info().cluster.uuid] = {
            replicas = {
                [box.info().uuid] = {
                    uri = uri,
                    name = 'storage',
                    master = true,
                },
            },
        },
    },
}
vshard.storage.cfg(cfg, box.info().uuid)
vshard.router.cfg(cfg)
vshard.router.bootstrap()

-- Initialize crud.
crud.init_storage()
crud.init_router()
crud.cfg{stats = true}

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create,read,write,drop,alter', 'space', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create', 'sequence', nil, { if_not_exists = true })

-- Set is_ready = is_ready_true only when every other thing is configured.
rawset(_G, 'is_ready', is_ready_true)
