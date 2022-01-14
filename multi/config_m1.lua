local fio = require('fio')
local nodes_load = require("config_load_nodes")

local work_dir = 'work_dir_1'

fio.rmtree(work_dir)
fio.mktree(work_dir)

box.cfg {
    listen = 3013,
    work_dir = work_dir,
}

get_cluster_nodes = nodes_load.get_cluster_nodes

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')
end)
