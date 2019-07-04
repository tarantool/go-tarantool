local nodes_load = require("config_load_nodes")

box.cfg {
    listen = 3013,
    wal_dir = 'm1/xlog',
    snap_dir = 'm1/snap',
}

get_cluster_nodes = nodes_load.get_cluster_nodes

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')
end)
