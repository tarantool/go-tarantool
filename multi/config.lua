local nodes_load = require("config_load_nodes")

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

get_cluster_nodes = nodes_load.get_cluster_nodes

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')
end)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
