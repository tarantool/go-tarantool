queue = require 'queue'

box.cfg{
    listen = 3013,
    wal_dir='xlog',
    snap_dir='snap',
}

box.once("init", function()
box.schema.user.create('test', {password = 'test'})
box.schema.func.create('queue.tube.test_queue:ack')
box.schema.func.create('queue.tube.test_queue:put')
box.schema.func.create('queue.tube.test_queue:drop')
box.schema.func.create('queue.tube.test_queue:peek')
box.schema.func.create('queue.tube.test_queue:kick')
box.schema.func.create('queue.tube.test_queue:take')
box.schema.func.create('queue.tube.test_queue:delete')
box.schema.func.create('queue.tube.test_queue:release')
box.schema.func.create('queue.tube.test_queue:bury')
box.schema.func.create('queue.statistics')
box.schema.user.grant('test', 'execute', 'universe')
box.schema.user.grant('test', 'read,write', 'space', '_queue')
box.schema.user.grant('test', 'read,write', 'space', '_schema')
box.schema.user.grant('test', 'read,write', 'space', '_space')
box.schema.user.grant('test', 'read,write', 'space', '_index')
box.schema.user.grant('test', 'read,write', 'space', '_queue_consumers')
box.schema.user.grant('test', 'read,write', 'space', '_priv')
box.schema.user.grant('test', 'read,write', 'space', '_queue_taken')
end)
