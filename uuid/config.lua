local fio = require('fio')
local uuid = require('uuid')
local msgpack = require('msgpack')

local work_dir = 'work_dir'

fio.rmtree(work_dir)
fio.mktree(work_dir)

box.cfg{
    listen = 3013,
    work_dir = work_dir,
}

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })

local uuid_msgpack_supported = pcall(msgpack.encode, uuid.new())
if not uuid_msgpack_supported then
    error('UUID unsupported, use Tarantool 2.4.1 or newer')
end

local s = box.schema.space.create('testUUID', {
    id = 524,
    if_not_exists = true,
})
s:create_index('primary', {
    type = 'tree',
    parts = {{ field = 1, type = 'uuid' }},
    if_not_exists = true
})
s:truncate()

box.schema.user.grant('test', 'read,write', 'space', 'testUUID', { if_not_exists = true })

s:insert({ uuid.fromstr("c8f0fa1f-da29-438c-a040-393f1126ad39") })
