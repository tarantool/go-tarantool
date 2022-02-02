local decimal = require('decimal')
local msgpack = require('msgpack')

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })

local decimal_msgpack_supported = pcall(msgpack.encode, decimal.new(1))
if not decimal_msgpack_supported then
    error('Decimal unsupported, use Tarantool 2.2 or newer')
end

local s = box.schema.space.create('testDecimal', {
    id = 524,
    if_not_exists = true,
})
s:create_index('primary', {
    type = 'TREE',
    parts = {
        {
            field = 1,
            type = 'decimal',
        },
    },
    if_not_exists = true
})
s:truncate()

box.schema.user.grant('test', 'read,write', 'space', 'testDecimal', { if_not_exists = true })

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}

require('console').start()
