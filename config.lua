box.cfg{
    listen = 3013,
    wal_dir='xlog',
    snap_dir='snap',
}
local s = box.schema.space.create('test', {if_not_exists = true})
local i = s:create_index('primary', {type = 'hash', parts = {1, 'NUM'}, if_not_exists = true})

box.schema.user.grant('guest', 'read,write,execute', 'universe')

-- auth testing: access control
if not box.schema.user.exists('test') then
    box.schema.user.create('test', {password = 'test'})
    box.schema.user.grant('test', 'read,write,execute', 'universe')
end

local console = require 'console'
console.listen '0.0.0.0:33015'

box.schema.user.revoke('guest', 'read,write,execute', 'universe')

