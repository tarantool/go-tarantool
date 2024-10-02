-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg {
    work_dir = os.getenv("TEST_TNT_WORK_DIR")
}

box.schema.user.create('test', {
    password = 'test',
    if_not_exists = true
})
box.schema.user.grant('test', 'execute', 'universe', nil, {
    if_not_exists = true
})

local s = box.schema.space.create('testArrow', {
    if_not_exists = true
})
s:create_index('primary', {
    type = 'tree',
    parts = {{
        field = 1,
        type = 'integer'
    }},
    if_not_exists = true
})
s:truncate()

box.schema.user.grant('test', 'read,write', 'space', 'testArrow', {
    if_not_exists = true
})

-- Set listen only when every other thing is configured.
box.cfg {
    listen = os.getenv("TEST_TNT_LISTEN")
}
