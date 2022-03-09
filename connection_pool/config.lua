-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')

    local s = box.schema.space.create('testPool', {
        id = 520,
        if_not_exists = true,
        format = {
            {name = "key", type = "string"},
            {name = "value", type = "string"},
        },
    })
    s:create_index('pk', {
        type = 'tree',
        parts = {{ field = 1, type = 'string' }},
        if_not_exists = true
    })
end)

local function simple_incr(a)
    return a + 1
end

rawset(_G, 'simple_incr', simple_incr)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
