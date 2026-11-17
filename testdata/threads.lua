-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
    app_threads = 2,
}

box.once("init", function()
    box.schema.user.passwd('admin', 'secret')

    local s = box.schema.space.create('test', {
        if_not_exists = true,
    })
    s:create_index('primary', {
        type = 'tree',
        parts = {1, 'uint'},
        if_not_exists = true,
    })
end)

box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
