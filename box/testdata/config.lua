-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.space.create('space1')

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'super', nil, nil, { if_not_exists = true })

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
    replication = {
        os.getenv("TEST_TNT_LISTEN"),
    },
}
