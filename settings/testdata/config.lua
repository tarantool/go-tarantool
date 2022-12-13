-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create,read,write,drop,alter', 'space', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create', 'sequence', nil, { if_not_exists = true })

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
