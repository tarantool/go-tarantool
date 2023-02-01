 local crud = require('crud')
 local vshard = require('vshard')

 -- Do not set listen for now so connector won't be
 -- able to send requests until everything is configured.
 box.cfg{
     work_dir = os.getenv("TEST_TNT_WORK_DIR"),
 }

 box.schema.user.grant(
     'guest',
     'read,write,execute',
     'universe'
 )

 local s = box.schema.space.create('test', {
     id = 617,
     if_not_exists = true,
     format = {
         {name = 'id', type = 'unsigned'},
         {name = 'bucket_id', type = 'unsigned', is_nullable = true},
         {name = 'name', type = 'string'},
     }
 })
 s:create_index('primary_index', {
     parts = {
         {field = 1, type = 'unsigned'},
     },
 })
 s:create_index('bucket_id', {
     parts = {
         {field = 2, type = 'unsigned'},
     },
     unique = false,
 })

 -- Setup vshard.
 _G.vshard = vshard
 box.once('guest', function()
     box.schema.user.grant('guest', 'super')
 end)
 local uri = 'guest@127.0.0.1:3013'
 local cfg = {
     bucket_count = 300,
     sharding = {
         [box.info().cluster.uuid] = {
             replicas = {
                 [box.info().uuid] = {
                     uri = uri,
                     name = 'storage',
                     master = true,
                 },
             },
         },
     },
 }
 vshard.storage.cfg(cfg, box.info().uuid)
 vshard.router.cfg(cfg)
 vshard.router.bootstrap()

 -- Initialize crud.
 crud.init_storage()
 crud.init_router()
 crud.cfg{stats = true}

 box.schema.user.create('test', { password = 'test' , if_not_exists = true })
 box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })
 box.schema.user.grant('test', 'create,read,write,drop,alter', 'space', nil, { if_not_exists = true })
 box.schema.user.grant('test', 'create', 'sequence', nil, { if_not_exists = true })

 -- Set listen only when every other thing is configured.
 box.cfg{
     listen = os.getenv("TEST_TNT_LISTEN"),
 }
