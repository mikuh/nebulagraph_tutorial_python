from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

# define a config
config = Config()

# init connection pool
connection_pool = ConnectionPool()

# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

# option 2 with session_context, session will be released automatically
with connection_pool.session_context('root', 'nebula') as session:
    session.execute('CREATE SPACE IF NOT EXISTS test_basketballplayer (partition_num=15, replica_factor=1, vid_type=FIXED_STRING(32));')
    result = session.execute('show spaces;')
    print(result)

# close the pool
connection_pool.close()
