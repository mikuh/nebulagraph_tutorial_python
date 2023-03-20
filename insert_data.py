# 这里我们演示如何把data中的示例数据集导入到数据库中
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import csv

# define a config
config = Config()

# init connection pool
connection_pool = ConnectionPool()

# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 9669)], config)


def insert_vertex(session, v_tag, v_id, **kwargs):
    keys = ",".join(kwargs.keys())
    values = ",".join([f"'{v}'" if isinstance(v, str) else f"{v}" for v in kwargs.values()])
    command = f'INSERT VERTEX {v_tag} ({keys}) VALUES "{v_id}":({values});'
    session.execute(command)


def insert_edge(session, e_type, _from, _to, **kwargs):
    keys = ",".join(kwargs.keys())
    values = ",".join([f"'{v}'" if isinstance(v, str) else f"{v}" for v in kwargs.values()])
    command = f'INSERT EDGE {e_type}({keys}) VALUES "{_from}" -> "{_to}":({values});'
    session.execute(command)


# insert_vertex("player", "player100", name="Tim Duncan", age=42)
# # option 2 with session_context, session will be released automatically
with connection_pool.session_context('root', 'nebula') as session:
    session.execute("use test_basketballplayer;")
    # 插入节点
    with open("data/vertex_player.csv", 'r', encoding='utf-8') as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            insert_vertex(session, "player", row[0], age=int(row[1]), name=row[2])
    with open("data/vertex_team.csv", 'r', encoding='utf-8') as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            insert_vertex(session, "team", row[0], name=row[1])
    print(session.execute("match (v:player) return v limit 3"))
    print(session.execute("match (v:team) return v limit 3"))
    # 插入关系
    with open("data/edge_follow.csv", 'r', encoding='utf-8') as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            insert_edge(session, "follow", row[0], row[1], degree=int(row[2]))
    with open("data/edge_serve.csv", 'r', encoding='utf-8') as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            insert_edge(session, "serve", row[0], row[1], start_year=int(row[2]), end_year=int(row[3]))

    print(session.execute("MATCH ()-[e:follow]->() RETURN e LIMIT 3"))
    print(session.execute("MATCH ()-[e:serve]->() RETURN e LIMIT 3"))

# close the pool
connection_pool.close()
