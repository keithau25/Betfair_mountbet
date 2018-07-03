import asyncio
import asyncpg
import pandas as pd
from sshtunnel import SSHTunnelForwarder

async def connectSQL(database, query, port):
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database=database, host='localhost', port=port)
    stmt = await conn.prepare(query)
    columns = [a.name for a in stmt.get_attributes()]
    data = await stmt.fetch()
    await conn.close()
    return pd.DataFrame(data, columns=columns)

def queryViaSSH(database, query):
    tunnel = SSHTunnelForwarder(
        ('35.177.108.175', 22), ssh_username="ubuntu",
        ssh_pkey="./multimatrix_key.pem", remote_bind_address=('localhost', 5432))
    tunnel.start()
    port = tunnel.local_bind_port
    loop = asyncio.get_event_loop()
    dataframe = loop.run_until_complete(connectSQL(database, query, port))
    tunnel.stop()
    return dataframe

database = 'dailyreport'
query = '''SELECT * FROM test_bet_placed limit 2'''

print(queryViaSSH(database, query))
