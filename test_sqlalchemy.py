from sqlalchemy import create_engine
import urllib.parse

# Die exakten Parameter aus dem erfolgreichen Test
params = {
    'server': '127.0.0.1',
    'user': 'heidi_user',
    'password': 'Arschloch1985!',
    'database': 'Ikarus_LVDB_Test',
    'port': 1433,
    'appname': 'MSOLEDBSQL'
}

# SQLAlchemy connection string bauen
connection_string = (
    f"mssql+pymssql://{params['user']}:{params['password']}"
    f"@{params['server']}:{params['port']}/{params['database']}"
    f"?charset=utf8&appname={params['appname']}"
)

print("Trying connection string:", connection_string)

engine = create_engine(connection_string)
try:
    with engine.connect() as conn:
        result = conn.execute("SELECT @@VERSION").scalar()
        print("Success!")
        print("Version:", result)
except Exception as e:
    print("Failed:", str(e)) 