import pymysql

try:
    conn = pymysql.connect(
        host='localhost',
        user='heidi_user',
        password='Arschloch1985!',
        database='Ikarus_LVDB_Test',
        port=1433
    )
    print("MySQL Verbindung erfolgreich!")
    conn.close()
except Exception as e:
    print(f"Fehler: {e}")