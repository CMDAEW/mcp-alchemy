import pymssql

try:
    # Exakte HeidiSQL Einstellungen
    conn = pymssql.connect(
        server='127.0.0.1',        # Hostname/IP wie in HeidiSQL
        user='heidi_user',
        password='Arschloch1985!',
        database='Ikarus_LVDB_Test',
        port=1433,                 # Port explizit angeben
        appname='MSOLEDBSQL'       # Bibliothek wie in HeidiSQL
    )
    
    print("Verbindung erfolgreich!")
    cursor = conn.cursor()
    cursor.execute('SELECT @@VERSION')
    row = cursor.fetchone()
    print("SQL Server Version:", row[0])
    conn.close()

except Exception as e:
    print(f"Fehler: {str(e)}")