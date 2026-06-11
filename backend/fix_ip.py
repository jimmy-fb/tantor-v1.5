import sqlite3

conn = sqlite3.connect('tantor.db')
c = conn.cursor()
c.execute("UPDATE hosts SET ip_address = '192.168.3.150' WHERE id = 'db8d612f-3f7e-4c49-b506-687228bfbe83'")
conn.commit()
conn.close()
print("Fixed IP address to 150 in DB")
