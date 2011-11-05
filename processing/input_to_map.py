import psycopg2

conn_string = "host='localhost' dbname='shish' user='shish' password='shish'"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS input")
cursor.execute("CREATE TABLE input(time datetime default now, name text, body text)")
cursor.execute("INSERT INTO input(name, body) VALUES (%s, %s)", ("cats", "hello gentlemen"))
cursor.execute("INSERT INTO input(name, body) VALUES (%s, %s)", ("cats", "all your base are belong to us"))
cursor.commit()

