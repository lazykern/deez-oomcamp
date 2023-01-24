import psycopg2

conn = psycopg2.connect(
    "dbname='taxi' user='postgres' password='docker' host='127.0.0.1' port='5432'"
)

cur = conn.cursor()

with open("create_table.sql", "r") as f:
    cur.execute(f.read())

conn.commit()
