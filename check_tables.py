import psycopg2

conn = psycopg2.connect(
    host='localhost', 
    port=5432, 
    user='postgres', 
    password='Admin', 
    database='ct_fda_pipeline'
)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
print('Tables in ct_fda_pipeline database:')
for r in cur.fetchall():
    print(f'  - {r[0]}')
conn.close()
