import psycopg2
import pandas as pd

# Load CSV
csv_file = r"F:/GUVI/guvi-2 projectss/gu/cleaned_agriculture_data.csv"
df = pd.read_csv(csv_file)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="agriculture_db",
    user="postgres",
    password="Raghul200168",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Drop table if exists
cur.execute('DROP TABLE IF EXISTS agriculture;')

# Dynamically create table based on CSV columns
create_cols = ', '.join([f'"{col}" TEXT' for col in df.columns])
cur.execute(f'CREATE TABLE agriculture ({create_cols});')

# Insert rows
cols = ','.join([f'"{col}"' for col in df.columns])
vals = [tuple(x) for x in df.to_numpy()]
query = f'INSERT INTO agriculture ({cols}) VALUES ({",".join(["%s"] * len(df.columns))})'
cur.executemany(query, vals)

# Commit and close
conn.commit()
cur.close()
conn.close()

print("✅ CSV successfully loaded into PostgreSQL with all columns")


