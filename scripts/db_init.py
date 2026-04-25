import os
import psycopg2 from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def main():
    sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "create_tables.sql")
    with open(sql_path) as f:
        ddl = f.read()

    print("Connecting to PostgreSQL...")
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("Running DDL...")
    cur.execute(ddl)

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    print(f"\nDatabase initialised. Tables found: {', '.join(tables)}")


if __name__ == "__main__":
    main()