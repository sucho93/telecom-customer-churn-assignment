import pandas as pd
import psycopg2
import os

PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    database=os.getenv("POSTGRES_DB", "localhost"), 
    user=os.getenv("POSTGRES_USER", "localhost"), 
    password=os.getenv("POSTGRES_PASSWORD", "localhost")
)

def main():
    df = pd.read_csv('/data/customer_churn_data.csv')  # RAW LOAD

    # Connect to PostgreSQL
    con = psycopg2.connect(**PG_CONFIG)
    cur = con.cursor()

    # Recreate staging table
    cur.execute("DROP TABLE IF EXISTS customer_staging;")
    cur.execute("""
    CREATE TABLE customer_staging (
        CustomerID INT PRIMARY KEY,
        Age INT,
        Gender VARCHAR(10),
        Tenure INT,
        MonthlyCharges FLOAT,
        ContractType VARCHAR(32),
        InternetService VARCHAR(32),
        TotalCharges FLOAT,
        TechSupport VARCHAR(32),
        Churn VARCHAR(32)
    );
    """)
    con.commit()

    # Insert raw data
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO customer_staging VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            tuple(row)
        )
    con.commit()
    cur.close()
    con.close()

if __name__ == '__main__':
    main()
