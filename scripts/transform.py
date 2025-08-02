import pandas as pd
import uuid
import psycopg2
import os

PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    database=os.getenv("POSTGRES_DB", "localhost"), 
    user=os.getenv("POSTGRES_USER", "localhost"), 
    password=os.getenv("POSTGRES_PASSWORD", "localhost")
)

def main():
    # Read from staging table 
    with psycopg2.connect(**PG_CONFIG) as con:
        df = pd.read_sql("SELECT * FROM customer_staging", con)

    # Fill missing values
    df['totalcharges'].fillna(0, inplace=True)
    df['techsupport'].fillna('Unknown', inplace=True)
    df['contracttype'].fillna('Month-to-Month', inplace=True)
    
    # Anonymize CustomerID
    df['AnonCustomerID'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df.drop('customerid', axis=1, inplace=True)

    # Write to processed table
    con = psycopg2.connect(**PG_CONFIG)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_transformed (
        Age INT,
        Gender VARCHAR(10),
        Tenure INT,
        MonthlyCharges FLOAT,
        ContractType VARCHAR(32),
        InternetService VARCHAR(32),
        TotalCharges FLOAT,
        TechSupport VARCHAR(32),
        Churn VARCHAR(32),
        AnonCustomerID UUID PRIMARY KEY
    );
    """)
    con.commit()

    # Insert transformed data
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO customer_transformed VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            tuple(row)
        )
    con.commit()
    cur.close()
    con.close()

if __name__ == '__main__':
    main()
