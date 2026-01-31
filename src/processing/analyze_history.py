import duckdb

# Establish an in-memory DuckDB connection
con = duckdb.connect(database=':memory:')

data_path = "data/raw/bitcoin_data_lake/**/*.parquet"

# Query the Parquet files and display the first 5 rows
con.sql(f"SELECT * FROM '{data_path}' LIMIT 5").show()

# Perform basic analysis: min, max, and average price
query = f"""
    SELECT 
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price
    FROM '{data_path}'
"""

con.sql(query).show()


