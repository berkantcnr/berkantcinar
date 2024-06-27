import pandas as pd
import sqlite3

# Connect to the SQLite database
sqlite_db_path = './data/processed/processed_data.sqlite'
conn = sqlite3.connect(sqlite_db_path)

# Query the world_population table
world_population_df = pd.read_sql_query("SELECT * FROM world_population", conn)
world_population_first_10 = world_population_df.head(10)

# Query the air_pollution table
air_pollution_df = pd.read_sql_query("SELECT * FROM air_pollution", conn)
air_pollution_first_10 = air_pollution_df.head(10)

# Close the connection
conn.close()

# Print the first 10 rows of each table
print("First 10 Rows of World Population Data:")
print(world_population_first_10)

print("\nFirst 10 Rows of Air Pollution Data:")
print(air_pollution_first_10)
