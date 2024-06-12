import os
import sqlite3
import pytest
import pandas as pd
from pipeline import (
    download_dataset, get_correct_file_path, etl_world_population, 
    etl_air_pollution, sql_load, main
)

# Defined the expected output files
expected_files = [
    "./data/processed/processed_data.sqlite"
]

expected_tables = {
    "./data/processed/processed_data.sqlite": ["world_population", "air_pollution"]
}

# Defined expected data types for integrity tests
expected_columns_types = {
    "./data/processed/processed_data.sqlite": {
        "world_population": {
            "Country": "object",
            "Population_2022": "int64"
        },
        "air_pollution": {
            "Country": "object",
            "AQI_Value": "int64"
        }
    }
}

# Testing to output files 
@pytest.mark.parametrize("filepath", expected_files)
def test_file_existence(filepath):
    print(f"Checking existence of file: {filepath}")
    assert os.path.isfile(filepath), f"File {filepath} does not exist."

# Testing tables in SQLite 
@pytest.mark.parametrize("filepath,tables", expected_tables.items())
def test_sqlite_tables(filepath, tables):
    print(f"Checking tables in SQLite file: {filepath}")
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = [row[0] for row in cursor.fetchall()]
    print(f"Existing tables: {existing_tables}")
    conn.close()
    for table in tables:
        assert table in existing_tables, f"Table {table} not found in {filepath}."

# Test to check data in SQLite files
@pytest.mark.parametrize("filepath,tables_columns", expected_columns_types.items())
def test_data_integrity(filepath, tables_columns):
    print(f"Checking data integrity for file: {filepath}")
    conn = sqlite3.connect(filepath)
    for table, columns_types in tables_columns.items():
        print(f"Checking table: {table}")
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        print(f"Columns in {table}: {df.columns}")
        for column, dtype in columns_types.items():
            print(f"Checking column: {column} (expected type: {dtype})")
            assert column in df.columns, f"Column {column} not found in table {table} of {filepath}"
            actual_dtype = df[column].dtype
            print(f"Actual type of column {column}: {actual_dtype}")
            assert str(actual_dtype) == dtype, f"Column {column} in table {table} of {filepath} has incorrect type {actual_dtype}, expected {dtype}"
    conn.close()

# Missing value tests for columns 
@pytest.mark.parametrize("filepath,tables_columns", expected_columns_types.items())
def test_missing_values(filepath, tables_columns):
    print(f"Checking for missing values in file: {filepath}")
    conn = sqlite3.connect(filepath)
    for table, columns_types in tables_columns.items():
        print(f"Checking table: {table} for missing values")
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        for column in columns_types.keys():
            missing_values = df[column].isnull().sum()
            print(f"Column {column} has {missing_values} missing values")
            assert missing_values == 0, f"Column {column} in table {table} of {filepath} has {missing_values} missing values"
    conn.close()


# Unique value test
@pytest.mark.parametrize("filepath", expected_files)
def test_unique_values(filepath):
    print(f"Checking unique values in file: {filepath}")
    conn = sqlite3.connect(filepath)
    df_world_population = pd.read_sql_query("SELECT * FROM world_population", conn)
    
    # I merge the country column because I had multiple and I am testing each country column is unique or not
    print("Checking for unique Country values in world_population table")
    assert df_world_population["Country"].is_unique, "Country column in world_population table has duplicate values"
    
    conn.close()

if __name__ == "__main__":
    pytest.main()
