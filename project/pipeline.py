import pandas as pd
import sqlite3
import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(asctime)s - %(levelname)s - %(message)s')

# Kaggle API
api = KaggleApi()
api.authenticate()

# File paths and datasets
world_population_dataset = 'iamsouravbanerjee/world-population-dataset'
world_population_file = 'world_population.csv'
air_pollution_dataset = 'hasibalmuzdadid/global-air-pollution-dataset'
air_pollution_file = 'global air pollution dataset.csv'

data_dir = './data'
os.makedirs(data_dir, exist_ok=True)

# Download datasets from Kaggle
def download_dataset(dataset, filename, dest_path):
    api.dataset_download_file(dataset, filename, path=dest_path)
    zip_path = os.path.join(dest_path, f"{filename}.zip")
    csv_path = os.path.join(dest_path, filename)
    if os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_path)
        os.remove(zip_path)
    return csv_path

# File name controlling 
def get_correct_file_path(dataset, dest_path):
    api.dataset_download_files(dataset, path=dest_path, unzip=True)
    for file in os.listdir(dest_path):
        if file.endswith('.csv'):
            return os.path.join(dest_path, file)
    raise FileNotFoundError(f"No CSV file found in the downloaded dataset: {dataset}")

def etl_world_population(input_path):
    df = pd.read_csv(input_path)
    df = df[['Country/Territory', 'Continent', '2022 Population', 'Area (km²)', 'Density (per km²)', 'Growth Rate', 'World Population Percentage']]
    df.dropna(inplace=True)
    df.columns = ['Country', 'Continent', 'Population_2022', 'Area(km²)', 'Density(per km²)', 'Growth_Rate', 'World_Population_Percentage']
    return df

def etl_air_pollution(input_path):
    df = pd.read_csv(input_path)
    df = df.drop(columns=['City']).dropna()
    df.columns = [
        'Country', 'AQI_Value', 'AQI_Category', 
        'CO_AQI_Value', 'CO_AQI_Category', 'Ozone_AQI_Value', 'Ozone_AQI_Category', 
        'NO2_AQI_Value', 'NO2_AQI_Category', 'PM25_AQI_Value', 'PM25_AQI_Category'
    ]
    return df

def sql_load(df, path, table):
    with sqlite3.connect(path) as conn:
        df.to_sql(table, conn, if_exists='replace', index=False)

def main():
    # Data download
    world_population_path = download_dataset(world_population_dataset, world_population_file, data_dir)
    air_pollution_path = get_correct_file_path(air_pollution_dataset, data_dir)

    # data process 
    world_population_data = etl_world_population(world_population_path)
    air_pollution_data = etl_air_pollution(air_pollution_path)

    # processed data directory
    processed_data_dir = os.path.join(data_dir, 'processed')
    os.makedirs(processed_data_dir, exist_ok=True)

    #loaded to sqlite
    sqlite_db_path = os.path.join(processed_data_dir, 'processed_data.sqlite')
    sql_load(world_population_data, sqlite_db_path, 'world_population')
    sql_load(air_pollution_data, sqlite_db_path, 'air_pollution')

    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
