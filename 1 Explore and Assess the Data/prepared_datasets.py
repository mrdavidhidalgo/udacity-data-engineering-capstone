import pandas as pd
import numpy as np

def prepare_dataset_accidents():
    """
    Prepare dataset accidents,
    standardize field names and select important fields from dataset
    """
    df = pd.read_csv("US_Accidents_June20.csv")

    subDF = df.get(["ID", "Source","TMC",
    "Timezone","Temperature(F)","Stop","Start_Time","End_Time","City",
    "County", "State", "Zipcode","Description","Weather_Timestamp",
    "Weather_Condition","Start_Lat","Start_Lng"])

    preparedDF = subDF.rename(index=str, columns={'Temperature(F)': 'Temperature',
                                              'Start_Lat': 'Latitude',
                                              'Start_Lng': 'Longitude'
                                              })
    print("saving dataset accidents")
    preparedDF.to_csv("../prepared_datasets/accidents.csv")

def prepare_dataset_cities():
    """
    Prepare dataset cities,
    clean repeated data and standardize field names and select important fields from dataset
    """

    df = pd.read_csv("worldcitiespop.csv")
    df = df.drop_duplicates()
    df = df.drop_duplicates(['Country','City','AccentCity','Region'])
    print("saving dataset cities")
    df.to_csv("../prepared_datasets/cities.csv")

if __name__ == '__main__':
    prepare_dataset_accidents()
    prepare_dataset_cities()
