import requests
#from spark_airbnb.upload import setup_path, upload, azure_credentials
from spark_airbnb.upload_aws import upload_file
import argparse
import os
from azure.storage.blob import BlockBlobService
import pandas as pd

LISTING_LOCATIONS = [
    "Amsterdam",
    "Antwerp",
    "Asheville",
    "Athens",
    "Austin",
    "Bangkok",
    "Barcelona",
    "Barossa_Valley_Australia",
    "Barwon_South_West_Australia",
    "Beijing_China",
    "Belize_Belize",
    "Bergamo_Italy",
    "Berlin_Germany",
    "Bologna_Italy",
    "Bristol_England",
    "Miami_USA",
    "Brussels_Belgium",
    "Buenos_Aires_Argentina",
]
USA_LISTING_LOCATIONS = [
    "Asheville",
    "Austin",
    "Miami_USA",
    "Chicago",
    "Las_Vegas",
    "Dallas",
    "Denver",
    "Hawaii",
    "Jersey_City",
    "Los_angeles",
    "Nashville",
    "New_Orleans",
    "New_York",
    "Oakland",
    "Pacific_Grove",
    "Portland",
    "Rhode_Island",
    "Salem",
    "San_Diego",
    "San_Fransisco",
    "San_Mateo",
    "San_Jose",
    "Santa_Cruz",
    "Seattle",
    "Twin_Cities",
    "Washington_DC",
]
# Pull data from beginning of 2020 for pre pandemic data
USA_LISTING_URLS = [
    "http://data.insideairbnb.com/united-states/nc/asheville/2020-01-28/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/tx/austin/2020-01-12/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/fl/broward-county/2020-01-15/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/il/chicago/2020-01-19/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/nv/clark-county-nv/2020-01-26/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/tx/dallas/2021-02-21/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/co/denver/2020-01-30/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/hi/hawaii/2020-01-03/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/nj/jersey-city/2020-01-22/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/los-angeles/2020-01-04/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/tn/nashville/2021-02-19/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/la/new-orleans/2020-01-04/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ny/new-york-city/2020-01-03/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/oakland/2021-02-20/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/pacific-grove/2020-01-31/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/or/portland/2021-02-20/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ri/rhode-island/2020-01-31/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/or/salem-or/2020-01-09/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/san-diego/2020-01-15/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/san-francisco/2020-01-04/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/san-mateo-county/2021-02-21/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/santa-clara-county/2021-02-21/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/ca/santa-cruz-county/2020-01-31/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/wa/seattle/2020-01-15/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/mn/twin-cities-msa/2020-01-04/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/dc/washington-dc/2020-01-19/data/listings.csv.gz",
]
LISTING_URLS = [
    "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2020-01-05/data/listings.csv.gz",
    "http://data.insideairbnb.com/belgium/vlg/antwerp/2020-01-27/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/nc/asheville/2020-01-28/data/listings.csv.gz",
    "http://data.insideairbnb.com/greece/attica/athens/2020-01-13/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/tx/austin/2020-01-12/data/listings.csv.gz",
    "http://data.insideairbnb.com/thailand/central-thailand/bangkok/2020-03-18/data/listings.csv.gz",
    "http://data.insideairbnb.com/spain/catalonia/barcelona/2020-01-10/data/listings.csv.gz",
    "http://data.insideairbnb.com/australia/sa/barossa-valley/2020-01-22/data/listings.csv.gz",
    "http://data.insideairbnb.com/australia/vic/barwon-south-west-vic/2020-01-26/data/listings.csv.gz",
    "http://data.insideairbnb.com/china/beijing/beijing/2020-01-21/data/listings.csv.gz",
    "http://data.insideairbnb.com/belize/bz/belize/2020-01-28/data/listings.csv.gz",
    "http://data.insideairbnb.com/italy/lombardia/bergamo/2020-01-31/data/listings.csv.gz",
    "http://data.insideairbnb.com/germany/be/berlin/2020-01-10/data/listings.csv.gz",
    "http://data.insideairbnb.com/italy/emilia-romagna/bologna/2020-01-12/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-kingdom/england/bristol/2020-01-19/data/listings.csv.gz",
    "http://data.insideairbnb.com/united-states/fl/broward-county/2020-01-15/data/listings.csv.gz",
    "http://data.insideairbnb.com/belgium/bru/brussels/2020-01-13/data/listings.csv.gz",
    "http://data.insideairbnb.com/argentina/ciudad-aut%C3%B3noma-de-buenos-aires/buenos-aires/2020-01-23/data/listings.csv.gz",
]


def ingest_csv(url, location):
    req = requests.get(url)
    url_content = req.content
    csv_file = open(f"data/{location}.csv.gz", "wb")

    csv_file.write(url_content)
    csv_file.close()


def set_city(path, csv_file, city):
    df = pd.read_csv(f"{path}/{csv_file}")
    df["Metro"] = city
    df.to_csv(f"{path}/{csv_file}")

def setup_path(directory):
    dir_name = directory
    path = f"{dir_name}"
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Airbnb data to Azure or AWS")
    parser.add_argument(
        "-n",
        "--name",
        action="store",
        default="us-airbnb-data",
        help="Name of Bucket or Container to upload data to",
    )
    parser.add_argument(
        "--all_data",
        default=False,
        action="store_true",
        help="Downloads all world cities data if flag is set else downloads only \
                    US city data",
    )
    parser.add_argument(
       "- c",
       "--cloud",
       default="aws",
       action="store",
       help="choose aws or azure",
    )
    args = parser.parse_args()
    
    if args.cloud == "azure":
        account_name, account_key = azure_credentials()
        block_blob_service = BlockBlobService(
            account_name=account_name, account_key=account_key
        )
    container = args.name
    if args.all_data == False:

        result = zip(USA_LISTING_URLS, USA_LISTING_LOCATIONS)
        for x, y in result:
            ingest_csv(x, y)
            file_path = setup_path("data")
            data_file_names = os.listdir(file_path)
            set_city(file_path, data_file_names[0], y)
            if args.cloud == "azure":
                upload(data_file_names, container, file_path)
            if args.cloud == "aws":
                upload_file(f"{file_path}/{data_file_names[0]}", container,data_file_names[0])
            os.remove(f"{file_path}/{data_file_names[0]}")
