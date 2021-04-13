import requests

LISTING_LOCATIONS = ["Amsterdam","Antwerp","Asheville","Athens","Austin",
                     "Bangkok", "Barcelona", "Barossa_Valley_Australia", "Barwon_South_West_Australia",
                     "Beijing_China", "Belize_Belize","Bergamo_Italy", "Berlin_Germany", "Bologna_Italy",
                     "Bristol_England","Miami_USA","Brussels_Belgium", "Buenos_Aires_Argentina"]
#Pull data from beginning of 2020 for pre pandemic data
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
"http://data.insideairbnb.com/argentina/ciudad-aut%C3%B3noma-de-buenos-aires/buenos-aires/2020-01-23/data/listings.csv.gz"
]


def ingest_csv(url, location):
    req = requests.get(url)
    url_content = req.content
    csv_file = open(f"../data/{location}.csv.gz", "wb")

    csv_file.write(url_content)
    csv_file.close()


if __name__ == "__main__":
    result=zip(LISTING_URLS,LISTING_LOCATIONS)
    for x,y in result:
        ingest_csv(x,y)
