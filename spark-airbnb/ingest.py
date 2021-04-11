import requests

LISTING_LOCATIONS = ["Amsterdam","Antwerp","Ashville","Athens","Austin"]
#Pull data from beginning of 2020 for pre pandemic data
LISTING_URLS = [
    "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2020-01-05/data/listings.csv.gz",
"http://data.insideairbnb.com/belgium/vlg/antwerp/2020-01-27/data/listings.csv.gz",
"http://data.insideairbnb.com/united-states/nc/asheville/2020-01-28/data/listings.csv.gz",
"http://data.insideairbnb.com/greece/attica/athens/2020-01-13/data/listings.csv.gz",
"http://data.insideairbnb.com/united-states/tx/austin/2020-01-12/data/listings.csv.gz"

]


def ingest_csv(url, location):
    req = requests.get(url)
    url_content = req.content
    csv_file = open(f"../data/{location}.csv", "wb")

    csv_file.write(url_content)
    csv_file.close()


if __name__ == "__main__":
    result=zip(LISTING_URLS,LISTING_LOCATIONS)
    for x,y in result:
        ingest_csv(x,y)
