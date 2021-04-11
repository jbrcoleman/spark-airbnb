#from spark_airbnb.ingest import ingest_csv
from spark_airbnb.test import add
import os
from pathlib import Path

def test_add():
    test=add(5,10)
    assert test == 15


#def test_ingest_csv():
#    url = "http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2020-01-05/data/listings.csv.gz"
#    location = 'Amsterdam_test'
#    ingest_csv(url,location)
#    path = Path(f'../data/{location}.csv')
#    assert path.is_file()
#    os.remove(path) 
