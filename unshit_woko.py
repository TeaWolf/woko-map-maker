"""Unshit woko

Unshit the woko interface by downloading the website listings

Usage: unshit_woko.py [options] map <filename> [--distance=<dist>]
       unshit_woko.py [options] table [<filename>]
       unshit_woko.py [options] get

Options:
    --wait-time=<nominatim-wait-time>  Time to wait between calls to Nominatim [default: 1.2].

"""

import itertools as it
import functools as f
import operator as op
import datetime
import bs4
import pandas as pd
import re

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy import distance

import folium

from tqdm import tqdm

import selenium
from selenium import webdriver
import time

from docopt import docopt

import tinydb
from tinydb_serialization import Serializer
from tinydb_serialization import SerializationMiddleware

# CONFIGURATION
# -------------------------

NOMINATIME_WAIT_TIME = 1.2
# OUTPUT_NAME          = "map.html"
# LISTINGS_NAME        = "listings.html"

# DATA RETRIEVAL
# -------------------------

def get_data(webpage_name):
    """
    Download listings page from the WOKO buletin board
    and save to `webpage_name
    """
    
    browser = webdriver.Firefox()

    browser.get("http://www.woko.ch/en/nachmieter-gesucht")

    time.sleep(2)

    # Need to accept the stupid cookie
    browser.find_element_by_xpath("//button[text()='Accept']").click()

    time.sleep(2)

    browser.find_element_by_xpath("//button[text()='Zürich']").click()

    time.sleep(2)
    
    with open(webpage_name, "w") as f:
        f.write(browser.page_source)

    browser.quit()

    return webpage_name

# get_data(LISTINGS_NAME)

# Get the data
def extract_from_webpage(webpage_name):
    """
Extract the desired fields from the webpage file named `webpage_name
the data is a raw webpage (use `get_data to retrieve)
Returns: pandas.DataFrame with all the data
"""
    with open(webpage_name, 'r') as woko:
        soup = bs4.BeautifulSoup(woko)

    offers = soup.find_all("div", "inserat")  # div of class="inserat"

    def get_price(offer):
        return offer.find("div", "preis").text

    def get_address(offer):

        def is_address(row): return row.contents[1].text == "Address"

        row = next(filter(is_address, offer.find_all("tr")))  # Get address row
        return row.contents[3].text # Get the address text

    def get_link(offer):
        return offer.find("a")["href"]

    def get_availability_data(offer):
        datematch =  re.search("(..)\.(..)\.(....|..)"
                         , offer.find(text=re.compile("as from ..\...\....")))
        return datetime.date(int(datematch.group(3))
                             , int(datematch.group(2))
                             , int(datematch.group(1)))

    def get_added_datetime(offer):
        regex = re.compile("(..)\.(..)\.(....) (..):(..)")
        el = offer.find("span", text = regex)

        datematch = re.search(regex, el.text)
        return datetime.datetime(int(datematch.group(3))
                                 , int(datematch.group(2))
                                 , int(datematch.group(1))
                                 , int(datematch.group(4))
                                 , int(datematch.group(5)))
                              
        

    # Multiply the streams of offers
    # strm1, strm2, strm3 = it.tee(offers, 3)

    # data = zip(map(get_address, strm1)
    #            , map(get_price, strm2)
    #            , map(get_link, strm3))

    # df = pd.DataFrame(data, columns=("address", "price", "link"))

    df = teemapdf({"address":get_address
                   , "price":get_price
                   , "link":get_link
                   , "availability date":get_availability_data
                   , "added on":get_added_datetime}
                  , offers)

    return df

def teemap(functions, data):
    """ 
    teemap all the functions over data
    """

    streams = it.tee(data, len(functions))
    return zip(*[map(function, stream) for function, stream in zip(functions, streams)])

def teemapdf(maps, itdata):
    """
    map all the functions in {name: function} = maps to the 
    iterable data in data
    return a pandas dataframe
    """

    names     = maps.keys()
    functions = [maps[name] for name in names]
    
    transformed = teemap(functions, itdata)
    
    return pd.DataFrame(transformed, columns = names)
    

# CORDINATE MAPPING
# -------------------------

def add_coordinates(data, nominatim_wait_time=1.2):
    """
Adds geolocation information to extracted data
Also adds distance to reference location (ETH Honggerberg)
Returns: pandas.DataFrame with coordinates attached (operation done in place)
"""
    
    geolocator = Nominatim(user_agent="greenllama-woko-app") #, country_codes="Switzerland")
    geocode    = RateLimiter(geolocator.geocode, min_delay_seconds=nominatim_wait_time)

    tqdm.pandas()                   # Get progress bar on geocoding

    location = data['address'].progress_apply(geocode)
    data['point']    = location.apply(lambda x: tuple(x.point[0:2]) if x else None)

    data['distance'] = data['point'].apply(lambda x: distance.distance(x, (47.4082, 8.5084)).km)


    return data

# FOLIUM MAP CREATION
# -------------------------

def make_map(geolocated_data, mapname, distance=None):
    """
Make a folium map using the geolocated_data
and save in mapname
"""
# Create the map using folium
    m = folium.Map(location=[47.3728, 8.54443])

    # add the markers
    subtable = geolocated_data[['point', 'address', 'price', 'link']]
    if distance is not None:
        subtable = subtable.loc(geolocated_data.distance <= distance)
        
    for _, (point, addr, price, link) in subtable.iterrows():
        if point:
            folium.Marker(point[0:2], popup = "<a href=\"{link}\">{addr}</a> | <b>{price}</b>".format(link=link, addr=addr, price=price)).add_to(m)


    m.save(mapname)

    return mapname


# class DatetimeStorageMiddleWare(MiddleWare):
#     def __init__(seld, storage_cls):
#         super(self).__init__(storage_cls)

#     def read(self):
#         data = selr.storage.read()

#         for table_name in data:
#             table_data = data[table_name]

#             for doc_id in table:
#                 item = table_data[doc_id]

#                 if item == 
#                     de
#         return data
    
#     def write(self, data):
        
#         self.storage.write(data)

## Database stuff
class DateTimeSerializer(Serializer):
    OBJ_CLASS = datetime  # The class this serializer handles

    def encode(self, obj):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

    def decode(self, s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')

def initdb(name):
    # TODO get serialization to work, it seems borken
    
    serialization = SerializationMiddleware()
    serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

    return tinydb.TinyDB(name, storage=serialization)

def update_database(raw_data, wait_time=1.2):
    """
    update the tinyDB with new values from the webpage
    """

    db = initdb('db.json')


    # TODO make this only get the location of the newly added offers
    # extraction = add_coordinates(extract_from_webpage(raw_data), nominatim_wait_time=wait_time)
    # TODO put this back to the above
    extraction = extract_from_webpage(raw_data)
    
    # Match offers based on added date, availability, address, price
    for _, row in extraction.iterrows():
        query = tinydb.Query()
        result = db.search(f.reduce(op.and_
                                    , (query[field] == row[field] for field in row.index)))

        print(result)
    

if __name__ == "__main__":
    arguments = docopt(__doc__)

    

    def get_full_data(): return data = add_coordinates(extract_from_webpage(get_data("listings.html"))
                                                       , nominatim_wait_time=float(arguments["--wait-time"]))

    if arguments['map']:

        make_map(get_full_data(), arguments['<filename>'], distance = arguments['--distance'])

    if arguments['table']:

        data = get_full_data()
        if arguments['<filename>']:
            data.to_csv(arguments['<filename>'])
        else:
            data.set_option('display.max_rows', None)
            data.set_option('display.max_columns', None)
            data.set_option('display.width', None)
            data.set_option('display.max_colwidth', -1)

            print(data)

    if arguments['get']:
        update_database(get_data("listings.html")
                        , wait_time=arguments['--wait-time']
            
