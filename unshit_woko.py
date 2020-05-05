import itertools as it
import bs4
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import folium

from tqdm import tqdm

import selenium
from selenium import webdriver
import time

# CONFIGURATION
# -------------------------

NOMINATIME_WAIT_TIME = 1.2
OUTPUT_NAME          = "map.html"
LISTINGS_NAME        = "listings.html"

# DATA RETRIEVAL
# -------------------------

def get_data(webpage_name):
    "Download listings page from the WOKO buletin board"
    
    browser = webdriver.Firefox()

    browser.get("http://www.woko.ch/en/nachmieter-gesucht")

    time.sleep(2)

    # Need to accept the stupid cookie
    browser.find_element_by_xpath("//button[text()='Accept']").click()

    time.sleep(2)

    browser.find_element_by_xpath("//button[text()='Zürich']").click()

    with open(webpage_name, "w") as f:
        f.write(browser.page_source)

    browser.quit()

    return webpage_name

get_data(LISTINGS_NAME)

# Get the data
with open(LISTINGS_NAME, 'r') as woko:
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

# Multiply the streams of offers
strm1, strm2, strm3 = it.tee(offers, 3)

data = zip(map(get_address, strm1), map(get_price, strm2), map(get_link, strm3))

df = pd.DataFrame(data, columns=("address", "price", "link"))


# CORDINATE MAPPING
# -------------------------


geolocator = Nominatim(user_agent="greenllama-woko-app", country_bias="Switzerland")
geocode    = RateLimiter(geolocator.geocode, min_delay_seconds=NOMINATIME_WAIT_TIME)

tqdm.pandas()                   # Get progress bar on geocoding

df['location'] = df['address'].progress_apply(geocode)
df['point']    = df['location'].apply(lambda x: tuple(x.point) if x else None)

# FOLIUM MAP CREATION
# -------------------------

# Create the map using folium
m = folium.Map(location=[47.3728, 8.54443])

# add the markers
for _, (point, addr, price, link) in df[['point', 'address', 'price', 'link']].iterrows():
    if point:
        folium.Marker(point[0:2], popup = "<a href=\"{link}\">{addr}</a> | <b>{price}</b>".format(link=link, addr=addr, price=price)).add_to(m)


m.save("map.html")
