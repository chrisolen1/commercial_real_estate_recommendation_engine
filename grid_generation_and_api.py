### Grid/Zone Generation ###

"""
OVERLAY THE CITY OF CHICAGO WITH A SYSTEM OF 1KM X 1KM GRIDS; GRIDS WILL BECOME ONE OF THE PRIMARY UNITS OF ANALYSIS OF THE RECOMMENADTION 
ENGINE. THE SIZE AND LOCATION OF SAID GRIDS ARE ARBITRARILY CHOSEN.
"""

# install packages
import shapely.geometry
import pyproj
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd

# set up grid points
def zone_generate(ne,sw,stepsize):
    # Set up projections
    p_ll = pyproj.Proj(init='epsg:4326')
    p_mt = pyproj.Proj(init='epsg:3857')
    
    # Create corners of rectangle to be transformed to a grid
    sw = shapely.geometry.Point(sw)
    ne = shapely.geometry.Point(ne)

    stepsize = stepsize # 1 km grid step size
    
    # Project corners to target projection
    s = pyproj.transform(p_ll, p_mt, sw.x, sw.y) # Transform NW point to 900913
    e = pyproj.transform(p_ll, p_mt, ne.x, ne.y) # .. same for SE
    
    # Iterate over 2D area
    gridpoints = []
    x = s[0]
    while x < e[0]:
        y = s[1]
        while y < e[1]:
            p = shapely.geometry.Point(pyproj.transform(p_mt, p_ll, x, y))
            gridpoints.append(p)
            y += stepsize
        x += stepsize
        
    # convert list of shapely geopoints to df
    gridpoints_df = pd.DataFrame(gridpoints)
    
    # extract lat long values from shapely geopoints column
    gridpoints_df['lon'] = gridpoints_df[0].apply(lambda p: p.x)
    gridpoints_df['lat'] = gridpoints_df[0].apply(lambda p: p.y)
    
    # create combined lat_lon column
    gridpoints_df['lat_lon'] = gridpoints_df[['lat','lon']].values.tolist()
    
    #counts of cols and rows
    n_col = len(gridpoints_df['lon'].unique())
    n_row = len(gridpoints_df['lat'].unique())
    
    #list of longitudes from west to east
    lon_list = gridpoints_df['lon'].unique()
    
    #list of latitudes from north to south
    lat_list = gridpoints_df['lat'].unique()
    lat_list[::-1].sort()
    
    #iterate over lats and longs to get boundaries for each zone
    zone = []
    i = 0
    for a in range(0,n_row-1):
        north = lat_list[i]
        south = lat_list[i+1]
        i += 1
        j = 0
        for b in range(0,n_col-1):
            west = lon_list[j]
            east = lon_list[j+1]
            zone.append([north,south,west,east])
            j += 1
            
    #turn list of boundaries into dataframe
    zone_df = pd.DataFrame(zone,columns = ['n_bound','s_bound', 'w_bound', 'e_bound'], dtype= float)
    
    return zone_df, gridpoints_df

# define starting points and grid increment as you wish:
ne = (-87.5, 42.05) #format is long, lat
sw = (-87.9, 41.6) #format is long, lat
stepsize = 1000 #every 1000 is 1km

# assign zone_generate output
zone_df, gridpoints_df = zone_generate(ne,sw,stepsize)

# create output to inspect and for other groups
zone_df.to_csv("grid.csv")

# function to convert latitude and longitude into zones
def zone(input_lat,input_lon):
    try:
        box_num = zone_df[(zone_df['n_bound']>=input_lat) 
             & (zone_df['s_bound']<input_lat)
             & (zone_df['w_bound']<=input_lon)
             & (zone_df['e_bound']>input_lon)].index[0]
        return box_num
    except:
        return float('nan')

# random lat longs I chose from google maps to test out function
df = pd.DataFrame(
    [[41.966973, -87.665894],
     [41.967087, -87.665478],
     [41.967493, -87.667132],
     [41.906954, -87.638457],
     [41.907114, -87.628801]],columns=['lat','long'])

# apply function to get column of zones
df['zone'] = df.apply(lambda x: zone(x.lat, x.long), axis=1)

### Property Value Data ### 

"""
THIS SCRIPT USES A LISTING OF ADDRESSES (IN THIS CASE PROVIDED BY THE COOK COUNTY GOVERNMENT) TO CALL THE ZILLOW 'DEEP_SEARCH' API AND INSERT INTO MYSQL DB. IT USES
A SERIES OF IF CONDITIONS AND TRY/EXCEPT LOOPS TO DEAL WITH MULTI-UNIT PROPERTIES AND NON-RESIDENTIAL PROPERTIES, RESPECTIVELY. NOTE THAT SINCE ZILLOW LIMITS USERES
TO 1000 CALLS PER DAY, THIS SCRIPT PRODUCES A TXT FILE COUNTER THAT KEEPS TRACK OF WHERE YOU ARE IN THE ADDRESS BOOK AFTER EACH CALL
"""

import random
import requests
import xmltodict
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import gcsfs
cwd = '/home/chrisolen1/phoenix_scripts/'

# pull Cook County address book out of storage
addresses = pd.read_csv("gs://phoenix-storage/address_book.csv")
addresses.dropna(axis = 0, subset = ['ADDRDELIV'], inplace = True)
addresses.dropna(axis = 0, subset = ['ZIP5'], inplace = True)
addresses = addresses[addresses['PLACENAME']=='Chicago']
addresses['ZIP5'] = addresses['ZIP5'].apply(lambda x: str(int(x))) 
addresses['ZIP5'] = addresses['ZIP5'].astype(object)
addresses = addresses.reset_index(drop=True)

# define the number of daily API calls; must be under 1000
api_calls = random.randint(970, 990)

# pull out the current place in the address book from storage
with open(cwd+'total_count1.txt', 'r') as count_file:
    counter = int(count_file.read())
    count_file.close()

# take a slice out of the address book based on where we are in the count 
def address_slicer(addresses, counter, api_calls):
    if counter + api_calls > addresses.shape[0]: #If we're at the very end of the address book
        address_slice = addresses.iloc[counter:counter+(addresses.shape[0]-counter)][['ADDRDELIV','PLACENAME','ZIP5']]
        return address_slice
    else: #If we're not yet at the end of the address book
        address_slice = addresses.iloc[counter:counter+api_calls][['ADDRDELIV','PLACENAME','ZIP5']]
        return address_slice
sliced_addresses = address_slicer(addresses, counter, api_calls)

# using the address book to format the API parameters
def get_parameters(sliced_addresses):
    parameters = []
    for index, row in sliced_addresses.iterrows():
        parameters.append({"zws-id":"INSERT_ZILLOW_API_KEY_HERE","address":"%s" % row['ADDRDELIV'], 
                      "citystatezip":"%s" % row['PLACENAME'] + " " + "%s" % row['ZIP5']})
    return parameters
parameters = get_parameters(sliced_addresses)

# using API parameters to call the API
def get_search_results_api(parameters):
        response = (requests.get("http://www.zillow.com/webservice/GetDeepSearchResults.htm", params = parameters)).content.decode("utf-8")
        parsed = xmltodict.parse(response)
        try:
            lookup = parsed['SearchResults:searchresults']['response']['results']['result']
            if type(lookup) != list: #For addresses with single units
                try:
                    zestimate = float(lookup['zestimate']['amount']['#text'])
                    zipd = int(lookup['zpid'])
                    try:
                        street = lookup['address']['street']
                    except (KeyError, TypeError):
                        street = None
                    try:
                        city = lookup['address']['city']
                    except (KeyError, TypeError):
                        city = None
                    try:
                        zipcode = lookup['address']['zipcode']
                    except (KeyError, TypeError):
                        zipcode = None
                    try:
                        latitude = float(lookup['address']['latitude'])  
                    except (KeyError, TypeError):
                        latitude = None
                    try:
                        longitude = float(lookup['address']['longitude'])
                    except (KeyError, TypeError):
                        longitude = None
                    try:
                        valueChange = float(lookup['zestimate']['valueChange']['#text'])
                    except (KeyError, TypeError):
                        valueChange = None
                    try:
                        lowest = float(lookup['zestimate']['valuationRange']['low']['#text'])
                    except (KeyError, TypeError):
                        lowest = None
                    try:
                        highest = float(lookup['zestimate']['valuationRange']['high']['#text'])
                    except (KeyError, TypeError):
                        highest = None
                    try:
                        neighborhood = lookup['localRealEstate']['region']['@name']
                    except (KeyError, TypeError):
                        neighborhood = None
                    try:
                        zindex = float(lookup['localRealEstate']['region']['zindexValue'].replace(',',''))
                    except (KeyError, TypeError):
                        zindex = None
                    try:
                        hometype = lookup['useCode']
                    except (KeyError, TypeError):
                        hometype = None
                    try:
                        assessmentYear = int(lookup['taxAssessmentYear'])
                    except (KeyError, TypeError):
                        assessmentYear = None
                    try:
                        assessment = float(lookup['taxAssessment'])
                    except (KeyError, TypeError):
                        assessment = None
                    try:
                        yearBuilt = int(lookup['yearBuilt'])
                    except (KeyError, TypeError):
                        yearBuilt = None
                    try:
                        lotSize = int(lookup['lotSizeSqFt'])
                    except (KeyError, TypeError):
                        lotSize = None
                    try:
                        houseSize = int(lookup['finishedSqFt'])
                    except (KeyError, TypeError):
                        houseSize = None
                    try:
                        bathrooms = float(lookup['bathrooms'])
                    except (KeyError, TypeError):
                        bathrooms = None
                    try:
                        bedrooms = int(lookup['bedrooms'])
                    except (KeyError, TypeError):
                        bedrooms = None
                    try:
                        lastSold = (datetime.strptime(lookup['lastSoldDate'], '%m/%d/%Y')).date()
                    except (KeyError, TypeError):
                        lastSold = None
                    try:
                        lastSoldPrice = int(lookup['lastSoldPrice']['#text'])
                    except (KeyError, TypeError):
                        lastSoldPrice = None
                    list_of_rows = [zipd, street, city, zipcode, latitude, longitude, zestimate, valueChange, lowest, highest, neighborhood, zindex,
                                    hometype, assessmentYear, assessment, yearBuilt, lotSize, houseSize, bathrooms, bedrooms, lastSold, lastSoldPrice]    
                    return list_of_rows
                except (KeyError, TypeError):
                    return 'nan'
            if type(lookup) == list: #For address with more than one unit
                list_of_rows = []
                for i in range(len(lookup)-4):
                    try:
                        zestimate = float(lookup[i]['zestimate']['amount']['#text'])
                        zipd = int(lookup[i]['zpid'])
                        try:
                            street = lookup[i]['address']['street']
                        except (KeyError, TypeError):
                            street = None
                        try:
                            city = lookup[i]['address']['city']
                        except (KeyError, TypeError):
                            city = None
                        try:
                            zipcode = lookup[i]['address']['zipcode']
                        except (KeyError, TypeError):
                            zipcode = None
                        try:
                            latitude = float(lookup[i]['address']['latitude'])  
                        except (KeyError, TypeError):
                            latitude = None
                        try:
                            longitude = float(lookup[i]['address']['longitude'])
                        except (KeyError, TypeError):
                            longitude = None
                        try:
                            valueChange = float(lookup[i]['zestimate']['valueChange']['#text'])
                        except (KeyError, TypeError):
                            valueChange = None
                        try:
                            lowest = float(lookup[i]['zestimate']['valuationRange']['low']['#text'])
                        except (KeyError, TypeError):
                            lowest = None
                        try:
                            highest = float(lookup[i]['zestimate']['valuationRange']['high']['#text'])
                        except (KeyError, TypeError):
                            highest = None
                        try:
                            neighborhood = lookup[i]['localRealEstate']['region']['@name']
                        except (KeyError, TypeError):
                            neighborhood = None
                        try:
                            zindex = float(lookup[i]['localRealEstate']['region']['zindexValue'].replace(',',''))
                        except (KeyError, TypeError):
                            zindex = None
                        try:
                            hometype = lookup['useCode']
                        except (KeyError, TypeError):
                            hometype = None
                        try:
                            assessmentYear = int(lookup['taxAssessmentYear'])
                        except (KeyError, TypeError):
                            assessmentYear = None
                        try:
                            assessment = float(lookup['taxAssessment'])
                        except (KeyError, TypeError):
                            assessment = None
                        try:
                            yearBuilt = int(lookup['yearBuilt'])
                        except (KeyError, TypeError):
                            yearBuilt = None
                        try:
                            lotSize = int(lookup['lotSizeSqFt'])
                        except (KeyError, TypeError):
                            lotSize = None
                        try:
                            houseSize = int(lookup['finishedSqFt'])
                        except (KeyError, TypeError):
                            houseSize = None
                        try:
                            bathrooms = float(lookup['bathrooms'])
                        except (KeyError, TypeError):
                            bathrooms = None
                        try:
                            bedrooms = int(lookup['bedrooms'])
                        except (KeyError, TypeError):
                            bedrooms = None
                        try:
                            lastSold = (datetime.strptime(lookup['lastSoldDate'], '%m/%d/%Y')).date()
                        except (KeyError, TypeError):
                            lastSold = None
                        try:
                            lastSoldPrice = int(lookup['lastSoldPrice']['#text'])
                        except (KeyError, TypeError):
                            lastSoldPrice = None
                        list_of_rows.append([zipd, street, city, zipcode, latitude, longitude, zestimate, valueChange, lowest, highest, neighborhood, zindex,
                                    hometype, assessmentYear, assessment, yearBuilt, lotSize, houseSize, bathrooms, bedrooms, lastSold, lastSoldPrice])    
                    except (KeyError, TypeError):
                        return 'nan' 
                return list_of_rows
        except (KeyError, TypeError):
            return 'nan'
messy_list = [get_search_results_api(i) for i in parameters] 

# flatten out nested list
def flatten(messy_list):
    while messy_list:
        if isinstance(messy_list[0], list):  # Checks whether first element is a list
            messy_list = messy_list[0] + messy_list[1:]  # If so, flattens that first element one level
        else:
            yield messy_list.pop(0)  # Otherwise yield as part of the flat array
flattened_list = list(flatten(messy_list))

# remove None values that occur when no estimates or zpid exist for a property (often because the address is erroneous or its a commercial property)
nans_removed = [x for x in flattened_list if x != 'nan']

# create lists of equal length amongst all elements
df_rows = [nans_removed[i:i + 22] for i in range(0, len(nans_removed), 22)]

# combine lists into DataFrame
df_zillow_deepsearch = pd.DataFrame(df_rows, columns = ['zpid','street','city','zipcode','latitude','longitude','zestimate',
                                                    'valueChange','low_estimate','high_estimate','neighborhood','neighborhood_zindex_value',
                                                    'hometype','assessmentYear','assessment','yearBuilt','lotSize','houseSize','bathrooms',
                                                    'bedrooms','lastSold','lastSoldPrice'])

# insert dataframe into mysql database
passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "deep_search"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()
df_zillow_deepsearch.to_sql(con=con, name=table, if_exists='append', index=False)

# savings the new counter to storage
if counter + api_calls > addresses.shape[0]: #If we're at the end of the address book, go to zero
    counter = str(0)
    with open(cwd+'total_count1.txt', 'w') as count_file:
        count_file.write('%s' % counter)
        count_file.close()
else: #Otherwise add the amount of daily api calls
    counter = str(counter+api_calls)
    with open(cwd+'total_count1.txt', 'w') as count_file:
        count_file.write('%s' % counter)
        count_file.close()

# return notification
rows_returned = df_zillow_deepsearch.shape[0]
this_month = datetime.now().month
today = datetime.now().day
response = 'DeepSearch1 returned %s rows on %d-%d.\n' % (rows_returned, this_month, today)

with open(cwd+'deep_search_response.txt','a') as search_response:
    search_response.write('%s' % response)
    search_response.close()

### Chicago Transit Authority Stop Location Data ###

"""
THIS SCRIPT USES THE SOCRATA API TO PULL CTA 'EL' STOP DATA AND USES A CSV EXPORT FROM DATA.GOV 
(https://catalog.data.gov/dataset?metadata_type=non-geospatial&res_format=ZIP&tags=bus). CLEANED DATA IS THEN INSERTED INTO A MYSQL DB. 
"""

import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import numpy as np

client = Socrata("data.cityofchicago.org", 
                 "YOUR_KEY",
                 username = "YOUR_USERNAME",
                 password = "YOUR_PASSWORD",
                 timeout = 1000)

#CTA Trains
results = client.get("8pix-ypme", limit=260000, content_type='csv')
results_df = pd.DataFrame.from_records(results[1:], columns = [i.lower().replace(" ","_") for i in results[0]])
cols = ['stop_id','direction_id','stop_name','station_name','ada','blue','brn','g','o','p','pexp','pnk','red','y','location']
trains = results_df[cols]

def lat_lon_split(row):
    row = row.replace('(',"").replace(')',"")
    lat = float(row.split(',')[0])
    lon = float(row.split(',')[1])
    return [lat, lon]

trains['location'] = trains['location'].apply(lat_lon_split)

lat = [trains['location'].iloc[i][0] for i in range(len(trains))]
lon = [trains['location'].iloc[i][1] for i in range(len(trains))]
    
d = {'latitude':lat, 'longitude':lon}

trains[['latitude','longitude']] = pd.DataFrame(data=d)
trains.drop(['station_name','location','ada'], inplace=True, axis = 1)

#CTA Buses
df = pd.read_csv('/Users/chrisolen/Documents/uchicago_courses/data_engineering_platforms/zillow/testing/CTA_BusStops.csv')

df.drop(['OBJECTID','the_geom','CROSS_ST','POS','OWLROUTES','CITY','STATUS','STREET','ROUTESSTPG'],inplace=True,axis=1)
buses = df.rename(columns={"SYSTEMSTOP":"stop_id","DIR":"direction_id","PUBLIC_NAM":'stop_name',
           "POINT_X":'longitude',"POINT_Y":'latitude'})
bus = np.repeat([True],len(buses))
buses['bus'] = bus

#Merged
transit = trains.append(buses,sort=False)
transit.fillna("False", inplace=True)
transit['stop_id'] = transit['stop_id'].apply(int)

def bool_convert1(row):
    return row == 'true'
transit['blue'] = transit['blue'].apply(bool_convert1)
transit['brn'] = transit['brn'].apply(bool_convert1)
transit['g'] = transit['g'].apply(bool_convert1)
transit['o'] = transit['o'].apply(bool_convert1)
transit['p'] = transit['p'].apply(bool_convert1)
transit['pexp'] = transit['pexp'].apply(bool_convert1)
transit['pnk'] = transit['pnk'].apply(bool_convert1)
transit['red'] = transit['red'].apply(bool_convert1)
transit['y'] = transit['y'].apply(bool_convert1)

def bool_convert2(row):
    return row == True
transit['bus'] = transit['bus'].apply(bool_convert2)

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "transit"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()
transit.to_sql(con=con, name=table, if_exists='append', index=False)

### Crime Data ###

"""
THIS SCRIPT USES THE SOCRATA API TO PULL CRIME DATA WITHIN THE CITY OF CHICAGO AND INSERTS A CLEANED UP RESULT INTO A MYSQL DB.
"""

import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import numpy as np
import datetime

client = Socrata("data.cityofchicago.org", 
                 "APP_KEY",
                 username = "API_USERNAME",
                 password = "API_PASSWORD",
                 timeout = 1000)

results = client.get("3i3m-jwuy", limit=260000, content_type='csv')

results_df = pd.DataFrame.from_records(results[1:], columns = [i.lower().replace(" ","_") for i in results[0]])

crime = results_df.drop(['historical_wards_2003-2015', 'zip_codes',
       'census_tracts', 'boundaries_-_zip_codes',
       'police_districts', 'police_beats',
       'x_coordinate', 'y_coordinate', 'location'], axis = 1)

cols = ['id', 'case_number', 'date', 'year', 'primary_type', 'description', 'domestic', 'arrest',
        'beat', 'block', 'community_area', 'ward', 'district','latitude', 'longitude', 'location_description', 
        'fbi_code', 'iucr', 'updated_on']

crime = crime[cols]
crime.rename(columns={'updated_on':'entered_on'}, inplace=True)
dates = [datetime.datetime.strptime(crime['date'].iloc[i], '%m/%d/%Y %I:%M:%S %p') for i in range(len(crime['date']))]
crime['date'] = [dates[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(len(dates))]
entereds = [datetime.datetime.strptime(crime['entered_on'].iloc[i], '%m/%d/%Y %I:%M:%S %p') for i in range(len(crime['entered_on']))]
crime['entered_on'] = [entereds[i].strftime('%Y-%m-%d %H:%M:%S') for i in range(len(entereds))]

def bool_convert(s):
    return s == 'true'

crime['arrest'] = [bool_convert(crime['arrest'].iloc[i]) for i in range(len(crime['arrest']))]
crime['domestic'] = [bool_convert(crime['domestic'].iloc[i]) for i in range(len(crime['domestic']))]
crime = crime[crime['latitude']!='']
crime = crime.reset_index(drop=True)
crime = crime.replace('', np.nan)

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "crime"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()
crime.to_sql(con=con, name=table, if_exists='append', index=False)

### Demographic Data ###

"""
ONE OF THE LIMITATIONS OF THE U.S. CENSUS API IS THAT DEMOGRAPHIC DATA CAN ONLY BE LOOKED UP BY ZIP CODE, AND THE SIZES AND SHAPES OF ZIP CODES
VARY; SO, THE BELOW SCRIPT PULLS THE ZIP CODES FOR EACH OF THE FOUR CORNERS OF THE GENERATED ZONES/GRIDS AND THEN DOES A SIMPLE CALCULATION TO APPROXIMATE THE 
PROPORTIONAL ZIP CODE MAKE-UP OF EACH RESPECTIVE GRID/ZONE. FOR INSTANCE, IF ALL FOUR CORNERS OF THE ZONE ARE '60640', THEN THE ZIP CODE MAKE-UP OF THE ZONE IS 100 PERCENT
60640. CONVERSELY, IF TWO OF THE CORNERS ARE '60659' AND TWO ARE '60640' THEN THE ZIP CODE MAKE-UP OF THE ZONE IS 50%-50%. 
"""

### Zipcodes to Generated Grids/Zones ###

# import packages
import pandas as pd
import time
import googlemaps

# create deep copy
grid = zone_df.copy()

grid['NW ZIP'] = ''
grid['NE ZIP'] = ''
grid['SW ZIP'] = ''
grid['SE ZIP'] = ''

# create zone column
p = grid.index.values
grid.insert( 0, column="zone",value = p)

# load zones where we have business data
### NOTE: BUSINESS DATA HIGHLY SENSITIVE AND NOT UPLOADED TO GITHUB ###
zones = pd.read_csv("dunnhumbyzones.csv")

# remove zones from look-up that are not even going to be used
grid = pd.merge(grid, zones, on = 'zone', how ='right')

# our business information data set resulted in 1126 zones for the City of Chicago
len(grid.index)

# start - row to start
# end - row to end

start = 909
end = 1127

#'key' is own api key
gmaps = googlemaps.Client(key='YOUR_KEY_HERE')
     
# use grid.tail() for testing
for index, row in grid.iloc[start:end].iterrows():    

    # Access data using column names
    n_lat = row['n_bound']
    w_long = row['w_bound']
    s_lat = row['s_bound']
    e_long = row['e_bound']
    
    # Wait for 5 seconds
    time.sleep(5)
    
    # Look up an address with reverse geocoding
    NW = gmaps.reverse_geocode((n_lat, w_long))
    NE = gmaps.reverse_geocode((n_lat, e_long))
    SW = gmaps.reverse_geocode((s_lat, w_long))
    SE = gmaps.reverse_geocode((s_lat, e_long))
    
    # Look through address components in returned address elements
    for element in NW[0]['address_components']:
        # check the type of each address component for ZIP
        if 'postal_code' in element['types'] :
            grid.at[index, 'NW ZIP'] = element['long_name']
    # Look through address components in returned address elements
    for element in NE[0]['address_components']:
        # check the type of each address component for ZIP
        if 'postal_code' in element['types'] :
            grid.at[index, 'NE ZIP'] = element['long_name']
    for element in SW[0]['address_components']:
        # check the type of each address component for ZIP
        if 'postal_code' in element['types'] :
            grid.at[index, 'SW ZIP'] = element['long_name']
    for element in SE[0]['address_components']:
        # check the type of each address component for ZIP
        if 'postal_code' in element['types'] :
            grid.at[index, 'SE ZIP'] = element['long_name']

# visually inspect
grid.to_csv("grid_ZIP.csv")

# create a deep copy
grid_final = grid.copy()

# need to find missing values due to Reverse Geocode look-up issues, 
# i.e. physical address not at one of the four corners of the grid

print('NW ZIP index:', grid_final[grid_final['NW ZIP'] == ''].index)
print('NE ZIP index:', grid_final[grid_final['NE ZIP'] == ''].index)
print('SW ZIP index:', grid_final[grid_final['SW ZIP'] == ''].index)
print('SW ZIP index:', grid_final[grid_final['SE ZIP'] == ''].index)

# fixing NW ZIP grid corner missing values
grid_final.iloc[778:779]
grid_final.at[778, 'NW ZIP'] = 60638
grid_final.iloc[778:779]

# fixing NE ZIP grid corner missing values
grid_final.iloc[396:397]
grid_final.at[396, 'NE ZIP'] = 60611
grid_final.iloc[396:397]

# fixing NE ZIP grid corner missing values
grid_final.iloc[621:622]
grid_final.at[621, 'NE ZIP'] = 60653
grid_final.iloc[621:622]

# fixing SW ZIP grid corner missing values
grid_final.iloc[751:752]
grid_final.at[751, 'SW ZIP'] = 60638
grid_final.iloc[751:752]

# fixing SE ZIP grid corner missing values
grid_final.iloc[419:420]
grid_final.at[419, 'SE ZIP'] = 60611
grid_final.iloc[419:420]

# fixing SE ZIP grid corner missing values
grid_final.iloc[750:751]
grid_final.at[750, 'SE ZIP'] = 60638
grid_final.iloc[750:751]

# fixing SE ZIP grid corner missing values
grid_final.iloc[1116:1117]
grid_final.at[1116, 'SE ZIP'] = 60633
grid_final.iloc[1116:1117]

# validate whether there are any blanks after manual clean-up
print('NW ZIP index:', grid_final[grid_final['NW ZIP'] == ''].index)
print('NE ZIP index:', grid_final[grid_final['NE ZIP'] == ''].index)
print('SW ZIP index:', grid_final[grid_final['SW ZIP'] == ''].index)
print('SW ZIP index:', grid_final[grid_final['SE ZIP'] == ''].index)

# output to inspect
grid_final.to_csv("grid_final2.csv")  

# create a deep copy
grid_final_2 = grid_final.copy()

# create shallow copy for comparison purposes
copy = grid_final_2

# count across grid; unfortunately, needs to be refactored but works.
copy['46320']=(grid_final_2 == '46320').T.sum(); copy['46324']=(grid_final_2 == '46324').T.sum(); copy['46327']=(grid_final_2 == '46327').T.sum()
copy['46394']=(grid_final_2 == '46394').T.sum(); copy['60018']=(grid_final_2 == '60018').T.sum(); copy['60053']=(grid_final_2 == '60053').T.sum()
copy['60068']=(grid_final_2 == '60068').T.sum(); copy['60076']=(grid_final_2 == '60076').T.sum(); copy['60077']=(grid_final_2 == '60077').T.sum()
copy['60130']=(grid_final_2 == '60130').T.sum(); copy['60131']=(grid_final_2 == '60131').T.sum(); copy['60153']=(grid_final_2 == '60153').T.sum()
copy['60160']=(grid_final_2 == '60160').T.sum(); copy['60165']=(grid_final_2 == '60165').T.sum(); copy['60171']=(grid_final_2 == '60171').T.sum()
copy['60176']=(grid_final_2 == '60176').T.sum(); copy['60202']=(grid_final_2 == '60202').T.sum(); copy['60203']=(grid_final_2 == '60203').T.sum()
copy['60301']=(grid_final_2 == '60301').T.sum(); copy['60302']=(grid_final_2 == '60302').T.sum(); copy['60304']=(grid_final_2 == '60304').T.sum()
copy['60305']=(grid_final_2 == '60305').T.sum(); copy['60406']=(grid_final_2 == '60406').T.sum(); copy['60409']=(grid_final_2 == '60409').T.sum()
copy['60426']=(grid_final_2 == '60426').T.sum(); copy['60453']=(grid_final_2 == '60453').T.sum(); copy['60455']=(grid_final_2 == '60455').T.sum()
copy['60456']=(grid_final_2 == '60456').T.sum(); copy['60459']=(grid_final_2 == '60459').T.sum(); copy['60462']=(grid_final_2 == '60462').T.sum()
copy['60463']=(grid_final_2 == '60463').T.sum(); copy['60464']=(grid_final_2 == '60464').T.sum(); copy['60473']=(grid_final_2 == '60473').T.sum()
copy['60482']=(grid_final_2 == '60482').T.sum(); copy['60501']=(grid_final_2 == '60501').T.sum(); copy['60525']=(grid_final_2 == '60525').T.sum()
copy['60534']=(grid_final_2 == '60534').T.sum(); copy['60601']=(grid_final_2 == '60601').T.sum(); copy['60603']=(grid_final_2 == '60603').T.sum()
copy['60605']=(grid_final_2 == '60605').T.sum(); copy['60606']=(grid_final_2 == '60606').T.sum(); copy['60607']=(grid_final_2 == '60607').T.sum()
copy['60608']=(grid_final_2 == '60608').T.sum(); copy['60609']=(grid_final_2 == '60609').T.sum(); copy['60610']=(grid_final_2 == '60610').T.sum()
copy['60611']=(grid_final_2 == '60611').T.sum(); copy['60612']=(grid_final_2 == '60612').T.sum(); copy['60613']=(grid_final_2 == '60613').T.sum()
copy['60614']=(grid_final_2 == '60614').T.sum(); copy['60615']=(grid_final_2 == '60615').T.sum(); copy['60616']=(grid_final_2 == '60616').T.sum()
copy['60617']=(grid_final_2 == '60617').T.sum(); copy['60618']=(grid_final_2 == '60618').T.sum(); copy['60619']=(grid_final_2 == '60619').T.sum()
copy['60620']=(grid_final_2 == '60620').T.sum(); copy['60621']=(grid_final_2 == '60621').T.sum(); copy['60622']=(grid_final_2 == '60622').T.sum()
copy['60623']=(grid_final_2 == '60623').T.sum(); copy['60624']=(grid_final_2 == '60624').T.sum(); copy['60625']=(grid_final_2 == '60625').T.sum()
copy['60626']=(grid_final_2 == '60626').T.sum(); copy['60628']=(grid_final_2 == '60628').T.sum(); copy['60629']=(grid_final_2 == '60629').T.sum()
copy['60630']=(grid_final_2 == '60630').T.sum(); copy['60631']=(grid_final_2 == '60631').T.sum(); copy['60632']=(grid_final_2 == '60632').T.sum()
copy['60633']=(grid_final_2 == '60633').T.sum(); copy['60634']=(grid_final_2 == '60634').T.sum(); copy['60636']=(grid_final_2 == '60636').T.sum()
copy['60637']=(grid_final_2 == '60637').T.sum(); copy['60638']=(grid_final_2 == '60638').T.sum(); copy['60639']=(grid_final_2 == '60639').T.sum()
copy['60640']=(grid_final_2 == '60640').T.sum(); copy['60641']=(grid_final_2 == '60641').T.sum(); copy['60642']=(grid_final_2 == '60642').T.sum()
copy['60643']=(grid_final_2 == '60643').T.sum(); copy['60644']=(grid_final_2 == '60644').T.sum(); copy['60645']=(grid_final_2 == '60645').T.sum()
copy['60646']=(grid_final_2 == '60646').T.sum(); copy['60647']=(grid_final_2 == '60647').T.sum(); copy['60649']=(grid_final_2 == '60649').T.sum()
copy['60651']=(grid_final_2 == '60651').T.sum(); copy['60652']=(grid_final_2 == '60652').T.sum(); copy['60653']=(grid_final_2 == '60653').T.sum()
copy['60654']=(grid_final_2 == '60654').T.sum(); copy['60655']=(grid_final_2 == '60655').T.sum(); copy['60656']=(grid_final_2 == '60656').T.sum()
copy['60657']=(grid_final_2 == '60657').T.sum(); copy['60659']=(grid_final_2 == '60659').T.sum(); copy['60660']=(grid_final_2 == '60660').T.sum()
copy['60661']=(grid_final_2 == '60661').T.sum(); copy['60666']=(grid_final_2 == '60666').T.sum(); copy['60706']=(grid_final_2 == '60706').T.sum()
copy['60707']=(grid_final_2 == '60707').T.sum(); copy['60712']=(grid_final_2 == '60712').T.sum(); copy['60714']=(grid_final_2 == '60714').T.sum()
copy['60803']=(grid_final_2 == '60803').T.sum(); copy['60804']=(grid_final_2 == '60804').T.sum(); copy['60805']=(grid_final_2 == '60805').T.sum()
copy['60827']=(grid_final_2 == '60827').T.sum(); copy['60402']=(grid_final_2 == '60402').T.sum()
copy['60452']=(grid_final_2 == '60452').T.sum(); copy['60457']=(grid_final_2 == '60457').T.sum()

# should be the same as the original length 
len(copy.index)

# output to inspect
copy.to_csv("grid_final4.csv")

grid_final_3 = copy.copy()

# since there are four corners in a grid, this gives a simply percentage breakdown of ZIPs within grid
right = grid_final_3.loc[:, '46320':'60827'].div(4)

left = grid_final_3.loc[:,'zone':'SE ZIP']

grid_final_3 = pd.concat([left, right], axis=1)

# visual inspection
grid_final_3.to_csv("grid_ZIP_percentages.csv")

### PULLING FROM THE CENSUS API ###

# import libraries
import pandas as pd
from census import Census
from us import states

# default year is 2016, i.e. year=2016
c = Census("YOUR_KEY_HERE") #API Key

# test API using 'B25010','description': 'AVERAGE HOUSEHOLD SIZE OF OCCUPIED HOUSING UNITS BY TENURE
c.acs5.zipcode(('NAME', 'B25010_001E'),
          60640, year = 2016)

# to view all available census tables
c.acs5.tables()

ZIP5DList = [46320, 46324, 46327, 46394, 60018,
             60053, 60068, 60076, 60077, 60130,
             60131, 60153, 60160, 60165, 60171,
             60176, 60202, 60203, 60301, 60302,
             60304, 60305, 60402, 60406, 60409,
             60426, 60452, 60453, 60455, 60456,
             60457, 60459,
             60462, 60463, 60464, 60473, 60482,
             60501, 60525, 60534, 60601, 60603,
             60605, 60606, 60607, 60608, 60609,
             60610, 60611, 60612, 60613, 60614,
             60615, 60616, 60617, 60618, 60619,
             60620, 60621, 60622, 60623, 60624,
             60625, 60626, 60628, 60629, 60630,
             60631, 60632, 60633, 60634, 60636,
             60637, 60638, 60639, 60640, 60641,
             60642, 60643, 60644, 60645, 60646,
             60647, 60649, 60651, 60652, 60653,
             60654, 60655, 60656, 60657, 60659,
             60660, 60661, 60666, 60706, 60707,
             60712, 60714, 60803, 60804, 60805,
             60827]

Table1 = ['B07011_001E', 'B25010_001E',
          'B01002_002E', 'B01002_003E',
          'B02001_001E','B02001_002E', 'B02001_003E',
          'B02001_004E','B02001_005E', 'B02001_006E',
          'B02001_007E', 'B02001_008E']


#'''
#'B07011_001E' - Median income in the past 12 months!!Total!
#'B25010_001E' - AVERAGE HOUSEHOLD SIZE OF OCCUPIED HOUSING UNITS
#'B01002_002E' - Estimate!!Median age!!Male
#'B01002_003E' - Estimate!!Median age!!Female
#'B02001_001E' - Estimate!!Total
#'B02001_002E' - Estimate!!Total!!White alone
#'B02001_003E' - Estimate!!Total!!Black or African American alone
#'B02001_004E' - Estimate!!Total!!American Indian and Alaska Native alone
#'B02001_005E' - Estimate!!Total!!Asian alone
#'B02001_006E' - Estimate!!Total!!Native Hawaiian and Other Pacific Islander alone
#'B02001_007E' - Estimate!!Total!!Some other race alone
#'B02001_008E' - Estimate!!Total!!Two or more races
#'''

census = pd.DataFrame()
census['ZIP'] = ZIP5DList

for i in Table1:
    list = [0] * len(ZIP5DList)
    for j in range(0, len(ZIP5DList)):
        lookup = c.acs5.zipcode(i, ZIP5DList[j])
        for k in lookup:
            list[j] = k[i]
    print('Completed Table:', i)
    census[i] = list

# rename census table names into something more meaningful
census.rename(columns={'ZIP':'ZIP',
                       'B07011_001E' : 'Median_income',
                       'B25010_001E' : 'Ave_Household_size',
                       'B01002_002E' : 'Male_Median_Age',
                       'B01002_003E' : 'Female_Median_Age',
                       'B02001_001E' : 'Pop_Total',
                       'B02001_002E' : 'White_Total',
                       'B02001_003E' : 'Black_Total',
                       'B02001_004E' : 'Native_Total',
                       'B02001_005E' : 'Asian_Total',
                       'B02001_006E' : 'Hawaiian_Total',
                       'B02001_007E' : 'Other_Alone_Total',
                       'B02001_008E' : 'Two_or_more_Total',},
                 inplace=True)

# drop pop_total 
census = census.drop(columns=['Pop_Total'])

# create a deep copy
census_2 = census.copy()

# define columns used to generate percentage breakdowns
cols = ['White_Total', 'Black_Total', 'Native_Total', 'Asian_Total', 'Hawaiian_Total','Other_Alone_Total', 'Two_or_more_Total']

# calculate percentages with each ZIP code of demographic breakdowns
census_2[cols] = census_2[cols].div(census_2[cols].sum(axis=1), axis=0)

# transpose dataframe so that can be combined with ZIP Code/ Zone percentage mapping
census_final = census_2.copy().set_index('ZIP').T 

# output to inspect
census_final.to_csv("census_final.csv")  

# now we need to combine census results and grid breakdowns:

# load in the two csv outputs from before:
grid_ZIP = pd.read_csv("grid_ZIP_percentages.csv")
census_ZIP = pd.read_csv("census_final.csv")

# median calculation by Zone
median = census_ZIP.loc[0, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
median['median_total'] = median.sum(axis=1)
median_total = median['median_total']

# ave_household_size calculation by Zone
ave_household_size = census_ZIP.loc[1, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
ave_household_size['ave_household_size_total'] = ave_household_size.sum(axis=1)
ave_household_size = ave_household_size['ave_household_size_total']

# male median age by Zone
median_male_age = census_ZIP.loc[2, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
median_male_age['median_male_age'] = median_male_age.sum(axis=1)
median_male_age  = median_male_age['median_male_age']

# female median age by Zone
median_female_age = census_ZIP.loc[3, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
median_female_age['median_female_age'] = median_female_age.sum(axis=1)
median_female_age  = median_female_age['median_female_age']

# White_Total by Zone
white_total = census_ZIP.loc[4, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
white_total['white_total'] = white_total.sum(axis=1)
white_total = white_total['white_total']

# Black_Total by Zone
black_total = census_ZIP.loc[5, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
black_total['black_total'] = black_total.sum(axis=1)
black_total = black_total['black_total']

# Native_Total by Zone
native_total = census_ZIP.loc[6, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
native_total['native_total'] = native_total.sum(axis=1)
native_total = native_total['native_total']

# Asian_Total by Zone
asian_total = census_ZIP.loc[7, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
asian_total['asian_total'] = asian_total.sum(axis=1)
asian_total = asian_total['asian_total']

# Hawaiian_Total by Zone
hawaiian_total = census_ZIP.loc[8, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
hawaiian_total['hawaiian_total'] = hawaiian_total.sum(axis=1)
hawaiian_total = hawaiian_total['hawaiian_total'] 

# Other_Alone_Total by Zone
other_alone_total = census_ZIP.loc[9, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
other_alone_total['other_alone_total'] = other_alone_total.sum(axis=1)
other_alone_total = other_alone_total['other_alone_total']

# Two_or_more_Total by Zone
two_or_more_total = census_ZIP.loc[10, '46320':'60827'] * grid_ZIP.loc[:, '46320':'60827']
two_or_more_total['two_or_more_total'] = two_or_more_total.sum(axis=1)
two_or_more_total = two_or_more_total['two_or_more_total']

grid_census = pd.concat([grid_ZIP.loc[:,'zone':'SE ZIP'], median_total, ave_household_size, median_male_age, median_female_age, white_total, black_total, native_total, asian_total, hawaiian_total, other_alone_total, two_or_more_total], axis=1)

grid_census.to_csv("grid_census_check.csv")

























