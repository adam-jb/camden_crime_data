
# Code to acquire data on road collisions, crime, postcodes, IMD amd population
# Wrangles and exports files
# Enables subsequent document creation (make_documents.py)
# Adam Bricknell, Feb 2021

from sodapy import Socrata
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO
import os


# get road collision data and convert to dataframe
client = Socrata("opendata.camden.gov.uk", None)
results = client.get("puar-wf4h", limit=100000)
road_collision = pd.DataFrame.from_records(results)
columns_for_numeric = ["longitude", "latitude", "number_of_casualties"]
road_collision[columns_for_numeric] = road_collision[columns_for_numeric].apply(pd.to_numeric)


# get crime data
results = client.get("qeje-7ve7", limit=1000000)
crime = pd.DataFrame.from_records(results)
crime[["longitude", "latitude"]] = crime[["longitude", "latitude"]].apply(pd.to_numeric)


# get NSPL (check if this is used)
results = client.get("tr8t-gqz7", local_authority_code = "E09000007", limit=100000)
nspl = pd.DataFrame.from_records(results)



# get IMD data by LSOA
results = client.get("8x5x-eu22", local_authority_district_code = 'E09000007', limit=1000)
imd = pd.DataFrame.from_records(results)

# get population data by LSOA (takes a few minutes as it's a 40mb download)
url = 'https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates/mid2019sape22dt2/sape22dt2mid2019lsoasyoaestimatesunformatted.zip'
r = requests.get(url)
filebytes = BytesIO(r.content)
myzipfile = ZipFile(filebytes)
filename = myzipfile.namelist()[0]
n =  myzipfile.open(filename)
population = pd.read_excel(n, 'Mid-2019 Persons', skiprows = 4, engine='openpyxl')
population = population[['LSOA Code', 'All Ages']]   # columns of interest

# inner join IMD and Population, which prevents non-Camden LSOAs from going forward
imd_pop = imd.merge(population, left_on = 'lower_super_output_area_code', right_on = 'LSOA Code')
imd_pop[["longitude", "latitude"]] = imd_pop[["longitude", "latitude"]].apply(pd.to_numeric)



# find lsoa of collisions and crime by which LSOA lat/long is closest to the event lat/long
# using pythagoras to find distance to each LSOA, for each event
collision_lsoa = []
for i in range(len(road_collision)):
    ix_max = (road_collision['longitude'][i] - imd_pop['longitude']).pow(2).add((road_collision['latitude'][i] - imd_pop['latitude']).pow(2)).argmin()
    collision_lsoa.append(imd_pop['lower_super_output_area_code'][ix_max])
road_collision['lsoa_code'] = collision_lsoa

crime_lsoa = []   #  ~200k rows so takes a minute or so
for i in range(len(crime)):
    ix_max = (crime['longitude'][i] - imd_pop['longitude']).pow(2).add((crime['latitude'][i] - imd_pop['latitude']).pow(2)).argmin()
    crime_lsoa.append(imd_pop['lower_super_output_area_code'][ix_max])
crime['lsoa_code'] = crime_lsoa


# group collisions data (inc diffrent types) by quarter and lsoa
road_collision['date'] = pd.to_datetime(road_collision['date'], infer_datetime_format=True)
road_collision['qtr'] = road_collision['date'].dt.quarter
road_collision['year'] = road_collision['date'].dt.year

collision_trends = road_collision['number_of_casualties'].groupby(by=[road_collision['lsoa_code'], road_collision['year'], road_collision['qtr']]).agg(['sum', 'count'])
collision_trends.rename(columns = {'sum': 'collisions_casualties', 'count': 'collisions_count'}, inplace = True)
collision_trends.reset_index(level=collision_trends.index.name, inplace = True)


# group crime data across quarter and lsoa (outcome date, different to date of crime)
crime['outcome_date'] = pd.to_datetime(crime['outcome_date'], infer_datetime_format=True)
crime['qtr'] = crime['outcome_date'].dt.quarter
crime['year'] = crime['outcome_date'].dt.year

crime_trends = crime['category'].groupby(by=[crime['lsoa_code'], crime['year'], crime['qtr']]).agg(['count'])
collision_trends.rename(columns = {'count': 'crime_outcome_count'}, inplace = True)
crime_trends.reset_index(level=crime_trends.index.name, inplace = True)


# join crime and collisions at quarter and LSOA level
all_trends = crime_trends.merge(collision_trends, how = 'left',
                                on = ['lsoa_code', 'year', 'qtr'])




# aggregate numeric collisions data, giving timeseries of Camden as a whole
collision_cols = ['casualty_age', 'number_of_casualties', 'number_of_vehicles']
road_collision[collision_cols] = road_collision[collision_cols].apply(pd.to_numeric)

collision_float_trends = pd.DataFrame(columns=['year', 'count', 'mean', 'sum', 'std', 'category'])
for i in range(len(collision_cols)):
    x = road_collision[collision_cols[i]].groupby([road_collision['year']]).agg(['count', 'mean', 'sum', 'std'])
    x = pd.DataFrame(x)
    x.reset_index(inplace = True)
    x['category'] = collision_cols[i]
    collision_float_trends = collision_float_trends.append(x)

## might not need the above now


# aggregate categorical collisions data
cats_to_summarise = [
'number_of_casualties',
'number_of_vehicles',
'casualty_sex',
'casualty_class',
'casualty_age_band',
'casualty_severity',
'day',
'road_type',
'speed_limit',
'junction_detail',
'junction_control',
'road_class_1',
'weather',
'road_surface',
'casualty_age_band'
]

# make all categories upper case
road_collision[cats_to_summarise] = road_collision[cats_to_summarise].apply(lambda x: x.astype(str).str.upper())

# make summary of grouped categories
collision_cat_trends = pd.DataFrame(columns=['subcategory', 'count', 'category'])
for i in range(len(cats_to_summarise)):
    x = road_collision[cats_to_summarise[i]].groupby([road_collision[cats_to_summarise[i]], road_collision['year']]).agg('count')
    x = pd.DataFrame(x)
    x.rename(columns = {cats_to_summarise[i]: 'count'}, inplace = True)
    x.reset_index(inplace = True)
    x.rename(columns = {cats_to_summarise[i]: 'subcategory'}, inplace = True)
    x['category'] = cats_to_summarise[i]
    collision_cat_trends = collision_cat_trends.append(x)

collision_cat_trends['event_type'] = 'collisions'



# make summary of grouped categories for crime data
crime_to_summarise = ['service', 'location_subtype', 'category']
crime[crime_to_summarise] = crime[crime_to_summarise].apply(lambda x: x.astype(str).str.upper())

crime_cat_trends = pd.DataFrame(columns=['subcategory', 'count', 'category'])
for i in range(len(crime_to_summarise)):
    x = crime[crime_to_summarise[i]].groupby([crime[crime_to_summarise[i]], crime['year']]).agg('count')
    x = pd.DataFrame(x)
    x.rename(columns = {crime_to_summarise[i]: 'count'}, inplace = True)
    x.reset_index(inplace = True)
    x.rename(columns = {crime_to_summarise[i]: 'subcategory'}, inplace = True)
    x['category'] = crime_to_summarise[i]
    crime_cat_trends = crime_cat_trends.append(x)

crime_cat_trends['event_type'] = 'crime'


# append crime data
all_category_trends = collision_cat_trends.append(crime_cat_trends)



# get total events in most recent year (2019) for each LSOA
lsoa_events_latest = all_trends[all_trends['year'] == all_trends['year'].max()]
lsoa_events_latest = lsoa_events_latest[['count', 'collisions_count', 'collisions_casualties']].groupby([lsoa_events_latest['lsoa_code']]).agg(['sum'])
lsoa_events_latest.rename(columns = {'count': 'crimes_count'}, inplace = True)
lsoa_events_latest.reset_index(inplace = True)
lsoa_events_latest.columns = ['lsoa_code', 'crimes_count', 'collisions_count', 'collisions_casualties']

# adding rows for LSOAs with no crime/collisions in that year
all_lsoas = pd.DataFrame({'lsoa_code': imd_pop['lower_super_output_area_code'].unique()})

ix = ~all_lsoas['lsoa_code'].isin(lsoa_events_latest['lsoa_code'])   # find LSOAs not in master list
if sum(ix) > 0:
    ix_missing = ix[ix].index
    to_add = []
    for i in ix_missing:
        to_add = to_add + [all_lsoas['lsoa_code'][i]]
    to_append = pd.DataFrame({'lsoa_code': to_add})
    to_append['crimes_count'] = [0] * len(to_add)
    to_append['collisions_count'] = [0.0] * len(to_add)
    to_append['collisions_casualties'] = [0.0] * len(to_add)
    lsoa_events_latest = lsoa_events_latest.append(to_append, sort = False)
    lsoa_events_latest.reset_index(drop=True, inplace = True)



# export (1) LSOA level pop and IMD, (2) LSOA level time series of collisions and crime
directory = os.path.dirname(os.path.abspath(__file__))
imd_pop.to_csv(directory + '/imd_pop_lsoa.csv', index = False)
all_trends.to_csv(directory + '/crime_collision_trends_lsoa.csv', index = False)

# export raw data
road_collision.to_csv(directory + '/road_collisions_all.csv', index = False)
crime.to_csv(directory + '/crime_all.csv', index = False)

# export Camden level time series
all_category_trends.to_csv(directory + '/camden_category_timeseries.csv', index = False)
collision_float_trends.to_csv(directory + '/camden_float_timeseries.csv', index = False)
lsoa_events_latest.to_csv(directory + '/camden_latest_events_lsoa.csv', index = False)



























