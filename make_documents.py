


# Code to get geometry data for mapping, and create document outputs
# Before running this, run process_data.py to acquire and wrangle necessary data
# Adam Bricknell, Feb 2021


import os
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import Polygon
import json
import warnings
import plotly.express as px
import folium
from folium import plugins




# import processed files
directory = os.path.dirname(os.path.abspath(__file__))
imd_pop = pd.read_csv(directory + '/imd_pop_lsoa.csv')
all_trends = pd.read_csv(directory + '/crime_collision_trends_lsoa.csv')

road_collision = pd.read_csv(directory + '/road_collisions_all.csv')
crime = pd.read_csv(directory + '/crime_all.csv')

all_category_trends = pd.read_csv(directory + '/camden_category_timeseries.csv')
collision_float_trends = pd.read_csv(directory + '/camden_float_timeseries.csv')
lsoa_events_latest = pd.read_csv(directory + '/camden_latest_events_lsoa.csv')



## Legacy: should be unnecessary
# remove nan row (when tested, a NaN won't be equal to itself)
if lsoa_events_latest.iloc[0][0] != lsoa_events_latest.iloc[0][0]:
    lsoa_events_latest = lsoa_events_latest.iloc[1:]




# get LSOA polygons in json format (used for LSOA maps below)
x = requests.get('https://opendata.camden.gov.uk/resource/vxrv-q2tp.json')
lsoa = x.json()

# suppressing warnings (which break script run, however no actual error)
# assigning geometry to geodataframe
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    lsoa_polygons = gpd.GeoDataFrame(crs = {'init': 'epsg:4326'})
    for i in range(len(lsoa)):
        polygon_geom = Polygon(lsoa[i]['the_geom']['coordinates'])
        lsoa_polygons.loc[i,'lsoa_code'] = lsoa[i]['lsoa_2011_code']
        lsoa_polygons.loc[i, 'geometry'] = polygon_geom

# converting geodataframe to json, which is plotly express' prefered input format
lsoa_string = lsoa_polygons.to_json()
lsoa_json_for_plot = json.loads(lsoa_string)




# ensuring appropriate columns are numeric for LSOA maps
numeric_cols = ['crimes_count', 'collisions_count', 'collisions_casualties']
lsoa_events_latest[numeric_cols] = lsoa_events_latest[numeric_cols].apply(pd.to_numeric)



# saving LSOA level interactive maps as an html document
title_names = ['Crimes in 2019', 'Road collisions in 2019', 'Road collision casualties in 2019']
with open(directory + "/lsoa_maps.html", 'w') as f:

    f.write('<h2 style="font-family:courier;text-align:center;">Maps on crime and collisions in 2019 in Camden. Scroll down to see all the maps</h2>')
    for i in range(len(numeric_cols)):
        fig = px.choropleth(lsoa_events_latest,
                            geojson=lsoa_json_for_plot,
                            color=numeric_cols[i],
                            color_continuous_scale="Viridis",
                            locations="lsoa_code",       # district is column name to link to
                            featureidkey="properties.lsoa_code",   # json has properties -> district for key to link to df
                            projection="mercator",
                            title = title_names[i]
                           )
        fig.update_geos(fitbounds="locations", visible=False)
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
        #fig.write_html(directory + "/" + numeric_cols[i] + "_map.html")




### saving Camden level time series plots

# creating df of category names, types and titles to loop through
crime_trends = all_category_trends[all_category_trends['event_type'] == 'crime']
crime_cats = pd.DataFrame({'category': crime_trends['category'].unique()})
crime_cats['event_type'] = 'crime'
coll_trends = all_category_trends[all_category_trends['event_type'] == 'collisions']
coll_cats = pd.DataFrame({'category': coll_trends['category'].unique()})
coll_cats['event_type'] = 'collisions'
plot_titles = crime_cats.append(coll_cats)
plot_titles.reset_index(drop = True, inplace = True)
plot_titles['plot_title'] = plot_titles['event_type'] + ' trends by ' + plot_titles['category']


# making plots and exporting
with open(directory + "/camden_trends.html", 'w') as f:
    f.write('<h2 style="font-family:courier;text-align:center;">Trends of crime and collisions in Camden. Scroll down to see all the plots</h2>')
    for i in range(len(plot_titles)):
        ix = all_category_trends['category'] == plot_titles['category'][i]
        fig = px.line(all_category_trends[ix], x="year", y="count",
                        color='subcategory',
                        template = "simple_white",
                        labels={
                             "count": "Number of incidents",
                             "subcategory": plot_titles['category'][i]
                         },
                        title = plot_titles['plot_title'][i]
                    )
        fig.update_xaxes(dtick=1)
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))



# saving doc with plots on IMD vs number of collisions at LSOA levels, also correlation statistics
# looking at collisions instead of crime because crime is already captured in IMD statistics
imd_cols = [
'index_of_multiple_deprivation_score',
'income_score',
'employment_score',
'education_skills_and_training_score',
'health_deprivation_and_disability_score',
'crime_score',
'barriers_to_housing_and_services_score',
'living_environment_score'
]

for_plot = imd_pop[imd_cols + ['LSOA Code', 'All Ages']]
for_plot = for_plot.merge(lsoa_events_latest, left_on = 'LSOA Code', right_on = 'lsoa_code')

# explorting doc
with open(directory + "/collisions_vs_imd.html", 'w') as f:
    f.write('<h2 style="font-family:sans-serif;text-align:center;">Exploring the relationship between indices of multiple deprivation and collisions in Camden</h2>')
    f.write('<h4 style="font-family:sans-serif;text-align:center;">Scroll down to see all the plots</h4>')
    f.write('<br><br><br><br>')
    for i in range(len(imd_cols)):

        title_text = imd_cols[i] + ' against number of collisions (each dot is an LSOA)'
        f.write('<h3 style="font-family:sans-serif;text-align:left;">' + title_text +'</h3>')
        title_text
        corr = for_plot['collisions_count'].corr(for_plot[imd_cols[i]])
        f.write('<p style="font-family:sans-serif;text-align:left;font-size:22px">Correlation of ' + str(corr) + '</p>')

        fig = px.scatter(x = for_plot[imd_cols[i]], y = for_plot['collisions_count'],
                         size = for_plot['All Ages'],
                        labels={
                             "size": "Population",
                              'x': imd_cols[i],
                              'y': 'Number of road collisions'
                        },
                         opacity = 0.5,
                        template = "simple_white",
                         )
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))



###  heatmap
m = folium.Map()

# Layer of collisions in latest year (2019)
collision_2019 = road_collision[road_collision['year'] == road_collision['year'].max()]

# setting bounding box
sw = collision_2019[['latitude', 'longitude']].min().values.tolist()
ne = collision_2019[['latitude', 'longitude']].max().values.tolist()
m.fit_bounds([sw, ne])

# adding a point for each event as one layer
fg = folium.FeatureGroup(name='Collisions', show=True)
for index, row in collision_2019.iterrows():
    folium.CircleMarker([row['latitude'], row['longitude']],
                        radius=4,
                        # popup=row['name'],
                        fill_color="#3db7e4",  # divvy color
                        ).add_to(fg)

# adding heatmap
fg.add_children(plugins.HeatMap(collision_2019[['latitude', 'longitude']], radius=15))
fg.add_to(m)  # saving layer to map

### Making a layer for each crime type
crime_2019 = crime[crime['year'] == crime['year'].max()]

crimes = crime_2019['category'].unique()

for i in range(len(crimes)):
    crime_df = crime_2019[crime_2019['category'] == crimes[i]][['latitude', 'longitude']]
    fg = folium.FeatureGroup(name=crimes[i], show=False)
    for index, row in crime_df.iterrows():
        folium.CircleMarker([row['latitude'], row['longitude']],
                            radius=3,
                            # popup=row['name'],
                            fill_color="#3db7e4",
                            ).add_to(fg)

    # adding heatmap
    fg.add_children(plugins.HeatMap(crime_df[['latitude', 'longitude']], radius=10))
    fg.add_to(m)

# add layer toggle
folium.LayerControl(collapsed=False).add_to(m)

m.save(directory + "/collision_and_crimetypes_heatmap.html")


























