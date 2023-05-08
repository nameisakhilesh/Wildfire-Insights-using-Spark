import pandas as pd
from meteostat import Point, Daily
from datetime import datetime
import json
import csv

with open('/Users/akhilesh/WildfireDB_Resources/Weather/us_data_full.json') as f:
    jsondata = json.load(f)
stations = list()
for item in jsondata:
    stations.append(item.get('id'))
#stations = ['70133','70000']
 
# now we will open a file for writing
# us_csv_file = open('/Users/akhilesh/WildfireDB_Resources/Weather/us_csv_file.csv', 'w')
 
# # create the csv writer object
# csv_writer = csv.writer(us_csv_file)

# # Counter variable used for writing
# # headers to the CSV file
# count = 0
 
# for j in jsondata:
#     if count == 0:
 
#         # Writing headers of CSV file
#         header = j.keys()
#         csv_writer.writerow(header)
#         count += 1
 
#     # Writing data of CSV file
#     csv_writer.writerow(j.values())
 
# us_csv_file.close()
stationsdf = pd.read_csv(r'/Users/akhilesh/WildfireDB_Resources/Weather/us_csv_file.csv')
#print(stationsdf.dtypes)
# List of station IDs in the US
#print(stations)
start = datetime(2012, 1, 1)
end = datetime(2012, 12, 31)
# Create an empty DataFrame to store the data
df = pd.DataFrame()

# Loop over the stations and download the data
for station in stations:
    # Create a Point object for the station

    # Download the daily temperature data for the station
    data = Daily(station,start,end)
    data = data.fetch()
    data['id']=station
    dates = pd.date_range(start, periods=len(data), freq='D')
    data['date'] = dates
    #data = data.assign(id = [station])
    #print(data.head())
    

    # Add the data to the DataFrame
    df = pd.concat([df, data])
    #print(df.dtypes)

# Save the data to a CSV file

joined=df.merge(stationsdf,on='id',how='left')
#to remove snow,wpgt,tsun
#to remove elevation, daily,timezone,region,country,name
joined.drop(['snow', 'wpgt','tsun','elevation','daily','timezone','region','country','name'], axis=1, inplace=True)
joined = joined.rename(columns={'id': 'station_id'})
joined = joined.rename(columns={'latitude': 'station_latitude'})
joined = joined.rename(columns={'longitude': 'station_longitude'})
joined = joined.drop(df.columns[0], axis=1)
joined=joined[['station_latitude','station_longitude','tavg', 'tmin', 'tmax', 'prcp', 'wdir', 'wspd', 'pres', 'station_id','date']]

#print(joined)
joined.to_csv('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_2012.csv',index=False)