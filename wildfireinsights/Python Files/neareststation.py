import pandas as pd
from shapely.geometry import Point, Polygon, MultiPoint
from shapely.ops import nearest_points
from shapely import wkt

# read the polygons dataframe
polygons_df = pd.read_csv("/Users/akhilesh/WildfireDB_Resources/cal_grid_wgs83csv.csv",delimiter='\t')

# read the stations dataframe
stations_df = pd.read_csv("/Users/akhilesh/WildfireDB_Resources/Weather/us_csv_file.csv")

stations_df['geometry'] = stations_df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)


print(stations_df.head(5))
print(polygons_df.head(5))

# Define the buffer distance in degrees (assuming the coordinates are in WGS84)
buffer_distance = 160 / 111  # 160 km divided by the approximate length of 1 degree in km (111 km)

def find_nearest_station_within_radius(polygon_wkt):
    polygon = wkt.loads(polygon_wkt)
    # Find the centroid of the polygon
    centroid = polygon.centroid

    # Create a 160 km buffer around the centroid
    buffered_area = centroid.buffer(buffer_distance)

    # Find the stations within the buffered area
    stations_within_radius = stations_df[stations_df['geometry'].apply(lambda x: x.intersects(buffered_area))]

    if not stations_within_radius.empty:
        # Create a MultiPoint geometry from the 'geometry' column of the stations_within_radius dataframe
        station_points = MultiPoint(stations_within_radius['geometry'].tolist())

        # Find the nearest station using the nearest_points function
        nearest_station = nearest_points(centroid, station_points)[1]

        # Calculate the distance between the centroid and the nearest station
        distance = centroid.distance(nearest_station)

        # Get the station_id of the nearest station
        nearest_station_id = stations_within_radius[stations_within_radius['geometry'] == nearest_station]['id'].values[0]
    else:
        nearest_station_id = None
        distance = None
    return nearest_station_id

def find_nearest_station(polygon_wkt):
    polygon = wkt.loads(polygon_wkt)
    # Find the centroid of the polygon
    centroid = polygon.centroid
    print(centroid)

    station_points = MultiPoint(stations_df['geometry'].tolist())

    # Find the nearest station using the nearest_points function
    nearest_station = nearest_points(centroid, station_points)[1]
    
    # Calculate the distance between the centroid and the nearest station
    # Get the station_id of the nearest station
    nearest_station_id = stations_df[stations_df['geometry'] == nearest_station]['id'].values[0]

    return nearest_station_id

polygons_df['nearest_station_id']= polygons_df['WKT'].apply(find_nearest_station_within_radius)
polygons_df.to_csv('/Users/akhilesh/WildfireDB_Resources/polygonweather.csv',index=False)
print(polygons_df.head(5))

# convert the stations dataframe to a GeoDataFrame
# stations_gdf = gpd.GeoDataFrame(
#     stations_df, geometry=gpd.points_from_xy(stations_df.longitude, stations_df.latitude)
# )

# # create a function to find the nearest station for each polygon
# def find_nearest_station(row, stations_gdf):
#     polygon = row.geometry
#     nearest_station = stations_gdf.distance(polygon).idxmin()
#     return pd.Series({
#         'distance': stations_gdf.loc[nearest_station, 'geometry'].distance(polygon),
#         'closest_station_id': stations_gdf.loc[nearest_station, 'station_id']
#     })

# # apply the function to the polygons dataframe
# result_df = polygons_df.apply(find_nearest_station, stations_gdf=stations_gdf, axis=1)
# result_df = pd.concat([polygons_df, result_df], axis=1)

# # show the resulting dataframe
# result_df.head()
