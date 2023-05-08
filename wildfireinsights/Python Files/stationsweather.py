import json
with open('/Users/akhilesh/Downloads/full.json', 'r') as f:
    data = json.load(f)

# Filter the data to keep only records where "region":"US"
us_data = [d for d in data if d.get('country') == 'US']
us_data_copy=[]
count=0
for i in us_data:
    if 'identifiers' in i:
        del i['identifiers']
    dly = i["inventory"]["daily"]
    del i["inventory"]
    i["daily"]=dly
    max_date = "2012-01-01"
    
    if i['daily'] and i['daily']['end']!=None and i['daily']['end'] >= max_date:
        us_data_copy.append(i.copy())


# Save the filtered data to a new JSON file
with open('/Users/akhilesh/WildfireDB_Resources/Weather/us_data_full.json', 'w') as f:
    json.dump(us_data_copy, f, indent=4)

with open('/Users/akhilesh/WildfireDB_Resources/Weather/us_data_full.json') as f:
    data = json.load(f)

# Extract the "name" value and remove the "name" key
for item in data:

    # Extract the "name" value
    name = item["name"]["en"]

    # Extract the "location" values and remove the "location" key
    latitude = item["location"].pop("latitude")
    longitude = item["location"].pop("longitude")
    elevation = item["location"].pop("elevation")

    # Update the dictionary with the new structure
    item.update({
        "name": name,
        "latitude": latitude,
        "longitude": longitude,
        "elevation": elevation
    })
    del item['location']


# Write the modified dictionary back to the JSON file
with open('/Users/akhilesh/WildfireDB_Resources/Weather/us_data_full.json', 'w') as f:
    json.dump(data, f, indent=4)