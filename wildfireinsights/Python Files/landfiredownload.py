import requests 
from zipfile import ZipFile
import io
import json
import time

def buildurl(params):
    '''
    params is a dictionary containing the parameters neded to  build the url, I will later use this to build the url properly rather than hardcoding
    and I have to implement Error handling mechanisms as well. 
    '''
    base_url = "https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/submitJob?"
    Layer_List = ["220CBH_22","220CBD_22","220CC_22","220CH_22","220EVC_22","220EVH_22","220EVT","ELEV2020","SLPD2020"]
    Area_Of_Interest=[-123.7835, 41.7534, -123.6352, 41.8042]    
    
if __name__ == "__main__":

    #I am adding the Layer List and Area of Interest to the base URL, here I am taking the example as first_url where I am taking all the Layers and a sample Area of Interest

    first_url = "https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/submitJob?f=json&Layer_List=220CBH_22%3B220CBD_22%3B220CC_22%3B220CH_22%3B220EVC_22%3B220EVH_22%3B220EVT%3BELEV2020%3BSLPD2020&Area_Of_Interest=13"


    first_response = requests.get(first_url).content 

    job_details = json.loads(first_response)

    job_id = job_details["jobId"]
    job_status = job_details["jobStatus"]

    status_url = f"https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/jobs/{job_id}?f=json"

    print(f"Check your status here: {status_url.strip('?f=json')}")

    #I am checking status every second untill the jobstatus changes to Succeeded

    while (job_status != "esriJobSucceeded"):
        status = requests.get(status_url)
        job_status = json.loads(status.content)["jobStatus"]
        print(job_status)
        time.sleep(100)

    #After the job gets succeded we can download the data using the job_id 

    download_url = requests.get(f"https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/jobs/{job_id}/results/Output_File?f=json")

    download_json = json.loads(download_url.content)

    print(f"https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/jobs/{job_id}/results/Output_File?f=json")

    print(download_json['value']['url'])

    #the response is a zip file

    response_zip = requests.get(download_json['value']['url'])

    zfile = ZipFile(io.BytesIO(response_zip.content))

    file_names = zfile.namelist()

    print(file_names)

    #I am just using an example here to show the xml file that got downloaded

    content = zfile.open(file_names[4], 'r').read()

    #result_xml = content.decode('utf-8')

    with open(f"{job_id}.tif", 'w') as file:
        file.write(result_tif)