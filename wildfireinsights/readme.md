# WildfireDB
WildfireDB : an open-source dataset that links wildfire occurrence with relevant features 

## Introduction
---

Recent wildfires in the United States and other parts of the world have resulted in loss of life and billions of dollars, destroying countless structures and forests. Fighting wildfires is extremely complex. A major problem in using data-driven models to combat wildfires is the lack of comprehensive data sources that relate fires with relevant covariates. We present the first open-source wildfire dataset that combines historical wildifre occurrences with relevant features extracted from satellite imagery. Our dataset, with over 17 million data points, is created using a novel approach to process large-scale raster and vector data. The data is created and maintained by a group of researchers from Vanderbilt University, University of California, Riverside, and Stanford University.


## Release Description
---

|Version|Release Date|Associated Publication|Location|     
|----|-----|-------|-------|      
|1.1|June 10, 2021|[Link](https://ai4earthscience.github.io/neurips-2020-workshop/papers/ai4earth_neurips_2020_43.pdf)|[Link](https://drive.google.com/file/d/1B582y8_cPWxNuevpm3ZM-SZf_23HRUAQ/view?usp=sharing)|



## Data Sources
---

We gather the raster dataset of [vegetation](https://www.landfire.gov/vegetation.php), [fuel type](https://www.landfire.gov/fuel.php), and [topography](https://www.landfire.gov/topographic.php) of years 2012, 2014, 2016, 2018 and 2020 from the LANDFIRE website. They are in 30-meter square cells.

The near real-time (NRT) [fire occurrence data](https://firms2.modaps.eosdis.nasa.gov/map/#d:2020-09-20..2020-09-21;@0.0,0.0,3z) in vector form are from the [Visible Infrared Imaging Radiometer Suite (VIIRS)](https://earthdata.nasa.gov/earth-observation-data/near-real-time/download-nrt-data/viirs-nrt) thermalanomalies/active fire database. They are in 375-meter square cells.

We collect weather data from [Meteostat](https://meteostat.net/en), a free online service which provides weather and climate statistics around the globe. For continental USA, Meteostat collects raw data from the National Oceanic and Atmospheric Administration (NOAA). We gather aggregated daily weather data for 5,787 weather stations in the continental USA.


## Collecting the Data
----

### FIRMS Dataset
---

The vector data can be directly downloaded from the [VIIRS Website](https://firms.modaps.eosdis.nasa.gov/download/) by just selecting the area of interest or the country and selecting the year or you can make a new request to download for a specific area of interest and a particular time period as well.



### LANDFIRE Dataset

---

The raster data is a huge part of the dataset and can be exponentially large if the area of interest keeps expanding. To simplify things we write a simple API which can download the data from the [LANDFIRE website](https://landfire.gov/version_download.php).

At the time of writing the LANDFIRE Website provides an LFPS(LANDFIRE Products Service) API to get the data from the website. The hyperlink here provides a simple [user guide](https://lfps.usgs.gov/helpdocs/LFProductsServiceUserGuide.pdf) on how to download the data. The `Landfiredownload.py` provides a simple code to download the data from LFPS.

We need to specify the features that we need or the Layer List which can be found [here](https://landfire.gov/comparison_table.php) and Area of Interest to the base URL to get any JSON responses from the website. The Area of Interest needs to be the bounding box which is defined by latitude and longitude in decimal degrees in WGS84 and listed in the following order: lower left longitude, lower left latitude, upper right longitude,
upper right latitude.

Example Code:
```python
base_url = "https://lfps.usgs.gov/arcgis/rest/services/LandfireProductService/GPServer/LandfireProductService/submitJob?"

Layer_List = ["220CBH_22","220CBD_22","220CC_22","220CH_22","220EVC_22","220EVT","ELEV2020","SLPD2020"]

Area_Of_Interest = [-123.7835, 41.7534, -123.6352, 41.8042]
```
The response is a zip file which has the relevant files with features specified. As we are interested in the raster data, we consider taking only the `.tif` file.



### Meteostat Weather Dataset

---

Meteostat provides a [user guide](https://dev.meteostat.net/guide.html#our-services) for downloading the data. There are multiple nterfaces for downloading the data but for simplicity we'll choose python for now but anything works. the python code for downloading the weather data is written in `weatherdownload.py` file.

Before using `weatherdownload.py` file, you need to run `stationsweather.py` to download the list of all the weather stations in the US. 

You need to mention the start and end times for your time period of interest. The user guide mentions all the parameters like stations, weather parameters, etc. You can use that to follow all the steps and download the required data.



### Deploying in Beast

---

[Beast](https://bitbucket.org/bdlabucr/beast) is a Spark add-on for Big Exploratory Analytics of Spatio-temporal data. We deploy our project on beast to find zonal statistics for all the cells and then compute the neighbouring cells. Before we find the zonal statistics let's install all the dependencies, prerequisites required for our project to run on beast.

In order to use Beast, you need the following prerequisites installed on your machine.

* Java Development Kit (JDK). [Oracle JDK 1.8](https://www.oracle.com/technetwork/java/javase/downloads/index.html) or later is recommended.
* [Apache Maven](https://maven.apache.org/) or [SBT](https://www.scala-sbt.org/).
* [Git](https://git-scm.com/).
* [Spark](https://spark.apache.org/).

After installing the prerequisites the next step is to install Beast using command line interface by following the commands.

```bash
wget https://bitbucket.org/bdlabucr/beast/downloads/beast-0.9.5-RC2-bin.tar.gz
tar -xvzf beast-0.9.5-RC2-bin.tar.gz
echo 'export PATH=$PATH:'`pwd`/beast-0.9.5-RC2/bin >> .bashrc 
```
Donot forget to set your maven or SBT path to the Home as well
```bash
export PATH=$HOME/apache-maven-3.8.6/bin:$PATH 
```

You can test if Beast has been correctly installed or not by following the [README GUIDE](https://bitbucket.org/bdlabucr/beast/src/a593d1926c7632c44a1736d161d98b8e5ab74b09/doc/installation.md) in the [Beast](https://bitbucket.org/bdlabucr/beast) Documentation.

The next step is to setup your IDE.

[IntelliJ](https://www.jetbrains.com/idea/) is recommended to run the beast project. But any other IDE would work but would raise Maven or SBT issues and IntelliJ IDE is easier to resolve such issues. 

After installing the IDE next step is to setup your project.

Open the project with adding the maven or SBT dependencies following the user guide mentioned in the dev-setup. This is an important step because if the dependencies are not correctly installed then the Beast project will not run properly. If there any dependecies errors found check if all the prerequsites were installed properly or not. 

To run our project on beast we need to compile the code and generate a JAR file. For doing that we need to run `mvn package`. This will generate two `.JAR` files in the `target`subdirectory named, `beast-examples-<version>.jar` and `beast-uber-examples-<version>.jar`. To run any of the examples, using one of the following commands:  

```bash
spark-submit beast-uber-examples-<version>.jar <command>
```
OR
```bash
beast --jars beast-examples-<version>.jar <command>
```
For generating zonal stats with single machine you can run the scala file
```
BeastScala.scala
```
This will inturn generate all the zonal statistics for all the cells by using Raptor Join method from Beast and will save all the csv files to the output path mentioned in the scala file 

The next step is to generate all the neighbouring cells and append it to the zonal statistics that were previously generated. This is done if you run the python file
```
neighbors.py
```
The next step is to find fire points for each cell, this can be done by running the scala file
```
sjtest.scala
```
The input files to `sjtest.scala` would be the california vector file and fire occurence data.

Step 4 is to generate all the tuples. this can be done by running the python script 
```
firetuple.py
```
The input files to this script is the file generated from `neighbors.py` and `sjtest.scala`. This generates all the tuples, the last step is to add weather data to it.

First we need to find the nearest weather stations for a cell. To do that we run the python script 
```
neareststation.py
```

Then we combine all the data together, this python script will generate the final dataset

```
weatherjoin.py
```


