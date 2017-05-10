Release Notes

V 0.0.1
Date: 8 May 2017

About the solution

Lang: Python 2.7
Platform: Zeppelin 0.7
Architecture: Standalone Execution Mode On a single instance of M4.Large EC2 

Solution organization:
There are 3 parts to the solution as required
(a) Data exploration does a statistical review of the data based on hourly breakdown
(b) GeoHash - investigates the clustering of data based on a city level geography
(c) Research - looks at trends in data ingestion which may help in elastically scaling up/down the compute resources


v 0.0.2
Date 9 May 2017

New Features
a) Added automatic geocode conversion for naming location where a cluster of people are found.
Discovers number of people in a chosen distance i.e room, building, city, metro, continenet level and gives a name to the location
In the current version the name at larger distances is that of the closest city to the center. When room or building level is selected then the address is of street level.

b) Fixed environement issues with JVM that cause crashing of process writing to parquet files
