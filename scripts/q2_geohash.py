%spark.pyspark
#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as func
import json
import Geohash
import geocoder

def getDateVal(timestamp):
    from datetime import datetime
    dt = datetime.utcfromtimestamp(timestamp)
    return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+"-"+str(dt.hour)

def buildGeoHash(lat,lng,precision):
    cellSize={'ROOM':9,'BUILDING':7,'SUBURB':6,'CITY':4,'METRO':3,'CONTINENT':1}
    precisionval = cellSize.get(precision)
    try:
        if(precisionval==None):
            precisionval=12 #full precision
    except:
        precisionval=12
    finally:
        hash = Geohash.encode(lat,lng)
        return hash[:precisionval]
''' 
Cell precision based on length of hash (at higher lats is the width becomes smaller, 0 at the poles)
Length	Cell width	Cell height
1	= 5,000km	×	5,000km
2	= 1,250km	×	625km
3	= 156km	×	156km
4	= 39.1km	×	19.5km
5	= 4.89km	×	4.89km
6	= 1.22km	×	0.61km
7	= 153m	×	153m
8	= 38.2m	×	19.1m
9	= 4.77m	×	4.77m
10	= 1.19m	×	0.596m
11	= 149mm	×	149mm
12	= 37.2mm	×	18.6mm

'''
def reverseLookup(pdf,precision,sz):
    addr = [sz]
    for row in pdf['GeoHash']:
        vlatlng=Geohash.decode(row)
        lat=vlatlng[0]
        lng=vlatlng[1]
        if(float(lat)==0 and float(lng)==0):
            continue
        loc = geocoder.google([lat,lng],method='reverse')
        if(precision =="CITY" or precision=="METRO"):
            try:
                addr.append(str(loc.city)+","+str(loc.country))
            except:
                addr.append("Addr not found")
        if(precision == "BUILDING" or precision=="ROOM"):
            try:
                addr.append(str(loc.street)+","+str(loc.city)+","+str(loc.country))    
            except:
                addr.append("Addr not found")
        if(precision == "CONTINENT"):
            try:
                addr.append(str(loc.country))    
            except:
                addr.append("Addr not found")
 
    pdf["Address"]=addr
        
    return pdf


def getLocationDetail(lat,lng):

    loc = geocoder.google([lat,lng],method='reverse')
    return loc

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("test") \
    .config("spark.executor.memory", "3g") \
    .config("spark.cores.max","3") \
    .config("spark.sql.warehouse.dir", "/home/ubuntu/temp")\
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
SpContext = SpSession.sparkContext

#parallelize read into a rdd for all the gz files
freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/*.gz")
#freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/test5")

precision="BUILDING"
#get all events for all the idfa's this is the who, when and where
idfaGeohashRDD = freckleRDD.map(lambda x: (json.loads(x)['idfa'],getDateVal(json.loads(x)['event_time']), buildGeoHash(json.loads(x)['lat'],json.loads(x)['lng'],precision) ) )
idfaGeoHashDF = sqlContext.createDataFrame(idfaGeohashRDD)
#idfaGeoHashDF.show()
#idfaGeoHashDF.count()

# group by location (geohash) with precision set at RDD creation time and count the distinct idfa that visited the location at any time. No time granulatiy
geoClusterRDD=idfaGeoHashDF.groupby('_3').agg(func.countDistinct('_1').alias('DistinctVisitorsAtLocationOnAnyDay')).collect()
geoClusterDF = sqlContext.createDataFrame(geoClusterRDD,["GeoHash","UniqueVisitors"]).filter("`UniqueVisitors` >= 1").orderBy('UniqueVisitors', ascending=False)
geoClusterDF.show()


LOOKUP_SIZE=50
dfp = geoClusterDF.toPandas()
temppdf = dfp[:50]

topNpdf = reverseLookup(temppdf,precision,LOOKUP_SIZE)
print(topNpdf)
# For each row in the column,

'''
Uncomment the next 3 lines if you want to find the clustering on an hourly basis. By default the above code does it on the whole data set
group by location (geohash) with precision set at RDD creation time and count the distinct idfa there at the granulaity by the hour i.e 1pm-2pm

'''
#geoClusterRDD=idfaGeoHashDF.groupby('_3','_2').agg(func.countDistinct('_1').alias('DistinctVisitors')).collect()
#geoClusterDF = sqlContext.createDataFrame(geoClusterRDD,["GeoHash","YMD-Hour","UniqueVisitors"])
#geoClusterDF.show()


'''
Uncomment the next 3 lines if sql queries are to be enabled.
'''
#some easy sql to play with data 
#geoClusterDF.createOrReplaceTempView("geoview")
#SpSession.sql("select GeoHash, UniqueVisitors from geoview order by UniqueVisitors DESC").show()



