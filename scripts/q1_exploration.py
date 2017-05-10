%spark.pyspark
#the result of this script is in ans2_geohash.txt
#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as func
import json


def getDateVal(timestamp):
    from datetime import datetime
    dt = datetime.utcfromtimestamp(timestamp)
    return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+"-"+str(dt.hour)
    
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
#freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/test4")

#get all events for all the idfa's
idfaRDD = freckleRDD.map(lambda x: (json.loads(x)['idfa'],getDateVal(json.loads(x)['event_time'])) )
idfaDF = sqlContext.createDataFrame(idfaRDD)
#idfaDF.show()

#count all idfa events by the hour
hourly_idfa_detection_count_list=idfaDF.groupby('_1','_2').agg(func.count('_2').alias('HourlySum')).collect()
hourlyDetectionCount_idfaDF = sqlContext.createDataFrame(hourly_idfa_detection_count_list)
#hourlyDetectionCount_idfaDF.show()
#dailyDetectionCount_idfaDF.write.parquet("/home/ubuntu/data/data-statistics-sample-location-data")

#find the min,max,avg,stddev for all idfa across the day, i.e find which hour max or min and avg across the whole day
stats_on_daily_sightings_list=hourlyDetectionCount_idfaDF.groupby('_1').agg(func.max('HourlySum').alias('max'),func.avg('HourlySum').alias('avg'),func.min('HourlySum').alias('min'),func.stddev('HourlySum').alias('stddev'),func.sum('HourlySum').alias('total') ).collect()
statisticsBasedOnHourly_idfaDF = sqlContext.createDataFrame(stats_on_daily_sightings_list,["IDFA"])
#statisticsBasedOnHourly_idfaDF.show()

#some easy sql to play with data 
statisticsBasedOnHourly_idfaDF.createOrReplaceTempView("statview")
SpSession.sql("select * from statview order by total DESC").show()