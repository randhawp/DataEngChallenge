%spark.pyspark
#the result of this script is in the pdf file "research on data data ingestion across the data.pdf"
#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as func
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import StringIO
import pandas as pd
import StringIO
import json

def show(p):
    imgx = StringIO.StringIO()
    p.savefig(imgx, format='svg')
    imgx.seek(0)
    print "%html <div style='width:600px'>" + imgx.buf + "</div>"


# only interested in hour of the day from the time value
def getDateVal(timestamp):
    from datetime import datetime
    dt = datetime.utcfromtimestamp(timestamp)
    return str(dt.hour)
    #return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+"-"+str(dt.hour)
    
#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.executor.memory", "4g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/home/ubuntu/temp")\
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
SpContext = SpSession.sparkContext

#parallelize read into a rdd for all the gz files
freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/*.gz")
#freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/test4")

idfaRDD = freckleRDD.map(lambda x: (json.loads(x)['idfa'],getDateVal(json.loads(x)['event_time'])) )
idfaDF = sqlContext.createDataFrame(idfaRDD)
#count all idfa events by the hour
hourly_idfa_detection_count_list=idfaDF.groupby('_1','_2').agg(func.count('_2').alias('HourlySum')).collect()
hourlyDetectionCount_idfaDF = sqlContext.createDataFrame(hourly_idfa_detection_count_list,["IDFA","HOUR"])

hourlyTrendlist = hourlyDetectionCount_idfaDF.groupby('Hour').agg(func.sum('HourlySum').alias('TotalSum')).collect()
hourlyTrendDF = sqlContext.createDataFrame(hourlyTrendlist)
hourlyTrendDF.show()
dfp = hourlyTrendDF.toPandas()
x = dfp.TotalSum
y=dfp.Hour
plt.xlabel('No Events')
plt.ylabel('Hour of Day')
plt.title('Date Ingestion Trend Over 24 Hr period')
plt.scatter(x, y)
plt.show()
show(plt)


# enable
#hourlyDetectionCount_idfaDF.createOrReplaceTempView("statview")
#SpSession.sql("select * from statview").show()
#SpSession.sql("select HOUR, sum(HourlySum) from statview group by HOUR order by Sum(HourlySum) ").show()
