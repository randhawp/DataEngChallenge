#this script finds out how many android and ios users are in different countries
#currently this script does not handle roaming user and the result will add up if the user
#moves between two countries. Assuming data is on 24 hour basis
%spark.pyspark
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
    img2 = StringIO.StringIO()
    p.savefig(img2, format='svg')
    img2.seek(0)
    print "%html <div style='width:600px'>" + img2.buf + "</div>"



def getDateVal(timestamp):
    from datetime import datetime
    dt = datetime.utcfromtimestamp(timestamp)
    return str(dt.hour)
    #return str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+"-"+str(dt.hour)
    
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

selectedDataRDD = freckleRDD.map(lambda x: (json.loads(x)['idfa'],json.loads(x)['platform'],json.loads(x)['country_code'])  )
selectedDataRDD.take(5)
idfaDF = sqlContext.createDataFrame(selectedDataRDD)
#count all idfa events by the hour
platformlist=idfaDF.groupby('_3','_2').agg(func.countDistinct('_1').alias('Total')).collect()
#print(platformlist)
printableDF = sqlContext.createDataFrame(platformlist,["Country","Platform"]).orderBy('Total', ascending=False)
printableDF.show()

'''
Result is below
+-------+--------+------+
|Country|Platform| Total|
+-------+--------+------+
|     US| android|180486|
|     CA|     ios| 22493|
|     CA| android| 20000|
|     US|     ios|  8532|
|     GU|     ios|  4335|
|     JP| android|   635|
|     MX| android|   550|
|     GB| android|   358|
|     BR| android|   312|
|     TR| android|   220|
|     DE| android|   201|
|     AU| android|   180|
|     ES| android|   133|
|     CO| android|   126|
|     PH| android|   115|
|     PR| android|   101|
|     PK| android|   100|
|     FR| android|    97|
|     TH| android|    86|
|     IN| android|    80|
+-------+--------+------+
only showing top 20 rows





'''