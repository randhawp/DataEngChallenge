'''
If the idfa is really unique and data is really accurate then:
There is an very interesting aspect to the data. Using the location data we can determine 
how much distance the users are moving, are the travelling across countries and if yes 
how many and between which places. 

Lets attempt to find out first finding about cross border travel. 
This script finds the total number of users that moved beween Canada and USA

'''

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

# find travellers between the following two countries
COUNTRY_1="CA"
COUNTRY_2="US"

#parallelize read into a rdd for all the gz files
freckleRDD = SpContext.textFile("/home/ubuntu/data/location-data-sample/*.gz")

country1RDD = freckleRDD.map(lambda x: (json.loads(x)['idfa'],(json.loads(x)['country_code'],json.loads(x)['city'],json.loads(x)['state'], getDateVal(json.loads(x)['event_time']) ))  ).filter( lambda x: COUNTRY_1 in x[1])
country2RDD= freckleRDD.map(lambda x: (json.loads(x)['idfa'],(json.loads(x)['country_code'],json.loads(x)['city'],json.loads(x)['state'], getDateVal(json.loads(x)['event_time']) ) )  ).filter( lambda x: COUNTRY_2 in x[1])
travellerRDD = country1RDD.join(country2RDD)

travellerDF = sqlContext.createDataFrame(travellerRDD)
countTotal=travellerDF.agg(func.countDistinct('_1').alias('Total')).collect()
print(countTotal)


'''
And the answer is 770

A total of 770 idfa were found on the same date, (different hours) in Canada and US

This script is not perfect, it should be able to loop across all the countries, but that will be a cartesian operation.
Till I figure a better way out, lets use named countries as above
'''


