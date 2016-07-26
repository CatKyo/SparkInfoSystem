#encoding=utf-8
import os
import sys
# import random
import json
import jieba
# import shapefile
# import time
# from socketIO_client import SocketIO, LoggingNamespace
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# shpTw = open("./map/Town_MOI_1041215.shp", "rb")
# dbfTw = open("./map/Town_MOI_1041215.dbf", "rb")
# r = shapefile.Reader(shp=shpTw, dbf=dbfTw)
# records = r.records()
# shapeRec = r.shapeRecords()

# def saveFile(time, rdd):
#     cnt = rdd.count()
#     if cnt != 0:
#         path = "/tmp/" + str(time).replace(":", "").replace(" ","-")
#         print("-------------------------------------------")
#         print("Time: %s" % time)
#         print("-------------------------------------------")
#         print("cut result save in: " + path)
#         rdd.saveAsTextFile(path)

def jsonFileTranslate(rdd):
    cnt = rdd.count()
    if cnt != 0:
        for i in range(cnt):
            rdd_list = rdd.take(i+1)
            for i in rdd_list:
                j_file = json.loads(i)
                print i


# def sendLocation(rdd):
#     cnt = rdd.count()
#     if cnt != 0:
#         print ("process rdd number: " + str(cnt))
#         with SocketIO("192.168.4.21", 5000, LoggingNamespace) as socketIO:
#             result = rdd.map(lambda word: locationHashSereach(word))
#             for locate in result.collect():
#                 if locate is not None:
#                     location = str(locate[0]) + "/" + str(locate[1])
#                     socketIO.emit(location)
#                     socketIO.wait_for_callbacks(seconds=1)
#         print ("Finish")

# def locationHashSereach(locationString):
#
#     for i in range(len(shapeRec)):
#         countryID = records[i][9].decode('big5', 'ignore')
#         townID = records[i][3].decode('big5', 'ignore')
#         coordinate = shapeRec[i].shape.points[0]
#         if len(locationString) >= 2:
#             if locationString == townID:
#                 return coordinate[0], coordinate[1]
#             if locationString in countryID:
#                 return coordinate[0], coordinate[1]
#     return None

if __name__ == '__main__':
    os.environ['SPARK_HOME'] = "/usr/lib/spark"
    sys.path.append("/home/kyo/spark-1.6.1-bin-hadoop2.6/python")

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # sc = SparkContext("spark://192.168.4.213:7077", "NetworkWordCount")
    sc = SparkContext("local[2]", "NetworkWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 3)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("192.168.4.213", 9999)

    # Split each line into words


    raw_words = lines.flatMap(lambda line: jieba.cut_for_search(line))
    words = raw_words.filter(lambda line: line not in " ")

    # words.foreachRDD(sendLocation)
    # words.foreachRDD(saveFile)
    lines.foreachRDD(jsonFileTranslate)

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate