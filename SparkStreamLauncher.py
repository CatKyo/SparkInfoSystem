
import os
import sys
import json
from json import dump
import jieba
import zmq
# import random
# import shapefile
# import time
# from socketIO_client import SocketIO, LoggingNamespace
from jieba import analyse
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from hdfs import InsecureClient
import hdfs

def saveJsonFileToHDFS(jsonFile):
    if jsonFile is not None:
        jsonFile["HDFSurl"] = "/tmp/" + jsonFile["Date"] + jsonFile["Title"] + ".txt"
        os.system(("echo '%s' | hadoop fs -put - '" + jsonFile["HDFSurl"] + "'") % (json.dumps(jsonFile)))
        print("savePath: " + jsonFile["HDFSurl"])

def jsonFileTranslate(rdd):
    cnt = rdd.count()
    if cnt != 0:
        for rd in rdd.collect():
            j_file = json.loads(rd)
            j_file["KeyWord"] = analyse.textrank(j_file["Text"])
            j_file["SplitText"] = " ".join(jieba.cut_for_search(j_file["Text"]))
            j_file["TitleKey"] = analyse.textrank(j_file["Title"])
            print(j_file["Title"])
            print(j_file["KeyWord"])
            saveJsonFileToHDFS(j_file)
            # sendjson(j_file)

def sendjson(json_file):
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://192.168.4.213:5557")
    zmq_socket.send_json(json_file)

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

# shpTw = open("./map/Town_MOI_1041215.shp", "rb")
# dbfTw = open("./map/Town_MOI_1041215.dbf", "rb")
# r = shapefile.Reader(shp=shpTw, dbf=dbfTw)
# records = r.records()
# shapeRec = r.shapeRecords()

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
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    sys.path.append("/home/kyo/spark-1.6.1-bin-hadoop2.6/python")

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("spark://192.168.4.213:7077", "DataInfoSys")
    # sc = SparkContext("local[2]", "DataInfoSys")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 3)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("192.168.4.213", 9999)

    # Split each line into words
    raw_words = lines.flatMap(lambda line: jieba.cut_for_search(line))
    words = raw_words.filter(lambda line: line not in " ")

    lines.foreachRDD(jsonFileTranslate)

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate