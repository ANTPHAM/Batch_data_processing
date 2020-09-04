#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 15:29:42 2019

@author: antoine

Read the raw data in the file "frwiki-latest-pagelinks.sql", parse it to extract the required information and write the result into HDFS under .avro format
"""

import hdfs
import fastavro
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import time
import re
import findspark
findspark.init("./spark-2.4.4")

schema = {
        "namespace":"antoine.opentreetmap",
        "type": "record",
        "name": "Node",
        "fields": [
                {"name": "pl_from", "type": "int", "default": {}},
                {"name": "pl_title", "type":  "string",
                  "default": {}}
                ]
        }

hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")

# creating a Spark context
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

def parser(row):
    try:
        r = row[1]
        R = r.split('),')
        nodes =[]
        for i in range(len(R)):
            try:
                page_id = int(R[i].strip().split(',')[::2][0].split('(')[1])
            except:
                print("\nOut of parsing caused by ValueError")
                continue
            s = R[i].strip().split(',')[::2][1]
            s = re.findall("[^\W\d_]+",s)
            article = ' '.join(s)
            dic = {"pl_from":page_id,"pl_title":article}
            nodes.append(dic)
        
        return nodes
    except:
        print("\nOut of parsing caused by IndexError")
        
    

def serializing(row,index):
    with hdfs_client.write("/data/article_links/master/"+ "pagelink" + str(index) + ".avro",overwrite = True) as avro_file:
        fastavro.writer(avro_file, schema, row)
             

# reading the original .sql file 
results = sc.textFile("./data/frwiki-latest-pagelinks.sql")
# build a RDD pipeline
sql_rdd = results.map(lambda row: row.split("INSERT INTO `pagelinks` VALUES "))

print("\nstart parsing")   
sql_rdd1 = sql_rdd.map(lambda row : parser(row)).filter(lambda row: row is not None).zipWithIndex()
sql_rdd1.persist()

sql_lineLengths = sql_rdd1.map(lambda s: len(s))
sql_totalLength = sql_lineLengths.reduce(lambda a, b: a + b)

# serializing the data stored in this RDD an put it into the hdfs directory
start_time = time.time()
for i in rangele(0,sql_totalLength): 
    k = sql_rdd1.filter(lambda key: key[1] == i).map(lambda key: key[0]).collect()[0]
    serializing(k,i)
    print("\n%d .avro file is donne" %i)
    
print("\nserializing took--- %f secondes ---" % (time.time() - start_time))

sc.stop()

