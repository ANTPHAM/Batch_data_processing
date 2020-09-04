#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 15:29:42 2019

@author: antoine

Read the raw data in the file "frwiki_history.xml", parse it to extract the required information and write the result into HDFS under .avro format
"""
import os
import hdfs
import fastavro
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import iterparse
schema = {
        "namespace":"antoine.opentreetmap",
        "type": "record",
        "name": "Node",
        "fields": [
                {"name": "title", "type": "string"},
                {"name": "contributors", "type": {"type": "array", "items": "string"},
                  "default": {}}
                ]
        }

# Connect to HDFS
hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")

NS = '{http://www.mediawiki.org/xml/export-0.10/}'

for xml in os.listdir('/data/frwiki_history_split'):
    articles = []
    get_last_title = []
    with open("/data/frwiki_history_split/" + xml) as f:
        try:            
            for event, elem in iterparse(f):
                if elem.tag == '{0}page'.format(NS):
                    title = elem.find("{0}title".format(NS))
                    contr = elem.findall(".//{0}username".format(NS))
                   
        
                    if title is not None:
                        title = title.text
                        print ("this is a title: %s"  %title)
                        get_last_title = title.append(title)
                    else:
                        print('no title')
                        title = str(get_last_title)[-1:])
                        print("so get the last title = %s" %title)
                    if contr is not None:
                        contrlist =[]
                        for i in range(len(contr)):
                            print ("this is a contributor: %s" %contr[i].text)
                            contrlist.append(contr[i].text)
                    
                    articles.append({
                   "title": title,
                   "contributors": contrlist
                    })
        
                    elem.clear()
        except:
            print('file not valide')
        print(articles)
        try:
            # serialize the data under .avro format and put it into HDFS
            with hdfs_client.write("/data/article_links/master/full/sous-dataset-1/"+ xml  +".avro",overwrite = True) as avro_file:
                fastavro.writer(avro_file, schema, articles)
        except:
            print('parsing error')


