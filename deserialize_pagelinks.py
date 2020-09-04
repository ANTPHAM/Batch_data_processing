#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:00:59 2019

@author: antoine
"""

import hdfs
import fastavro


hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")


with hdfs_client.read("/data/article_links/master/full/sous-dataset-1/pagelink10385.avro") as of:
    reader = fastavro.reader(of)
    i = 0
    for node in reader:
        if node['pl_title'] == "Cinéma surréaliste":
            print(node)
        else:
            pass
        i += 1


