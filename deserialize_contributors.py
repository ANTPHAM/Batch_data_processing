#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 18:00:59 2019

@author: antoine
"""

import hdfs
import fastavro


hdfs_client = hdfs.InsecureClient("http://0.0.0.0:50070")
with hdfs_client.read("/data/article_links/master/full/sous-dataset-1/frwiki-latest-stub-meta-history.00159.xml.avro") as of:
    reader = fastavro.reader(of)
    for node in reader:
        print(node)

