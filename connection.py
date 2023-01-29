#!/usr/bin/python3

import os
import json
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

def database():
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['database']
    engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}/{conf['database']}")
    return engine

def warehouse():
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['warehouse']
    engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}/{conf['database']}")
    return engine

def spark(app):
    path = open(os.getcwd()+'/config.json')
    conf = json.load(path)['spark']
    spark = SparkSession.builder.master(conf['master']).appName(app).getOrCreate()
    return spark