import configuration
from pymongo import MongoClient
from flask import g

def connect_db():
    mongoclient = MongoClient(configuration.MONGOHOST, configuration.MONGOPORT)
    return mongoclient[configuration.MONGOTABLE]

def get_db():
    # Bind db connection to application context variable 'g'
    if not hasattr(g, 'mongodb'):
        g.mongodb = connect_db()
    return g.mongodb