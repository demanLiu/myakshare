import pymongo

def getClient():
    return pymongo.MongoClient("mongodb://localhost:27017/")

def getDataBase(name):
    return getClient()[name]

def  getCol(dbName,colName):
    return getDataBase(dbName)[colName]