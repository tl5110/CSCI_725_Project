from pymongo import MongoClient



def connectToMongoDB():
    """
    Establishes a connection to the MongoDB database

    ::return:: active database object and client if successful, None otherwise
    """
    client = MongoClient('localhost', 27017)
    db = client['CSCI_725_Project']
    return db, client