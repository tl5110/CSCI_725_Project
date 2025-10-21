from pymongo import MongoClient


def connectToMongoDB():
    """
    Establishes a connection to the MongoDB database

    ::return:: active database object and client if successful, None otherwise
    """
    client = MongoClient('localhost', 27017)
    db = client['CSCI_725_Project']
    return db, client


def createBankingCollections(db):
    """
    Creates collections for banking application in the database

    ::param db:: active database object
    """
    # Customers Collection
    db.create_collection('Customers', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['customer_id', 'name', 'email'],
            'properties': {
                'customer_id': {'bsonType': 'int'},
                'name': {'bsonType': 'string'},
                'email': {'bsonType': 'string'},
                'phone': {'bsonType': 'string'},
                'creation_date': {'bsonType': 'date'}
            }
        }
    })

    # Accounts Collection
    db.create_collection('Accounts', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['account_id', 'customer_id', 'balance', 'status'],
            'properties': {
                'account_id': {'bsonType': 'int'},
                'customer_id': {'bsonType': 'int'},
                'balance': {'bsonType': 'int'},
                'overdraft_limit': {'bsonType': 'int'},
                'status': {'bsonType': 'string'},
                'creation_date': {'bsonType': 'date'},
                'update_date': {'bsonType': 'date'}
            }
        }
    })

    # Transactions Collection
    db.create_collection('Transactions', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['transaction_id', 'account_id', 'amount', 'type'],
            'properties': {
                'transaction_id': {'bsonType': 'int'},
                'account_id': {'bsonType': 'int'},
                'timestamp': {'bsonType': 'date'},
                'amount': {'bsonType': 'int'},
                'type': {'bsonType': 'string'},
                'transfer_id': {'bsonType': 'int'},
                'channel': {'bsonType': 'string'},
                'merchant_id': {'bsonType': 'int'},
                'note': {'bsonType': 'string'}
            }
        }
    })

    # Merchants Collection
    db.create_collection('Merchants', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['merchant_id', 'name', 'category'],
            'properties': {
                'merchant_id': {'bsonType': 'int'},
                'name': {'bsonType': 'string'},
                'category': {'bsonType': 'string'}
            }
        }
    })


def dropBankingCollections(db):
    """
    Drops collections for banking application in the database

    ::param db:: active database object
    """
    db.Customers.drop()
    db.Accounts.drop()
    db.Transactions.drop()