import json
from datetime import datetime
from pymongo import MongoClient


def connectToMongoDB():
    """
    Establishes a connection to the MongoDB database

    ::return:: active database object and client if successful, None otherwise
    """
    client = MongoClient('localhost', 27017)
    db = client['CSCI_725_Project']
    return db, client


def dropBankingCollections(db):
    """
    Drops collections for banking application in the database

    ::param db:: active database object
    """
    db.Customers.drop()
    db.Accounts.drop()
    db.Transactions.drop()
    db.Merchants.drop() 


def createBankingCollections(db):
    """
    Creates collections for banking application in the database

    ::param db:: active database object
    """
    # Customers Collection
    db.create_collection('Customers', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'name', 'email'],
            'properties': {
                '_id': {'bsonType': 'int'},
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
            'required': ['_id', 'customer_id', 'balance', 'status'],
            'properties': {
                '_id': {'bsonType': 'int'},
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
            'required': ['_id', 'account_id', 'amount', 'type'],
            'properties': {
                '_id': {'bsonType': 'int'},
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
            'required': ['_id', 'name', 'category'],
            'properties': {
                '_id': {'bsonType': 'int'},
                'name': {'bsonType': 'string'},
                'category': {'bsonType': 'string'}
            }
        }
    })


def loadSampleData(db, folderDir):
    """
    Loads sample data into the banking collections

    ::param db:: active database object
    ::param folderDir:: folder name containing the JSONL files (baseline, edgecases, hotspot, payday)
    """
    
    if folderDir == 'baseline':
        customersJson = 'banking_datasets/baseline/baseline_customers.jsonl'
        accountsJson = 'banking_datasets/baseline/baseline_accounts.jsonl'
        transactionsJson = 'banking_datasets/baseline/baseline_transactions.jsonl'
        merchantsJson = 'banking_datasets/baseline/baseline_merchants.jsonl'
    elif folderDir == 'edgecases':
        customersJson = 'banking_datasets/edgecases/edgecases_customers.jsonl'
        accountsJson = 'banking_datasets/edgecases/edgecases_accounts.jsonl'
        transactionsJson = 'banking_datasets/edgecases/edgecases_transactions.jsonl'
        merchantsJson = 'banking_datasets/edgecases/edgecases_merchants.jsonl'
    elif folderDir == 'hotspot':
        customersJson = 'banking_datasets/hotspot/hotspot_customers.jsonl'
        accountsJson = 'banking_datasets/hotspot/hotspot_accounts.jsonl'
        transactionsJson = 'banking_datasets/hotspot/hotspot_transactions.jsonl'
        merchantsJson = 'banking_datasets/hotspot/hotspot_merchants.jsonl'
    elif folderDir == 'payday':
        customersJson = 'banking_datasets/payday/payday_customers.jsonl'
        accountsJson = 'banking_datasets/payday/payday_accounts.jsonl'
        transactionsJson = 'banking_datasets/payday/payday_transactions.jsonl'
        merchantsJson = 'banking_datasets/payday/payday_merchants.jsonl'
    else:
        print("Invalid folder directory specified.")
        return

    with open(customersJson, 'r') as customerFile:
        for line in customerFile:
            customer = json.loads(line)
            customerInfo = {
                '_id': customer['customerId'],
                'name': customer['name'],
                'email': customer['email'],
                'phone': customer['phone'],
                'creation_date': datetime.fromisoformat(customer['createdAt'].replace('Z', '+00:00'))
            }
            db.Customers.insert_one(customerInfo)

    with open(accountsJson, 'r') as accountFile:
        for line in accountFile:
            account = json.loads(line)
            accountInfo = {
                '_id': account['accountId'],
                'customer_id': account['customerId'],
                'balance': account['balanceCents'],
                'overdraft_limit': account['overdraftCents'],
                'status': account['status'],
                'creation_date': datetime.fromisoformat(account['openedAt'].replace('Z', '+00:00')),
                'update_date': datetime.fromisoformat(account['updatedAt'].replace('Z', '+00:00'))
            }
            db.Accounts.insert_one(accountInfo)

    with open(transactionsJson, 'r') as transactionFile:
        for line in transactionFile:
            transaction = json.loads(line)
            transactionInfo = {
                '_id': transaction['txnId'],
                'account_id': transaction['accountId'],
                'timestamp': datetime.fromisoformat(transaction['ts'].replace('Z', '+00:00')),
                'amount': transaction['amountCents'],
                'type': transaction['type'],
                'transfer_id': transaction['transferId'],
                'channel': transaction['channel'],
                'merchant_id': transaction['merchantId'],
                'note': transaction['note']
            }
            db.Transactions.insert_one(transactionInfo)

    with open(merchantsJson, 'r') as merchantFile:
        for line in merchantFile:
            merchant = json.loads(line)
            merchantInfo = {
                '_id': merchant['merchantId'],
                'name': merchant['name'],
                'category': merchant['category']
            }
            db.Merchants.insert_one(merchantInfo)



def openAccount(db, customerId):
    # TODO
    return None


def deposit(db, customerId, amount):
    # TODO
    return None



def withdraw(db, customerId, amount):
    # TODO
    return None


def transfer(db, customerId, fromAccId, toAccId, amount):
    """
    Transfers amount from one account to another account/merchant for a customer, creating
    a transfer for each account. So if transferring $50 from Account A to Account B, 
    there should be two transactions created: 
        One debit of $50 from Account A
        One credit of $50 to Account B
    """
    # TODO
    return None

def getBalance(db, customerId):
    # TODO
    return None

def viewRecentTransactions(db, customerId):
    # TODO
    return None


def closeAccount(db, customerId):
    # TODO
    return None


def main():
    db, client = connectToMongoDB()

    dropBankingCollections(db)
    createBankingCollections(db)

    client.close()

if __name__ == "__main__":
    main()