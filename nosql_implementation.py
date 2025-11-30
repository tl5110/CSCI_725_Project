import json
from datetime import datetime, UTC
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

WRITE_CONCERN = WriteConcern("majority", wtimeout=10000)
READ_CONCERN = ReadConcern("snapshot")

# -------------------------------------------------------------------------------------------------
# Creation and Loading
# -------------------------------------------------------------------------------------------------

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
                'type': {'bsonType': ['string', 'null']},
                'transfer_id': {'bsonType': ['int', 'null']},
                'channel': {'bsonType': ['string', 'null']},
                'merchant_id': {'bsonType': ['int', 'null']},
                'note': {'bsonType': ['string', 'null']}
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

    createBankingIndexes(db)


def createBankingIndexes(db):
    """
    Creates indexes needed for banking operations and analysis.

    ::param db:: active database object
    """
    db.Transactions.create_index(
        [('transfer_id', 1)],
        name='transfer_id_lookup',
        partialFilterExpression={'transfer_id': {'$type': 'int'}}
    )

    db.Transactions.create_index(
        [('account_id', 1), ('timestamp', -1)],
        name='account_recent_history'
    )

    db.Accounts.create_index(
        [('customer_id', 1)],
        name='customer_lookup'
    )


def getNextId(collection, session=None):
    """
    Computes the next integer _id for a collection by looking at the current max.

    ::param collection:: pymongo collection
    ::param session:: optional active session
    """
    latest = collection.find_one({}, sort=[('_id', -1)], projection={'_id': 1}, session=session)
    if latest and '_id' in latest:
        return latest['_id'] + 1
    return 1


def getNextTransferId(db, session=None):
    """
    Generates the next transfer_id value by inspecting existing transfer rows.

    ::param db:: active database object
    ::param session:: optional active session
    """
    latest = db.Transactions.find_one(
        {'transfer_id': {'$type': 'int'}},
        sort=[('transfer_id', -1)],
        projection={'transfer_id': 1},
        session=session
    )
    if latest and 'transfer_id' in latest:
        return latest['transfer_id'] + 1
    return 1


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

# -------------------------------------------------------------------------------------------------
# Banking Application Functions
# -------------------------------------------------------------------------------------------------


def openAccount(db, accountId=None, logOutput=True):
    """
    Opens a new account for a customer for a random customer from
    the Customers collection. Or reopens an existing account if accountId is provided.
    
    ::param db:: active database object
    ::param accountId:: optional account ID to reopen
    ::param logOutput:: print status messages when True
    """
    try:
        with db.client.start_session() as session:
            with session.start_transaction(read_concern=READ_CONCERN, write_concern=WRITE_CONCERN):
                if accountId:
                    account = db.Accounts.find_one({'_id': accountId, 'status': 'closed'}, session=session)
                    if account:
                        db.Accounts.update_one(
                            {'_id': accountId},
                            {'$set': {'status': 'open', 'update_date': datetime.now(UTC)}},
                            session=session
                        )
                        if logOutput:
                            print(f"Account reopened successfully: {accountId}")
                        return True
                    else:
                        if logOutput:
                            print(f"No closed account found for account ID {accountId}.")
                        return False

                customer = db.Customers.aggregate([{'$sample': {'size': 1}}], session=session).next()
                newAccountId = getNextId(db.Accounts, session=session)
                accountInfo = {
                    '_id': newAccountId,
                    'customer_id': customer['_id'],
                    'balance': 0,
                    'overdraft_limit': 0,
                    'status': 'open',
                    'creation_date': datetime.now(UTC),
                    'update_date': datetime.now(UTC)
                }
                db.Accounts.insert_one(accountInfo, session=session)

        if logOutput:
            print(f"Account opened successfully: {accountInfo}")
        return True
    except Exception as e:
        if logOutput:
            print(f"Error opening account: {e}")
        return False


def deposit(db, accountId, amount, logOutput=True):
    """
    Deposits amount into a customer's account and creates a transaction record.

    ::param db:: active database object
    ::param accountId:: the ID of the account to deposit into
    ::param amount:: the amount to deposit
    ::param logOutput:: print status messages when True
    """
    try:
        if amount <= 0:
            if logOutput:
                print("Deposit amount must be positive.")
            return False

        with db.client.start_session() as session:
            with session.start_transaction(read_concern=READ_CONCERN, write_concern=WRITE_CONCERN):
                account = db.Accounts.find_one({'_id': accountId, 'status': 'open'}, session=session)
                if not account:
                    if logOutput:
                        print(f"No active account found for account ID {accountId}.")
                    return False

                db.Accounts.update_one({'_id': account['_id']}, {'$inc': {'balance': amount}}, session=session)

                newTransactionId = getNextId(db.Transactions, session=session)
                transactionInfo = {
                    '_id': newTransactionId,
                    'account_id': account['_id'],
                    'timestamp': datetime.now(UTC),
                    'amount': amount,
                    'type': 'deposit',
                    'transfer_id': None,
                    'channel': 'online',
                    'merchant_id': None,
                    'note': 'deposit'
                }
                db.Transactions.insert_one(transactionInfo, session=session)

        if logOutput:
            print(f"Deposit successful: \n{transactionInfo}")
        return True

    except Exception as e:
        if logOutput:
            print(f"Error depositing money: {e}")
        return False



def withdraw(db, accountId, amount, logOutput=True):
    """
    Withdraws amount from a customer's account, creating a transaction record.

    ::param db:: active database object
    ::param accountId:: the ID of the account to withdraw from
    ::param amount:: the amount to withdraw
    ::param logOutput:: print status messages when True
    """
    try:
        if amount <= 0:
            if logOutput:
                print("Withdrawal amount must be positive.")
            return False

        with db.client.start_session() as session:
            with session.start_transaction(read_concern=READ_CONCERN, write_concern=WRITE_CONCERN):
                account = db.Accounts.find_one({'_id': accountId, 'status': 'open'}, session=session)
                if not account:
                    if logOutput:
                        print(f"No active account found for account ID {accountId}.")
                    return False

                if account['balance'] < amount:
                    if logOutput:
                        print(f"Insufficient funds in account ID {accountId}.")
                    return False

                db.Accounts.update_one({'_id': account['_id']}, {'$inc': {'balance': -amount}}, session=session)

                newTransactionId = getNextId(db.Transactions, session=session)
                transactionInfo = {
                    '_id': newTransactionId,
                    'account_id': account['_id'],
                    'timestamp': datetime.now(UTC),
                    'amount': amount,
                    'type': 'withdraw',
                    'transfer_id': None,
                    'channel': 'online',
                    'merchant_id': None,
                    'note': 'withdraw'
                }
                db.Transactions.insert_one(transactionInfo, session=session)

        if logOutput:
            print(f"Withdrawal successful: \n{transactionInfo}")
        return True

    except Exception as e:
        if logOutput:
            print(f"Error withdrawing money: {e}")
        return False


def transfer(db, fromAccId, toAccId, amount, merchantId=None, note=None, channel='online', logOutput=True):
    """
    Transfers amount from one account to another account, creating paired transactions.
    Can optionally include merchant information for merchant-related transfers.
    
    For account-to-account transfers:
        -> One transfer_debit from Account A (negative amount)
        -> One transfer_credit to Account B (positive amount)
        -> Both share the same transfer_id
        -> Both can have merchant_id if payment is to/through a merchant

    ::param db:: active database object
    ::param fromAccId:: the ID of the account to transfer from
    ::param toAccId:: the ID of the account to transfer to
    ::param amount:: the amount to transfer (positive value)
    ::param merchantId:: optional merchant ID if this is a merchant payment
    ::param note:: optional note for the transaction (e.g., 'rent', 'groceries', 'refund')
    ::param channel:: transaction channel (default 'online', can be 'pos', 'atm', 'branch')
    ::param logOutput:: print status messages when True
    """
    try:
        if amount <= 0:
            if logOutput:
                print("Transfer amount must be positive.")
            return False

        with db.client.start_session() as session:
            with session.start_transaction(read_concern=READ_CONCERN, write_concern=WRITE_CONCERN):
                fromAcc = db.Accounts.find_one({'_id': fromAccId, 'status': 'open'}, session=session)
                toAcc = db.Accounts.find_one({'_id': toAccId, 'status': 'open'}, session=session)

                if not fromAcc:
                    if logOutput:
                        print(f"No active account found for account ID {fromAccId}.")
                    return False
                if not toAcc:
                    if logOutput:
                        print(f"No active account found for account ID {toAccId}.")
                    return False
                if fromAcc['balance'] < amount:
                    if logOutput:
                        print(f"Insufficient funds in account ID {fromAccId}.")
                    return False

                if merchantId:
                    merchant = db.Merchants.find_one({'_id': merchantId}, session=session)
                    if not merchant:
                        if logOutput:
                            print(f"No merchant found with ID {merchantId}.")
                        return False
                    if not note:
                        note = merchant['category']

                db.Accounts.update_one({'_id': fromAcc['_id']}, {'$inc': {'balance': -amount}}, session=session)
                db.Accounts.update_one({'_id': toAcc['_id']}, {'$inc': {'balance': amount}}, session=session)

                transferId = getNextTransferId(db, session=session)
                newTransactionId01 = getNextId(db.Transactions, session=session)

                transactionInfo01 = {
                    '_id': newTransactionId01,
                    'account_id': fromAcc['_id'],
                    'timestamp': datetime.now(UTC),
                    'amount': -amount,
                    'type': 'transfer_debit',
                    'transfer_id': transferId,
                    'channel': channel,
                    'merchant_id': merchantId,
                    'note': note
                }   

                db.Transactions.insert_one(transactionInfo01, session=session)

                newTransactionId02 = getNextId(db.Transactions, session=session)
                transactionInfo02 = {
                    '_id': newTransactionId02,
                    'account_id': toAcc['_id'],
                    'timestamp': datetime.now(UTC),
                    'amount': amount,
                    'type': 'transfer_credit',
                    'transfer_id': transferId,
                    'channel': channel,
                    'merchant_id': merchantId,
                    'note': note
                }
                db.Transactions.insert_one(transactionInfo02, session=session)

        if merchantId:
            if logOutput:
                print(f"Transfer successful (merchant payment): \n{transactionInfo01}\n{transactionInfo02}")
        else:
            if logOutput:
                print(f"Transfer successful: \n{transactionInfo01}\n{transactionInfo02}")
        return True

    except Exception as e:
        if logOutput:
            print(f"Error transferring money: {e}")
        return False


def getBalance(db, accountId, customerId, logOutput=True):
    """
    Retrieves the account balance for a given customer.

    ::param db:: active database object
    ::param accountId:: the _id of the account to retrieve the balance for
    ::param customerId:: the customer_id of the account owner
    ::param logOutput:: print status messages when True
    """
    try:
        account = db.Accounts.find_one({'_id': accountId, 'customer_id': customerId})
        if account:
            if logOutput:
                print(f"Account ID {accountId}'s Balance: {account['balance']}")
            return account['balance']
        else:
            if logOutput:
                print(f"No active account found for account ID {accountId} and customer ID {customerId}.")
            return None
    
    except Exception as e:
        if logOutput:
            print(f"Error retrieving balance: {e}")
        return None




def viewRecentTransactions(db, accountId, logOutput=True):
    """
    Retrieves recent transactions for a given account.

    ::param db:: active database object
    ::param accountId:: the _id of the account to retrieve transactions for
    ::param logOutput:: print status messages when True
    """
    try:
        transactions = list(db.Transactions.find({'account_id': accountId}).sort('timestamp', -1).limit(10))
        
        if not transactions:
            if logOutput:
                print(f"\n=== No transactions found for Account ID {accountId} ===\n")
            return []

        if logOutput:
            print(f"\n{'-'*110}")
            print(f"Recent Transactions for Account ID {accountId}")
            print(f"{'-'*110}")
            print(f"{'ID':<12} {'Amount':<12} {'Type':<18} {'Timestamp':<27} {'Note':<20} {'Channel':<10}")
            print(f"{'-'*110}")
        
            for txn in transactions:
                transferId = txn['_id']
                amount = f"${txn['amount']/100:,.2f}" if txn.get('amount') is not None else '$0.00'
                timestamp = txn['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if txn.get('timestamp') else 'N/A'
                type = txn.get('type') or 'N/A'
                note = str(txn.get('note')) if txn.get('note') is not None else 'N/A'
                channel = txn.get('channel') or 'N/A'

                print(f"{transferId:<12} {amount:<12} {type:<18} {timestamp:<27} {note:<20} {channel:<10}")

            print(f"{'-'*110}\n")

        return transactions
    except Exception as e:
        if logOutput:
            print(f"Error retrieving recent transactions: {e}")
            import traceback
            traceback.print_exc()
        return []





def closeAccount(db, accountId, customerId):
    """
    Closes a specific account for given user.

    ::param db:: active database object
    ::param accountId:: the _id of the account to close
    ::param customerId:: the customer_id of the account owner
    """
    try:
        result = db.Accounts.update_one({'_id': accountId, 'customer_id': customerId}, {'$set': {'status': 'closed'}})
        if result.modified_count > 0:
            print(f"Account {accountId} closed successfully.")
        else:
            print(f"Account {accountId} not found or already closed.")
    except Exception as e:
        print(f"Error closing account {accountId}: {e}")




# -------------------------------------------------------------------------------------------------
# Tester / Utility Functions
# -------------------------------------------------------------------------------------------------


def deleteAccount(db, accountId):
    """
    Deletes a specific account by account ID
    
    ::param db:: active database object
    ::param accountId:: the _id of the account to delete
    ::return:: True if deleted, False otherwise
    """
    try:
        result = db.Accounts.delete_one({'_id': accountId})
        if result.deleted_count > 0:
            print(f"Account {accountId} deleted successfully.")
            return True
        else:
            print(f"Account {accountId} not found.")
            return False
    except Exception as e:
        print(f"Error deleting account {accountId}: {e}")
        return False


def deleteNewAccounts(db, startingId):
    """
    Deletes all accounts with ID greater than or equal to startingId.
    Useful for removing test accounts you've added.
    
    ::param db:: active database object
    ::param startingId:: delete all accounts with _id >= this value
    ::return:: number of accounts deleted
    """
    try:
        result = db.Accounts.delete_many({'_id': {'$gte': startingId}})
        print(f"Deleted {result.deleted_count} account(s) with ID >= {startingId}.")
        return result.deleted_count
    except Exception as e:
        print(f"Error deleting accounts: {e}")
        return 0


def listRecentAccounts(db, limit=10):
    """
    Lists the most recently created accounts (by _id)
    
    ::param db:: active database object
    ::param limit:: number of accounts to show
    """
    try:
        accounts = list(db.Accounts.find().sort('_id', -1).limit(limit))
        print(f"\n=== Last {limit} Accounts ===")
        for acc in accounts:
            print(f"ID: {acc['_id']}, Customer: {acc['customer_id']}, "
                  f"Balance: {acc['balance']}, Status: {acc['status']}, "
                  f"Created: {acc['creation_date']}")
        print("=========================\n")
        return accounts
    except Exception as e:
        print(f"Error listing accounts: {e}")
        return []


def verifyData(db):
    """
    Prints statistics and sample data from all collections to verify data loading
    
    ::param db:: active database object
    """
    print("\n=== DATABASE VERIFICATION ===\n")
    
    # Accounts
    account_count = db.Accounts.count_documents({})
    print(f"Total Accounts: {account_count}")
    if account_count > 0:
        sample_account = db.Accounts.find_one()
        print(f"Sample Account: {sample_account}\n")
    
    # Transactions
    transaction_count = db.Transactions.count_documents({})
    print(f"Total Transactions: {transaction_count}")
    if transaction_count > 0:
        sample_transaction = db.Transactions.find_one()
        print(f"Sample Transaction: {sample_transaction}\n")
    

    # Customers
    customer_count = db.Customers.count_documents({})
    print(f"Total Customers: {customer_count}")
    if customer_count > 0:
        sample_customer = db.Customers.find_one()
        print(f"Sample Customer: {sample_customer}\n")
    
    # Merchants
    merchant_count = db.Merchants.count_documents({})
    print(f"Total Merchants: {merchant_count}")
    if merchant_count > 0:
        sample_merchant = db.Merchants.find_one()
        print(f"Sample Merchant: {sample_merchant}\n")
    
    print("=== END VERIFICATION ===\n")



# def main():
    # db, client = connectToMongoDB()

    # # Setup (uncomment to reset and reload data)
    # dropBankingCollections(db)
    # createBankingCollections(db)
    # loadSampleData(db, 'baseline')


    # # Applications
    # openAccount(db)
    # deposit(db, accountId=10000000, amount=100)
    # withdraw(db, accountId=10000003, amount=11000)
    
    # # non-merchant
    # transfer(db, fromAccId=10000006, toAccId=10000003, amount=5000)
    # # merchant
    # transfer(db, fromAccId=10000007, toAccId=10000008, amount=5000, merchantId=1003, note='rent', channel='online')
    

    # getBalance(db, accountId=10000003, customerId=1016942)
    # getBalance(db, accountId=10000007, customerId=1007807)
    # getBalance(db, accountId=10000001, customerId=1003433)
    # getBalance(db, accountId=10000002, customerId=1027482)


    # viewRecentTransactions(db, accountId=10000003)
    # viewRecentTransactions(db, accountId=10000007)
    # viewRecentTransactions(db, accountId=10000001)
    # viewRecentTransactions(db, accountId=10000002)


    # closeAccount(db, accountId=10000003, customerId=1016942)
    # openAccount(db, accountId=10000003)


    # # View recently added accounts
    # listRecentAccounts(db, limit=5)
    
    # # Verify data was loaded successfully
    # verifyData(db)
    
    # # Run query examples
    # # queryExamples(db)

    # # Delete a specific account by ID
    # deleteAccount(db, 10001)
    
    # # Delete all accounts with ID >= 10001 (all newly added test accounts)
    # deleteNewAccounts(db, 10001)

    # client.close()

# if __name__ == "__main__":
#     main()