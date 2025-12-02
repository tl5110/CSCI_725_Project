import time
import pytest
import pandas as pd
import psycopg as psql
from datetime import datetime, UTC
import psycopg as psql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
import matplotlib.pyplot as plt
import pandas as pd
from psycopg.errors import ForeignKeyViolation



def connectToDB():
    """
    Establishes a connection to the PostgreSQL database

    ::return:: active database connection object if successful, None otherwise
    """
    try:
        conn = psql.connect(
            dbname = "CSCI_725_Project",
            host = "127.0.0.1",
            user="root", 
            password="MYsql990001161",
        )
        print("connection success!!!!!")
        return conn
    except psql.Error:
        conn = None
        return conn



# -------------------------
# Read Data + Load Data
# -------------------------
def verifyAccountExists(cur, account_id):
    cur.execute("SELECT account_id FROM accounts WHERE account_id = %s", (account_id,))
    return cur.fetchone() is not None 

def verifyExist(conn,customer_id):
    try: 
        with conn.cursor() as cur:
            balance_check = "SELECT account_id FROM accounts WHERE customer_id = %s"
            cur.execute(balance_check,(customer_id,)) 
            customer = cur.fetchone()
            if customer  != None: 
                return customer[0]
            else: 
                return False
    except Exception as e:
        print(f"Error Getting balance {customer_id}: {e}")

def loadData(conn):


    customer_files = {
        "banking_datasets/baseline/baseline_customers.csv",
        "banking_datasets/edgecases/edgecases_customers.csv",
        "banking_datasets/hotspot/hotspot_customers.csv",
        "banking_datasets/payday/payday_customers.csv",
    }

    with conn.cursor() as cur:
        for file in customer_files: 
            read = pd.read_csv(file,skiprows=1,usecols=[0,1,2,3,4])
            for row in read.itertuples(index=False): 
                customer_id = row[0]
                name = row[1]
                email = row[2]
                phone_number = row[3]
                creation = row[4]

                sql = """
                        INSERT INTO customers (customer_id,name, email, phone_number, creation_date )
                        VALUES (%s, %s, %s,%s,%s)
                        ON CONFLICT (customer_id) DO NOTHING;
                    """
                cur.execute(sql, (customer_id,name,email,phone_number,creation))
        
        conn.commit()
        print("Customer data loaded.")


    merchant_files = {
        "banking_datasets/baseline/baseline_merchants.csv",
        "banking_datasets/edgecases/edgecases_merchants.csv",
        "banking_datasets/hotspot/hotspot_merchants.csv",
        "banking_datasets/payday/payday_merchants.csv",
    }

    with conn.cursor() as cur:
        for file in merchant_files: 
            read = pd.read_csv(file,skiprows=1,usecols=[0,1,2])
            for row in read.itertuples(index=False): 
                merchant_id = row[0]
                name = row[1]
                category = row[2]

                sql = """
                        INSERT INTO merchants (merchant_id, name, category)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (merchant_id) DO NOTHING;
                    """
                cur.execute(sql, (merchant_id, name, category))
        
        conn.commit()
        print("Merchant data loaded.")


    account_files = {
        "banking_datasets/baseline/baseline_accounts.csv",
        "banking_datasets/edgecases/edgecases_accounts.csv",
        "banking_datasets/hotspot/hotspot_accounts.csv",
        "banking_datasets/payday/payday_accounts.csv",
    }

    with conn.cursor() as cur:
        for file in account_files: 
            read = pd.read_csv(file,skiprows=1,usecols=[0,1,2,3,4,5,6])

            for row in read.itertuples(index=False): 
                account_id = row[0]
                customer_id= row[1]
                balance = row[2]
                overdraft = row[3]
                status = row[4]
                open_at = row[5]
                update = row[6]

                if verifyExist(conn, customer_id) ==  False: 
                    #print("customer doesnt exist")
                    continue
                try: 
                    sql = """
                            INSERT INTO accounts (account_id, customer_id,balance, overdraft_limit, status,creation_date, update_date)
                            VALUES (%s, %s, %s,%s,%s,%s,%s)
                        """
                    cur.execute(sql, (account_id,customer_id,balance,overdraft,status, open_at,update))
                except Exception as e:
                    conn.rollback()
                    print("Account load error:", e)
        conn.commit()
        print("Account data loaded.")

    transaction_files = {
        "banking_datasets/baseline/baseline_transactions.csv",
        "banking_datasets/edgecases/edgecases_transactions.csv",
        "banking_datasets/hotspot/hotspot_transactions.csv",
        "banking_datasets/payday/payday_transactions.csv",
    }

    with conn.cursor() as cur:
        for file in transaction_files:
            df = pd.read_csv(file, skiprows=1, usecols=range(9))


            for row in df.itertuples(index=False):
                txn_id      = row[0]
                account_id  = row[1]
                ts          = row[2]
                amount      = row[3]
                type        = row[4]
                transfer_id = None if pd.isna(row[5]) else int(row[5])
                channel     = None if pd.isna(row[6]) else row[6]
                merchant_id = None if pd.isna(row[7]) else int(row[7])
                note        = None if pd.isna(row[8]) else row[8]

                if not verifyAccountExists(cur, account_id):
                    #print(f"Skipping txn {txn_id}: account_id {account_id} does not exist.")
                    continue

                try:
                    cur.execute("""
                        INSERT INTO transactions
                        (txn_id, account_id, timestamp, amount, type, transfer_id, channel, merchant_id, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (txn_id) DO NOTHING;
                    """,
                    (txn_id, account_id, ts, amount, type, transfer_id, channel, merchant_id, note))

                except Exception as e:
                    conn.rollback()
                    print(f"Skipping txn {txn_id}: {e}")
                    continue

        conn.commit()
        print("Transaction data loaded.")

     
    
def createTables(conn):

    with conn.cursor() as cur:


        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id     INTEGER PRIMARY KEY,
                name            VARCHAR,
                email           VARCHAR,
                phone_number    VARCHAR,
                creation_date   VARCHAR
            );
        """)
        
    
        cur.execute("""
            CREATE TABLE IF NOT EXISTS merchants (
                merchant_id INTEGER PRIMARY KEY,
                name        VARCHAR,
                category    VARCHAR
            );
        """)


        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id      INT PRIMARY KEY,
                customer_id     INT NOT NULL REFERENCES customers(customer_id),
                balance         INT NOT NULL,
                overdraft_limit INT DEFAULT 0,
                status          VARCHAR NOT NULL,
                creation_date   TIMESTAMP,
                update_date     TIMESTAMP
            );
        """)

        #BIGINT BECAUSE LOADING FROM PRE-EXITING ID'S
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                txn_id        BIGINT PRIMARY KEY,     
                account_id    INT REFERENCES accounts(account_id),
                timestamp     TIMESTAMP NOT NULL DEFAULT NOW(),
                amount        INT NOT NULL,          
                type          VARCHAR(30) NOT NULL
                            CHECK (type IN ('deposit','withdrawal','transfer_debit','transfer_credit')),
                transfer_id   BIGINT,   
                channel       VARCHAR(50),
                merchant_id   INT REFERENCES merchants(merchant_id),
                note          TEXT  
            );

        """)

    conn.commit()
    print("Tables created successfully.")




def dropTables(conn):
    """
    Drops all tables for the banking application in the database
    """
    with conn.cursor() as cur:
        # Drop tables that depend on others first
        tables = [
            "transactions",
            "accounts",
            "merchants",
            "customers"
        ]

        for table in tables:
            sql = f"DROP TABLE IF EXISTS {table} CASCADE;" #Cascade drops depend. 
            cur.execute(sql)    

    conn.commit()
    print("All tables dropped")

def reset_transaction_sequence(conn):
    """
    Aligns the internal sequence with the max txn_id from CSV + inserted ops.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT setval(
                pg_get_serial_sequence('transactions', 'txn_id'),
                COALESCE((SELECT MAX(txn_id) FROM transactions), 1)
            );
        """)
    conn.commit()
    print("Transaction sequence reset to max(txn_id).")



# -------------------------
# Bank Functions
# -------------------------



def OpenAccount(conn, customer_id):
    """
    create a new account for a customer with status open and balance 0.
    """
    try:
        with conn.cursor() as cur:

                #Reopen account 
            if(verifyExist(conn,customer_id) != False): 
                reopen_account = "UPDATE accounts SET  status = %s WHERE customer_id = %s"
                cur.execute(reopen_account,('open',customer_id,)) 
                conn.commit()
                return True
            else: 
                #New account 

                #create the new account id 
                size = """
                SELECT account_id
                FROM accounts
                ORDER BY account_id DESC
                LIMIT 1;
                """
                col_size =  cur.execute(size,())
                col_size = cur.fetchone()
                new_id = col_size[0] + 1
                
                open_account = "INSERT INTO accounts(account_id,customer_id,balance, overdraft_limit,status, creation_date,update_date) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                open_date = datetime.now(UTC)
                values = (new_id,customer_id,0,0,'open',open_date,open_date)
                cur.execute(open_account,values)
                conn.commit()
                return True

    except Exception as e:
        print(f"Error opening account {customer_id}: {e}")
        conn.rollback()



   
 
def Deposit(conn,accountId, amount):
    """
    add money to an account. Append a transaction row and update the account balance inside one unit of work.
    """
    try: 
        new_id = 0
        with conn.cursor() as cur:



            #ADD MONEY TO ACCOUNT 
            update_account = "UPDATE accounts SET  balance = balance + %s WHERE account_id = %s"
            cur.execute(update_account,(amount,accountId))             
            #INSERT TRANSACTION ROW
            size = """
                SELECT txn_id
                FROM transactions
                ORDER BY txn_id DESC
                LIMIT 1;
                """
            col_size =  cur.execute(size,())
            col_size = cur.fetchone()
            new_id = col_size[0] + 1
            date = datetime.now(UTC)

            new_transfer = "INSERT INTO transactions(txn_id, account_id,timestamp, amount,type, transfer_id,channel,merchant_id,note) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values =(new_id,accountId,date,amount,'deposit',None,'online',None,None)
            cur.execute(new_transfer,values)
            
        conn.commit()
        return new_id

    except Exception as e:
        print(f"Error depositing balance {accountId}: {e}")
        conn.rollback()






def Withdraw(conn,accountId, amount):
    """
    take money out if funds plus overdraft allow it. Append a transaction row and update the balance in one unit of work.
    """
    try: 
        with conn.cursor() as cur:
            #CHECK THAT DOESNT GO BELOW OVERDRAFT + FUNDS
            currBalance = getBalance(conn, accountId)
            over = "SELECT overdraft_limit FROM accounts WHERE account_id = %s"
            cur.execute(over,(accountId,)) 
            limit = cur.fetchone()

            if(  (limit[0] + currBalance) < amount):
                print(f"Account {accountId} does not have suff funds")
                return False

            # MONEY TO ACCOUNT 
            update_account = "UPDATE accounts SET  balance = balance - %s WHERE account_id = %s"
            cur.execute(update_account,(amount,accountId)) 

            #INSERT TRANSACTION ROW
            size = """
                SELECT txn_id
                FROM transactions
                ORDER BY txn_id DESC
                LIMIT 1;
                """
            col_size =  cur.execute(size,())
            col_size = cur.fetchone()
            new_id = col_size[0] + 1
            date = datetime.now(UTC)

            new_transfer = """
                INSERT INTO transactions(account_id, timestamp, amount, type, transfer_id, channel, merchant_id, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING txn_id
            """
            values = (accountId, date, amount, 'withdrawal', None, 'online', None, None)
            cur.execute(new_transfer, values)

        conn.commit()
    except Exception as e:
        print(f"Error withdrawing balance {accountId}: {e}")
        conn.rollback()




def Transfer(conn, fromAccId, toAccId=None, amount=0, merchantId=None, note=None, channel='online'):
    """
    Move money between two accounts or to a merchant.
    Creates two transaction rows (debit + credit) with same transfer_id.
    Updates balances atomically.
    """
    try:
        with conn.cursor() as cur:
            # Lock sender row
            cur.execute("SELECT balance, overdraft_limit FROM accounts WHERE account_id = %s FOR UPDATE", (fromAccId,))
            sender = cur.fetchone()
            if not sender:
                print(f"Sender account {fromAccId} does not exist.")
                return False

            balance, overdraft = sender
            if balance + overdraft < amount:
                print(f"Insufficient funds in account {fromAccId}.")
                return False

            # Determine recipient type
            if merchantId:
                cur.execute("SELECT merchant_id FROM merchants WHERE merchant_id = %s", (merchantId,))
                if not cur.fetchone():
                    print(f"Merchant {merchantId} does not exist.")
                    return False
                recipient_type = 'merchant'
            else:
                cur.execute("SELECT account_id FROM accounts WHERE account_id = %s", (toAccId,))
                if not cur.fetchone():
                    print(f"Receiver account {toAccId} does not exist.")
                    return False
                recipient_type = 'account'

            # Generate transfer_id (unique)
            cur.execute("SELECT COALESCE(MAX(transfer_id),0) + 1 FROM transactions")
            transfer_id = cur.fetchone()[0]

            timestamp = datetime.now(UTC)

            # Debit transaction
            cur.execute("""
                INSERT INTO transactions 
                    (account_id, timestamp, amount, type, transfer_id, channel, merchant_id, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING txn_id
            """, (fromAccId, timestamp, -amount, 'transfer_debit', transfer_id, channel, None, note))
            debit_txn = cur.fetchone()[0]

            # Credit transaction
            credit_account_id = toAccId if recipient_type == 'account' else None
            credit_merchant_id = merchantId if recipient_type == 'merchant' else None
            cur.execute("""
                INSERT INTO transactions 
                    (account_id, timestamp, amount, type, transfer_id, channel, merchant_id, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING txn_id
            """, (credit_account_id, timestamp, amount, 'transfer_credit', transfer_id, channel, credit_merchant_id, note))
            credit_txn = cur.fetchone()[0]

            # Update balances
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE account_id = %s", (amount, fromAccId))
            if recipient_type == 'account':
                cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id = %s", (amount, toAccId))

        conn.commit()
        print(f"Transfer {transfer_id} completed successfully.")
        return transfer_id
    except Exception as e:
        print(f"Transfer failed: {e}")
        conn.rollback()
 



#Verified small test 
def getBalance(conn, accountId):
    """
    read the current account balance.
    ::param db:: active database object
    ::param accountId:: the _id of the account to retrieve the balance for
    """
    try: 
        with conn.cursor() as cur:
            person = "SELECT customer_id FROM accounts WHERE account_id = %s"
            cur.execute(person,(accountId,)) 
            per_exist = cur.fetchone()[0]
            check = verifyExist(conn,per_exist)
            if(not check):
                print("doesnt exit")
                return False


            balance_check = "SELECT balance FROM accounts WHERE account_id = %s"
            cur.execute(balance_check,(accountId,)) 
            balance = cur.fetchone()
            return balance[0]
    except Exception as e:
        print(f"Error Getting balance {accountId}: {e}")




def viewRecentTransactions(conn, accountId):
    """
    read the last N transactions for an account, ordered by timestamp descending.
    """
    try: 
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM transactions WHERE account_id = %s ORDER BY txn_id DESC", (accountId,))
            transact = cur.fetchall()
            return transact     
    except Exception as e:
        print(f"Error Getting balance {accountId}: {e}")




#Verified small test 
def closeAccount(conn, account_id, status):
    """
    change status to closed or frozen. Do not delete rows. You want an audit trail.
    """
    try:
        with conn.cursor() as cur:
            update_account = "UPDATE accounts SET  status = %s WHERE account_id = %s"
            cur.execute(update_account,(status,account_id,)) 
            conn.commit()
        
    except Exception as e:
        print(f"Error closing account {account_id}: {e}")
        conn.rollback()

# -------------------------
# TESTING - Correct Action
# -------------------------
def setup_test_customer(conn, TEST_CUSTOMER_ID, ACC_ID):
    """
    Create a single fixed test customer and account.
    Ensures tests never collide with existing data.
    """

    with conn.cursor() as cur:
        # create customer if not exists
        cur.execute("""
            INSERT INTO customers (customer_id, name, email, phone_number, creation_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            TEST_CUSTOMER_ID,
            "Alex Test",
            "alex.anderson120@example.com",
            "+1-555-120-1008",
            "2025-01-02T07:22:56Z"
        ))

        # create account if not exists
        cur.execute("""
            INSERT INTO accounts (account_id, customer_id, balance, overdraft_limit, status, creation_date,update_date)
            VALUES (%s, %s,0, 500,'open',NOW(),NOW())
        """, (ACC_ID,TEST_CUSTOMER_ID,))
        return TEST_CUSTOMER_ID,ACC_ID

    conn.commit()




def test_open_account(conn, TEST_CUSTOMER_ID):
    """
    Test 1: 
    """
    OpenAccount(conn, customer_id=TEST_CUSTOMER_ID)

    cur = conn.cursor()
    cur.execute("SELECT * FROM accounts WHERE customer_id = %s",(TEST_CUSTOMER_ID,))
    acc = cur.fetchone()

    assert acc is not None
    assert acc[2] == 0
    assert acc[4] == 'open'
    print("Test Passed - open account")


def test_close_account(conn,TEST_CUSTOMER_ID):
    """
    Test 2: 
    """
    OpenAccount(conn, TEST_CUSTOMER_ID)
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM accounts WHERE customer_id=%s",(TEST_CUSTOMER_ID,))
    acc = cur.fetchone()[0]
    closeAccount(conn, acc, "closed")

    cur.execute("SELECT status FROM accounts WHERE account_id=%s", (acc,))
    assert cur.fetchone()[0] == "closed"
    print("Test Passed -close account")

def test_deposit(conn,TEST_CUSTOMER_ID,amount):
    """
    Test 3: 
    """

    cur = conn.cursor()
    cur.execute("SELECT account_id FROM accounts WHERE customer_id = %s", (TEST_CUSTOMER_ID,))
    acc_id = cur.fetchone()[0]


    txn = Deposit(conn, acc_id, amount)
    
    cur.execute("SELECT * FROM transactions WHERE txn_id=%s", (txn,))
    tx = cur.fetchone()
    assert tx[3] == amount
    assert tx[4] == 'deposit'
    print("Test Passed - Get Balance")
    print("Test Passed - deposit")

def test_recent_transactions(conn):
    """
    Test 8: 
    """
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM accounts WHERE customer_id=1005010")
    acc = cur.fetchone()[0]

    Deposit(conn, acc, 6000)
    Withdraw(conn, acc, 3000)
    Deposit(conn, acc, 1500)

    txs = viewRecentTransactions(conn, acc)

    assert len(txs) >= 3
    assert txs[0][2] >= txs[-1][2]
    
    print("Test Passed - recent_transactions")



def test_withdraw_with_overdraft(conn,accId):
    cur = conn.cursor()

    Withdraw(conn, accId, 200)

    # Assert: 55676 - 58000 = -2324
    assert getBalance(conn, accId) == -200
    print("Test Passed - withdraw with overdraft")


def test_withdraw_insufficient(conn,customerId):
    """
    Test 5: 
    """
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM accounts WHERE customer_id = %s", (customerId,))
    row = cur.fetchone()
   
    acc_id = row[0]

    # starting balance  = zero
    cur.execute("SELECT balance FROM accounts WHERE account_id = %s", (acc_id,))

    # Expect failure
    test =  Withdraw(conn, acc_id, 100000)
    assert test == False
    print("Test Passed - withdraw with insufficient funds")

def test_transfer_between_accounts(conn, customer_id,reciver):
    """
    Test transferring funds between two regular accounts.
    """


    cur = conn.cursor()
    cur.execute("SELECT account_id FROM accounts WHERE customer_id =%s", (customer_id, ))
    acc_from = cur.fetchone()[0]

    cur.execute("SELECT account_id FROM accounts WHERE customer_id = %s", (reciver,))
    acc_to = cur.fetchone()[0]

    # Deposit initial funds
    Deposit(conn, acc_from, 50000)  # 500.00 dollars in cents

    # Act: Transfer $120.84 → 12084 cents
    Transfer(conn, fromAccId=acc_from, toAccId=acc_to, amount=12084, channel="atm")

    # Assert balances
    assert getBalance(conn, acc_from) == 50000 - 12084
    assert getBalance(conn, acc_to) == 12084

    # Assert transaction records
    cur.execute("SELECT * FROM transactions WHERE transfer_id IS NOT NULL AND account_id IN (%s, %s)", (acc_from, acc_to))
    transfer_rows = cur.fetchall()
    assert len(transfer_rows) == 2  # debit + credit
    debit_tx = next(tx for tx in transfer_rows if tx[4] == "transfer_debit")
    credit_tx = next(tx for tx in transfer_rows if tx[4] == "transfer_credit")
    assert debit_tx[3] == -12084
    assert credit_tx[3] == 12084
    print("Test Passed - transfer btwn accounts")


def test_transfer_to_active_merchant(conn):
    """
    Test transferring funds from an account to a merchant.
    """
    # create merchant
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO merchants(merchant_id, name, category)
        VALUES (1004, 'retail_merchant_1004', 'retail')
    """)
    conn.commit()

    # create customer account and deposit
    OpenAccount(conn, 1002)
    cur.execute("SELECT account_id FROM accounts WHERE customer_id=1002")
    acc_id = cur.fetchone()["account_id"]

    Deposit(conn, acc_id, 30000)  # $300.00

    # transfer $24.08 → 2408 cents to merchant 1004
    Transfer(conn, fromAccId=acc_id, amount=2408, merchantId=1004, channel="online")

    # account balance
    assert getBalance(conn, acc_id) == 30000 - 2408

    #  transaction record
    cur.execute("SELECT * FROM transactions WHERE merchant_id=1004")
    merchant_tx = cur.fetchone()
    assert merchant_tx is not None
    assert merchant_tx["amount"] == 2408
    assert merchant_tx["type"] == "transfer_credit"

    # sender debit transaction exists
    cur.execute("SELECT * FROM transactions WHERE account_id=%s AND type='transfer_debit'", (acc_id,))
    debit_tx = cur.fetchone()
    assert debit_tx is not None
    assert debit_tx["amount"] == -2408
    print("Test Passed - transfer btwn active merchant")





# -------------------------
# Main
# -------------------------
def main():

    conn = connectToDB()
    
    #Testing connection 
    if conn is not None: 
        
        #Create + Initialize db 
        print("Dropping Tables ...")
        dropTables(conn)
        print("Creating Tables ...")
        createTables(conn)
        print("Loading Data ...")
        loadData(conn) 


        # TEST Bank Operaations - USER: 1005010 #
        # testCustomer,accId = setup_test_customer(conn,20,200000001)
        # test_open_account(conn,testCustomer)
        # test_close_account(conn,testCustomer)
        # test_open_account(conn,testCustomer) #reopen account
        # test_withdraw_with_overdraft(conn,accId)
        # test_withdraw_insufficient(conn,testCustomer)
        # test_deposit(conn, testCustomer,1) #Tests deposit and get balance
        # test_recent_transactions(conn)
        # testCustomer,accId = setup_test_customer(conn,21,210000001)
        # testCustomer02,accId02 = setup_test_customer(conn,22,220000001)
        # test_transfer_between_accounts(conn,testCustomer,testCustomer02)
        # test_transfer_to_active_merchant(conn)

        # TEST PERFORMANCE #
        # reset_transaction_sequence(conn) #CALL/UNCOMMENT AFTER LOADING DATA

        conn.close()    
    else:
        print("Failed to connect to the database.")
    
main()



    