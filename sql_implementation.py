
import pandas as pd
from datetime import datetime, UTC
import psycopg as psql
import time
from concurrent.futures import ThreadPoolExecutor


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



############## READ IN DATA ##############
def loadData(conn):
    """
    Read in the data
    """
   
    #CUSTOMER file
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
                        INSERT INTO customer (customer_id,name, email, phone_number, creation_date )
                        VALUES (%s, %s, %s,%s,%s)
                        ON CONFLICT (customer_id) DO NOTHING;
                    """
                cur.execute(sql, (customer_id,name,email,phone_number,creation))
        
        conn.commit()
        print("customer data loaded")




    #MERCHANT - read in the merchant files and load them into the db 
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
                        INSERT INTO merchant (merchant_id, name, category)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (merchant_id) DO NOTHING;
                    """
                cur.execute(sql, (merchant_id, name, category))
        
        conn.commit()
        print("Merchant data loaded")


    # #Account files 
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
                try: 
                    sql = """
                            INSERT INTO account (account_id, customer_id, open_date,status,balance, last_update,overdraft_limit)
                            VALUES (%s, %s, %s,%s,%s,%s,%s)
                            ON CONFLICT (account_id) DO NOTHING
                            N CONFLICT (customer_id) DO NOTHING
                        """
                    cur.execute(sql, (account_id,customer_id,open_at,status,balance,update,overdraft))
                except Exception as e:
                    print(f"Skipping account {account_id}: {e}")
        conn.commit()
        print("account data loaded")



    # transaction files -> minor issue - 10001 not present 

    transaction_files = {
        "banking_datasets/baseline/baseline_transactions.csv",
        "banking_datasets/edgecases/edgecases_transactions.csv",
        "banking_datasets/hotspot/hotspot_transactions.csv",
        "banking_datasets/payday/payday_transactions.csv",
    }

    with conn.cursor() as cur:
        for file in transaction_files: 
            read = pd.read_csv(file,skiprows=1,usecols=[0,1,2,3,4,5,6,7,8])
            for row in read.itertuples(index=False): 
                transaction_id = row[0]
                account_id = row[1]
                time_stamp = row[2]
                amount = row[3]
                type = row[4]
                transfer_id = int(row[5]) if not pd.isna(row[5]) else None
                channel = row[6] if not pd.isna(row[6]) else None
                merchant_id = int(row[7]) if not pd.isna(row[7]) else None
                note = row[8] if not pd.isna(row[8]) else None
                try: 
                    conn.autocommit = True
                    sql = """
                                INSERT INTO transactions (transaction_id, account_id, merchant_id,type, time_stamp, amount,channel, note, transfer_id)
                                VALUES (%s, %s, %s,%s,%s,%s,%s, %s, %s)
                                ON CONFLICT (transaction_id) DO NOTHING;
                            """
                    cur.execute(sql, (transaction_id, account_id, merchant_id,type, time_stamp, amount,channel, note, transfer_id))
                except Exception as e:
                    print(f"Skipping account {account_id}: {e}")
                    
            

        conn.commit()
        print("transaction data loaded")



        
    

def createTables(conn): 
    """
    Creates collections for banking application in the database
    """
    customer_table_sql = """
                create table customer
                (
                    customer_id   integer not null
                        primary key,
                    name          varchar,
                    email         varchar,
                    phone_number  varchar,
                    creation_date varchar
                );
            """
    with conn.cursor() as cur:
        cur.execute(customer_table_sql)
        conn.commit()
        print("CUSTOMER table created successfully!")



    # #Create the MERCHANT Table 

    merchant_table_sql = """
                create table merchant
                (
                    merchant_id integer not null
                        primary key,
                    name        varchar,
                    category    varchar
                );
            """
    with conn.cursor() as cur:
        cur.execute(merchant_table_sql)
        conn.commit()
        print("MERCHANT table created successfully!")

    account_table_sql = """
            CREATE table account (
                account_id      INTEGER PRIMARY KEY,
                customer_id     INTEGER REFERENCES customer(customer_id),
                open_date       VARCHAR,
                status          VARCHAR,
                balance         INTEGER,
                last_update     VARCHAR,
                overdraft_limit INTEGER
            );
            """
    
    with conn.cursor() as cur:
        cur.execute(account_table_sql)
        conn.commit()
        print("accounts table created successfully!")



    # #Create the Transaction Table
    sql = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id BIGSERIAL PRIMARY KEY,
        account_id     BIGINT REFERENCES account(account_id),
        merchant_id    INTEGER REFERENCES merchant(merchant_id),
        type           VARCHAR(30) NOT NULL
                    CHECK (type IN ('deposit', 'withdrawal', 'transfer_debit', 'transfer_credit')),
        time_stamp     TIMESTAMP NOT NULL DEFAULT NOW(),
        amount         BIGINT NOT NULL CHECK (amount >= 0),
        channel        VARCHAR(50),
        note           TEXT,
        transfer_id    BIGINT,
        
        CONSTRAINT transfer_id_type_unique UNIQUE (transfer_id, type)
    );

    CREATE INDEX IF NOT EXISTS idx_transactions_account_time
        ON transactions (account_id, time_stamp DESC);
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
        print("transactions table created successfully!")





def dropTables(conn):
    """
    Drops all tables for the banking application in the database
    """
    with conn.cursor() as cur:
        # Drop tables that depend on others first
        tables = [
            "transactions",
            "account",
            "merchant",
            "customer"
        ]

        for table in tables:
            sql = f"DROP TABLE IF EXISTS {table} CASCADE;" #Cascade drops depend. 
            cur.execute(sql)
            

    conn.commit()
    print("All tables dropped")



############## BANK FUNCTIONS ############## 

#Verified small test 
def verifyExist(conn,customer_id):
    """
    verify if there already exists a bank account with this customer
    """
    try: 
        with conn.cursor() as cur:
            balance_check = "SELECT account_id FROM account WHERE customer_id = %s"
            cur.execute(balance_check,(customer_id,)) 
            customer = cur.fetchone()
            if customer  != None: 
                return customer[0]
            else: 
                return False
    except Exception as e:
        print(f"Error Getting balance {customer_id}: {e}")



def OpenAccount(conn, customer_id):
    """
    create a new account for a customer with status open and balance 0.
    """
    try:
        with conn.cursor() as cur:

                #Reopen account 
            if(verifyExist(conn,customer_id) != False): 
                reopen_account = "UPDATE account SET  status = %s WHERE customer_id = %s"
                cur.execute(reopen_account,('open',customer_id,)) 
                conn.commit()
            else: 
                #New account 

                #create the new account id 
                size = """
                SELECT account_id
                FROM account
                ORDER BY account_id DESC
                LIMIT 1;
                """
                col_size =  cur.execute(size,())
                col_size = cur.fetchone()
                new_id = col_size[0] + 1
                
                open_account = "INSERT INTO account(account_id,customer_id,open_date,status,balance,last_update,overdraft_limit) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                open_date = datetime.now(UTC)
                values = (new_id,customer_id,open_date,'open',0,open_date,0)
                cur.execute(open_account,values)


    except Exception as e:
        print(f"Error opening account {customer_id}: {e}")



   
 
def Deposit(conn,accountId, amount):
    """
    add money to an account. Append a transaction row and update the account balance inside one unit of work.
    """
    try: 
        with conn.cursor() as cur:

            #ADD MONEY TO ACCOUNT 
            update_account = "UPDATE account SET  balance = balance + %s WHERE account_id = %s"
            cur.execute(update_account,(amount,accountId))             

            #INSERT TRANSACTION ROW
            size = """
                SELECT transaction_id
                FROM transactions
                ORDER BY transaction_id DESC
                LIMIT 1;
                """
            col_size =  cur.execute(size,())
            col_size = cur.fetchone()
            new_id = col_size[0] + 1
            date = datetime.now(UTC)

            new_transfer = "INSERT INTO transactions(transaction_id, account_id,merchant_id,type,time_stamp,amount,channel,note,transfer_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values =(new_id,accountId,None,'deposit',date,amount,'online',None,None)
            cur.execute(new_transfer,values)
            print("test")
        conn.commit()
    except Exception as e:
        print(f"Error depositing balance {accountId}: {e}")






def Withdraw(conn,accountId, amount):
    """
    take money out if funds plus overdraft allow it. Append a transaction row and update the balance in one unit of work.
    """
    try: 
        with conn.cursor() as cur:

            #ADD MONEY TO ACCOUNT 
            update_account = "UPDATE account SET  balance = balance - %s WHERE account_id = %s"
            cur.execute(update_account,(amount,accountId)) 

            #INSERT TRANSACTION ROW
            size = """
                SELECT transaction_id
                FROM transactions
                ORDER BY transaction_id DESC
                LIMIT 1;
                """
            col_size =  cur.execute(size,())
            col_size = cur.fetchone()
            new_id = col_size[0] + 1
            date = datetime.now(UTC)

            new_transfer = "INSERT INTO transactions(transaction_id, account_id,merchant_id,type,time_stamp,amount,channel,note,transfer_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values =(new_id,accountId,None,'withdrawal',date,amount,'online',None,None)
            cur.execute(new_transfer,values)
            print("test")
        conn.commit()
    except Exception as e:
        print(f"Error depositing balance {accountId}: {e}")




def Transfer(conn, fromAccId, toAccId, amount, merchantId=None, note=None, channel='online'):
    """
    move money between two accounts or to a verified merchant. Before completing the transfer, the system validates that the sender’s account has sufficient funds, the receiver or merchant exists, and that the merchant (if involved) is active. If valid, it creates two transaction rows (a debit and a credit) with the same transfer_id, commits them together as one atomic operation, and rejects duplicates by transfer_id.
    """
    transfer_id = 0
    try:
        with conn.cursor() as cur:
            # DOUBLE CHECK SAME AS NOSQL
            cur.execute("SELECT balance, overdraft_limit FROM account WHERE account_id = %s FOR UPDATE", (fromAccId,))
            sender = cur.fetchone()
            if not sender:
                print(f"Sender account {fromAccId} does not exist.")
                return

            balance, overdraft = sender
            if balance + overdraft < amount:
                print(f"Insufficient funds in account {fromAccId}.")
                return

            # Determine recipient
            if merchantId:
                cur.execute("SELECT is_active FROM merchant WHERE merchant_id = %s", (merchantId,))
                merchant = cur.fetchone()
                if not merchant or not merchant[0]:
                    print(f"Merchant {merchantId} does not exist or is inactive.")
                    return
                toAccId = merchantId
                recipient_type = 'merchant'
            else:
                cur.execute("SELECT account_id FROM account WHERE account_id = %s", (toAccId,))
                receiver = cur.fetchone()
                if not receiver:
                    print(f"Receiver account {toAccId} does not exist.")
                    return
                recipient_type = 'account'

            # prevent duplicate transfer
            cur.execute("SELECT 1 FROM transactions WHERE transfer_id = %s", (transfer_id,))
            if cur.fetchone():
                print(f"Duplicate transfer {transfer_id} detected. Skipping.")
                return

            # Get next transaction_id
            cur.execute("SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT 1")
            last_tx = cur.fetchone()
            next_tx_id = last_tx[0] + 1 if last_tx else 1
            date = datetime.now(UTC)


            cur.execute("""
                INSERT INTO transactions (transaction_id, account_id, merchant_id, type, time_stamp, amount, channel, note, transfer_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (next_tx_id, fromAccId, None, 'transfer_debit', date, amount, channel, note, transfer_id))


            cur.execute("""
                INSERT INTO transactions (transaction_id, account_id, merchant_id, type, time_stamp, amount, channel, note, transfer_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (next_tx_id + 1, toAccId if recipient_type == 'account' else None,
                  merchantId if recipient_type == 'merchant' else None,
                  'transfer_credit', date, amount, channel, note, transfer_id))

            # update balances
            cur.execute("UPDATE account SET balance = balance - %s WHERE account_id = %s", (amount, fromAccId))
            if recipient_type == 'account':
                cur.execute("UPDATE account SET balance = balance + %s WHERE account_id = %s", (amount, toAccId))

        conn.commit()
        print(f"Transfer {transfer_id} completed successfully.")

    except Exception as e:
        print(f"Transfer failed: {e}")


#Verified small test 
def getBalance(conn, accountId):
    """
    read the current account balance.
    ::param db:: active database object
    ::param accountId:: the _id of the account to retrieve the balance for
    """
    try: 
        with conn.cursor() as cur:
            balance_check = "SELECT balance FROM account WHERE account_id = %s"
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
            cur.execute("SELECT transaction_id FROM transactions WHERE account_id = %s ORDER BY transaction_id DESC", (accountId,))
            transact = cur.fetchall()
            for tran in transact: 
                print(tran[0])
           
    except Exception as e:
        print(f"Error Getting balance {accountId}: {e}")




#Verified small test 
def closeAccount(conn, account_id, status):
    """
    change status to closed or frozen. Do not delete rows. You want an audit trail.
    """
    try:
        with conn.cursor() as cur:
            update_account = "UPDATE account SET  status = %s WHERE account_id = %s"
            cur.execute(update_account,(status,account_id,)) 
            conn.commit()
        
    except Exception as e:
        print(f"Error closing account {account_id}: {e}")

############# Testing ############# 



def timed_operation(func, *args, **kwargs):
    """
    Measure the runtime of a single operation.
    """
    start = time.time()
    func(*args, **kwargs)
    end = time.time()
    return end - start








############## BANK FUNCTIONS ############## 
def main():

    conn = connectToDB()
    
    #Testing connection 
    if conn is not None: 
        
        #Initialize db
        # dropTables(conn)
        # createTables(conn)
        # loadData(conn) 


        conn.close()    
    else:
        print("Failed to connect to the database.")
    
main()

############## DEVELOPMENT EFFORTS ##############

#Time Per Feature 
#AVG
#Feauture


    