
import pandas as pd
import psycopg as psql


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

                sql = """
                        INSERT INTO account (account_id, customer_id, open_date,status,balance, last_update,overdraft_limit)
                        VALUES (%s, %s, %s,%s,%s,%s,%s)
                        ON CONFLICT (account_id) DO NOTHING;
                    """
                cur.execute(sql, (account_id,customer_id,open_at,status,balance,update,overdraft))
        
        conn.commit()
        print("account data loaded")



    #transaction files -> minor issue - 10001 not present 

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
                
                sql = """
                        INSERT INTO transactions (transaction_id, account_id, merchant_id,type, time_stamp, amount,channel, note, transfer_id)
                        VALUES (%s, %s, %s,%s,%s,%s,%s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING;
                    """
                cur.execute(sql, (transaction_id, account_id, merchant_id,type, time_stamp, amount,channel, note, transfer_id))
        
        conn.commit()
        print("transaction data loaded")









############## BANK FUNCTIONS ############## 

def OpenAccount(conn):
    """
    create a new account for a customer with status open and balance 0.
    """
   
 
def Deposit():
    """
    add money to an account. Append a transaction row and update the account balance inside one unit of work.
    """

def Withdraw():
    """
    take money out if funds plus overdraft allow it. Append a transaction row and update the balance in one unit of work.
    """

def Transfer():
    """
    move money between two accounts or to a verified merchant. Before completing the transfer, the system validates that the sender’s account has sufficient funds, the receiver or merchant exists, and that the merchant (if involved) is active. If valid, it creates two transaction rows (a debit and a credit) with the same transfer_id, commits them together as one atomic operation, and rejects duplicates by transfer_id.
    """
def getBalance():
    """
    read the current account balance.
    """

def viewRecentTransactions():
    """
    read the last N transactions for an account, ordered by timestamp descending.
    """

def closeAccount():
    """
    change status to closed or frozen. Do not delete rows. You want an audit trail.
    """


############## BANK FUNCTIONS ############## 
def main():
    print("test")
    conn = connectToDB()
    
    #Testing connection 
    if conn is not None: 
        loadData(conn)     
        conn.close()       
    else:
        print("Failed to connect to the database.")
    


main()

############## DEVELOPMENT EFFORTS ##############

#Time Per Feature 
#AVG
#Feauture


    