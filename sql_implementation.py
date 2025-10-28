
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

    #Account files 
    account_files = {
        "banking_datasets/baseline/baseline_accounts.csv",
        "banking_datasets/edgecases/edgecases_accounts.csv",
        "banking_datasets/hotspot/hotspot_accounts.csv",
        "banking_datasets/payday/payday_accounts.csv",
    }

    for file in account_files: 
        read = pd.read_csv(file,skiprows=1,usecols=[0,1,2,3,4,5,6,7])

 
    # file = pd.read_csv(TrafficFile,skiprows=1, usecols=[0,1])
    # for row in file.itertuples(index=False): 
        
   
    #customer files 

    #read in the merchant files and load them into the db 
    merchant_files = {
        "banking_datasets/baseline/baseline_merchants.csv",
        "banking_datasets/edgecases/edgecases_merchants.csv",
        "banking_datasets/hotspot/hotspot_merchants.csv",
        "banking_datasets/payday/payday_merchants.csv",
    }

    for file in merchant_files: 
        read = pd.read_csv(file,skiprows=1,usecols=[0,1,2])
        for row in read.itertuples(index=False): 
            id = row[0]
            name = row[1]
            category = row[2]

            



    #transaction files









############## BANK FUNCTIONS ############## 

def OpenAccount():
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
    test = connectToDB()
   


main()

############## DEVELOPMENT EFFORTS ##############

#Time Per Feature 
#AVG
#Feauture


    