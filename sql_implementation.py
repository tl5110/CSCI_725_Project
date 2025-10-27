import mysql.connector



def connectToDB():
    """
    Establishes a connection to the PostgreSQL database
    ::return:: active database connection object if successful, None otherwise
    """
    try:

        ##DATA BASE CONNECTION SET UP####
        mydb = mysql.connector.connect(
            host="localhost", 
            user="root",
            password="MYsql990001161",
            database="CSCI_725_Project"  
        )

        
        return mydb #Connection successful
        
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong Username or Password")
        else:
            print(err)


############## READ IN DATA ##############
def readData():
    """
    Read in the data
    """






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
    conn = connectToDB()


main()

############## DEVELOPMENT EFFORTS ##############

#Time Per Feature 
#AVG
#Feauture


    