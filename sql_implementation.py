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

    
def main():
    print("test")
    conn = connectToDB()


main()
    