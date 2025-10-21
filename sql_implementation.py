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
        )
        return conn
    except psql.Error:
        conn = None
        return conn