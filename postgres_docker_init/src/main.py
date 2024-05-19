import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# get postgres credentials
def get_pg_cred():
    return {
        'username': os.environ.get("POSTGRES_USER"), 
        'password': os.environ.get("POSTGRES_PASSWORD"), 
        'host': os.environ.get("POSTGRES_HOST"), 
        'port': os.environ.get("POSTGRES_PORT"), 
        'db_name': os.environ.get("POSTGRES_DB")
    }

# start postgres connection
def start_postgres_connection():
    creds = get_pg_cred()
    connection = psycopg2.connect(
        user=creds['username'],
        password=creds['password'],
        host=creds['host'],
        port=creds['port'],
        dbname=creds['db_name']
    )
    return connection

# query database
def query_database(connection, query_string):
    with connection.cursor() as cursor:
        cursor.execute(query_string)
        no_of_record = cursor.fetchall()
    connection.close()
    return no_of_record


if __name__ == "__main__":

    # instantiate PostgreSQL connection
    conn = start_postgres_connection()

    # sql query to count the number of records in your table
    query = """
    SELECT COUNT(*) 
    FROM OLYMPIC.ATHLETE_EVENTS;
    """
    
    # query the database and get the result
    result = query_database(connection=conn, query_string=query)
    
    # print the result
    print(f"Number of records in OLYMPIC.ATHLETE_EVENTS: {result[0][0]}")