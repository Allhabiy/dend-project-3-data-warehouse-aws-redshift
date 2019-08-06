import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables
    
    Purpose of this funciton is to obtain song and log data 
    from the following S3 locations on the S3 Buckets
    s3://udacity-dend/song_data
    s3://udacity-dend/log_data
    
    and load them into staging tables - stg_event and stg_song
    
    Parameters
    ----------
    cur : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    conn : string
        Connection object which is responsible for execution of commands
        
    Returns
    ----------
    No return value
    
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load staging tables
    
    Purpose of this funciton is to extract the data from
    staging tables and load them into other dimension and
    fact tables song
    
    Parameters
    ----------
    cur : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    conn : object
        Connection object which is responsible for execution of commands
        
    Returns
    ----------
    No return value
    
    """       
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()