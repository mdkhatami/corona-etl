import psycopg2
import psycopg2.extras
import numpy as np
import os
DB_INFO = {'dbname':'coronadb',
            'user':'postgres',
            'password':'postgres',
            'host':'localhost',
            'port':5432}

def connect_db():
    conn = psycopg2.connect(dbname=DB_INFO['dbname'],
                user=DB_INFO['user'],
                password=DB_INFO['password'],
                host=DB_INFO['host'],
                port=DB_INFO['port'])
    return conn
def init_db():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    corona_drop_table_sql = """DROP TABLE IF EXISTS corona"""
    corona_table_sql="""
                        CREATE TABLE IF NOT EXISTS corona (
                        country text  NOT NULL,
                        date TIMESTAMP NOT NULL,                
                        confirmed INTEGER NOT NULL,
                        deaths INTEGER NOT NULL,
                        recovered INTEGER NOT NULL,
                        PRIMARY KEY (country, date))
                    """
    corona_hypertable_sql = "SELECT create_hypertable('corona', 'date',if_not_exists => TRUE);"
    cur.execute(corona_drop_table_sql)
    conn.commit()
    cur.execute(corona_table_sql)
    conn.commit()
    cur.execute(corona_hypertable_sql)
    conn.commit()
def load(df, table):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, sep=';',index=False, header=False, na_rep=np.nan)
    #f = open(tmp_df, 'r')
    conn = connect_db()
    cur = conn.cursor()
    try:
        with open(tmp_df, 'r') as f:
            cur.copy_from(f, table, sep=";")
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        
        os.remove(tmp_df)
        print("Error: %s" % error)
        conn.rollback()
        conn.close()
        return -1
    print("load - done")
    conn.close()
    os.remove(tmp_df)
def batch_load(df,table):
    conn = connect_db()
    cur = conn.cursor()
    try:
        values = [tuple(x) for x in df.to_numpy()]
        # dataframe columns with Comma-separated
        cols = ','.join(list(df.columns))
        fields_template = ','.join(['%s'] * len(df.columns))
        sql = "INSERT INTO {}({}) VALUES ({}) ON CONFLICT  \
            DO NOTHING;".format(table, cols,fields_template)
        psycopg2.extras.execute_batch(cur, sql,values)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        conn.close()
        return -1    
    conn.close()
    return 1

