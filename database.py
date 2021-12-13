import psycopg2
from psycopg2 import OperationalError
from pandas.io import sql as sqlio

from data_ETL import *

DB_LIMIT = 9000 # max num of rows*0.9

TABLE_NAMES = ['covid', 'covidhistorical']

DB_STATUS_CODES = {
    'Success': 0,
    'Failure': -1,
    'Missing covid table': 1,
    'Missing historical table': 2,
    'Missing both tables': 3
}


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def read_tables(connection, verbose=False):
    #show all tables in db 
    cursor = connection.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    res = cursor.fetchall()

    # Formatting: original eg. [('covid',), ('covidhistorical',)]
    res = [r[0] for r in res]

    if verbose:
        print(res)

    cursor.close()
    return res


def select_all_from_table(connection, table_name, verbose=False):
    sql = f"SELECT * from {table_name};"
    df = sqlio.read_sql_query(sql, connection)
    if verbose:
        print(f"Table size: {df.shape}")
        print(df_cov.head(10))
    return df


def select_count_from_table(connection, table_name, verbose=False):
    sql = f"SELECT count(*) from {table_name};"
    df = sqlio.read_sql_query(sql, connection)
    if verbose:
        print(df)
    return df


def create_table(connection, table_name, verbose=False):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        create_table_query = f"CREATE TABLE {table_name} ("
        features_types = [col + (" double precision" if df_covid.dtypes[i] == 'float' else " varchar(60)") for i, col in enumerate(df.columns)]
        create_table_query += ", ".join(features_types) + ");"
        cursor.execute(create_table_query)

    except Exception as e:
        print(e)
        return DB_STATUS_CODES['Failure']
    
    connection.commit()
    if verbose:
        print(f"Successfully created table {table_name}.")
    cursor.close()
    return DB_STATUS_CODES['Success']


def repopulate_table_complete(connection, df_total, table_name, feat_to_limit='date', limit=DB_LIMIT, verbose=False):
    create_table_status = create_table(connection, table_name)
    if create_table_status == DB_STATUS_CODES['Failure']:
        if verbose:
            print(f"Failed to create table {table_name}")
        return create_table_status

    # Remove 'overly null' rows and columns
    df_clean = clean_null_data(df_total, verbose=verbose)
    # Limit to 9000 rows
    df_clean = df_clean[-limit:]
    
    cursor = connection.cursor()
    for i in range(df_clean.shape[0]):
        if verbose:
            if i % 100 == 0:
                print(f"Inserting row {i}.")
        row_list = df.iloc[i].tolist()
        str_row = [str(f) for f in row_list]
        str_row = ["NULL" if s == 'nan' else s for s in str_row]
        str_placeholders = ['%s' for i in df.columns]
        insert_p1 = f"INSERT INTO {table_name} VALUES ({', '.join(str_placeholders)})"
        try:
            cursor.execute(insert_p1, df.iloc[i].tolist())
        
        except Exception as e:
            print(e)
            return DB_STATUS_CODES['Failure']

    connection.commit()
    cursor.close()
    return DB_STATUS_CODES['Success']
