import psycopg2
from psycopg2 import OperationalError
# from pandas.io import sql as sqlio
import pandas.io.sql as sqlio
import expiringdict

from data_ETL import *


RESULT_CACHE_EXPIRATION = 24 * 3600 # seconds; 24 hours

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
    print("IN READ TABLES")
    cursor = connection.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    res = cursor.fetchall()
    print("CURSOR EXECUTED READ TABLES")

    # Formatting: original eg. [('covid',), ('covidhistorical',)]
    res = [r[0] for r in res]

    if verbose:
        print(res)

    cursor.close()
    return res


def select_all_from_table(connection, table_name, verbose=False):
    cursor = connection.cursor()
    cursor.execute(f"select * from {table_name};")
    res = cursor.fetchall()
    descr = cursor.description
    columns = [d[0] for d in descr]

    df = pd.DataFrame(res, columns=columns)
    if verbose:
        print(df)
    return df


def select_count_from_table(connection, table_name, verbose=False):
    cursor = connection.cursor()
    cursor.execute(f"select count(*) from {table_name};")
    res = cursor.fetchall()
    descr = cursor.description
    columns = [d[0] for d in descr]

    df = pd.DataFrame(res, columns=columns)
    if verbose:
        print(df)
    return df


def create_table(connection, df_total, table_name, verbose=False):
    cursor = connection.cursor()
    print(f"CURSOR CREATED for {table_name}")
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print("TABLE DROPPED")
        create_table_query = f"CREATE TABLE {table_name} ("
        features_types = [col + (" double precision" if df_total.dtypes[i] == 'float' else " varchar(60)") for i, col in enumerate(df_total.columns)]
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


def repopulate_table_complete(connection, df_total, table_name, feat_to_limit='date', sort=True, limit=DB_LIMIT, verbose=False):
    print("In repopulate table complete")
    # Remove 'overly null' rows and columns
    df_clean = clean_null_data(df_total, verbose=verbose) # Should do nothing to 'latest covid' data
    print("CLEANED DATA")
    if sort:
        df_clean.sort_values(by=feat_to_limit, inplace=True)
    # Limit to 9000 rows
    df_clean = df_clean[-limit:]
    
    create_table_status = create_table(connection, df_clean, table_name, verbose=verbose)
    if create_table_status == DB_STATUS_CODES['Failure']:
        if verbose:
            print(f"Failed to create table {table_name}")
        return create_table_status
    
    cursor = connection.cursor()
    for i in range(df_clean.shape[0]):
        if verbose:
            if i % 100 == 0:
                print(f"Inserting row {i}.")
        row_list = df_clean.iloc[i].tolist()
#         str_row = [str(f) for f in row_list]
#         str_row = ["NULL" if s == 'nan' else s for s in str_row]
        str_placeholders = ['%s' for i in df_clean.columns]
        insert_p1 = f"INSERT INTO {table_name} VALUES ({', '.join(str_placeholders)})"
    
        try:
            cursor.execute(insert_p1, row_list)
        
        except Exception as e:
            print(e)
            return DB_STATUS_CODES['Failure']

    connection.commit()
    cursor.close()
    return DB_STATUS_CODES['Success']


#@TODO: MUST follow columns of existing table (otherwise inserts leave last columns as NULL)
## OPTION 1: Don't clean data, leave it at 67 columns 
## OPTION 2: Read column names from current table with res.description(), insert <-- Current implementation
def update_table(connection, df_total, table_name, feat_to_limit='date', sort=True, limit=DB_LIMIT, verbose=False):
    # @TODO: Only should be run for 'covid' table for now
    df_db = get_covid(connection, table_name)

    # Combining latest data w data from database
    df_combined = pd.concat([df_total, df_db])

    ## OPTION 1: Only keep data from last 2 days: 
    # unique_dates = df_combined['last_updated_date'].unique()
    # unique_dates.sort()
    # recent_mask = df_full['last_updated_date'].isin(unique_dates[-2:])

    ## OPTION 2: Only keep most recent data per location:
    df_combined.sort_values(by='last_updated_date', inplace=True)
    # Keep 'last' or most recent data, duplicates are by location and last_updated_date
    df_updated = df_combined.drop_duplicates(subset=['location', 'last_updated_date'], inplace=False, keep='last')
    print(f"df_updated shape: {df_updated.shape}")

    ## @TODO: Remove all rows from db
    # ...
    dummy_date = "'000-00-00'"
    cursor = connection.cursor()
    try:
        cursor.execute(f"DELETE FROM {table_name} WHERE last_updated_date > {dummy_date}")
    except Exception as e:
        print(e)
        return DB_STATUS_CODES['Failure']
    connection.commit()
    cursor.close()
    
    # Insert back into db: don't have to clean for 'covid' table/dataset
    cursor = connection.cursor()
    for i in range(df_updated.shape[0]):
        if verbose:
            if i % 100 == 0:
                print(f"Inserting row {i}.")
        row_list = df_updated.iloc[i].tolist()
#         str_row = [str(f) for f in row_list]
#         str_row = ["NULL" if s == 'nan' else s for s in str_row]
        str_placeholders = ['%s' for i in df_updated.columns]
        insert_p1 = f"INSERT INTO {table_name} VALUES ({', '.join(str_placeholders)})"
    
        try:
            cursor.execute(insert_p1, row_list)
        
        except Exception as e:
            print(e)
            return DB_STATUS_CODES['Failure']

    connection.commit()
    
    cursor.execute("SELECT count(*) FROM covid;")
    print("NEW COUNT IN COVID:")
    print(cursor.fetchall())
    
    cursor.close()
    return DB_STATUS_CODES['Success']


def get_covid(connection, table_name):
    try:
        if table_name == 'covid':  
            return _cached_covid_table['cache']
        elif table_name == 'covidhistorical':
            return _cached_historical_table['cache']
        else:
            raise NameError(f"{table_name} is not a valid table. The only available tables are: {TABLE_NAMES}.")
            return DB_STATUS_CODES['Failure']
    except KeyError:
        pass
    
    df_db = select_all_from_table(connection, table_name)
    if table_name == 'covid':
        _cached_covid_table['cache'] = df_db
    elif table_name == 'covidhistorical':
        _cached_historical_table['cache'] = df_db
    else:
        raise NameError(f"{table_name} is not a valid table. The only available tables are: {TABLE_NAMES}.")

    return df_db


_cached_covid_table = expiringdict.ExpiringDict(max_len=1,
                                                       max_age_seconds=RESULT_CACHE_EXPIRATION)

_cached_historical_table = expiringdict.ExpiringDict(max_len=1,
                                                       max_age_seconds=RESULT_CACHE_EXPIRATION)

# _conn = create_connection(
#         "dcegl8mv856qb8", "ndvqpnrwxtmwvu", "eec515b7f7a6c5c44d4df10499aa344d698310c1b39474bd2aefca27633fb241", "ec2-3-89-214-80.compute-1.amazonaws.com", "5432"
#     )