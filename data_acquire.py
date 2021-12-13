import time
import sched
import pandas
# import logging
# import requests
from io import StringIO

# import utils
# import database

from database import create_connection, read_tables, select_count_from_table, repopulate_table_complete, update_table, \
                     DB_STATUS_CODES, DB_LIMIT
from data_ETL import read_csv

TIMEOUT_PERIOD = 10 # seconds

def db_health_check(connection, verbose=False):
    health_status = DB_STATUS_CODES['Success']
    tables = read_tables(connection, verbose)
    print(tables)
    print('covid' not in tables)
    print(health_status)
    print('covidhistorical' not in tables)
    print("________")
#     return tables
    if 'covid' not in tables:
        print("covid not good")
        health_status = health_status | DB_STATUS_CODES['Missing covid table']
    # Complete repopulation of 'covidhistorical' table if DNE, or not enough data in it
    if ('covidhistorical' not in tables) or (select_count_from_table(connection, 'covidhistorical').iloc[0]['count'] < DB_LIMIT):
        print("covidistorical not good")
        health_status = health_status | DB_STATUS_CODES['Missing historical table']
    
    if ('covidhistorical' in tables):
        print(f"Historical count: {select_count_from_table(connection, 'covidhistorical').iloc[0]['count']}")
    else:
        print("Historical table DNE")
    print("Ending db_health_check")
    return health_status


def initial_db_setup(connection, df_covid_total, df_hist_total, health_status, verbose=False):
    if health_status == DB_STATUS_CODES['Success']:
        return health_status
    
    if health_status & DB_STATUS_CODES['Missing covid table']: # 1 if missing
        repop_status = repopulate_table_complete(connection, df_covid_total, 'covid', sort=False, verbose=verbose)
        if repop_status == DB_STATUS_CODES['Failure']:
            return repop_status

    if health_status & DB_STATUS_CODES['Missing historical table']: # 2 if missing
        print("In missing historical table intial_db_setup")
        repop_status = repopulate_table_complete(connection, df_hist_total, 'covidhistorical', verbose=verbose)
        if select_count_from_table(connection, 'covidhistorical').iloc[0]['count'] < DB_LIMIT:
            if verbose:
                print("CovidHistorical table failed to have enough rows populated.")
            repop_status = DB_STATUS_CODES['Failure']

        if repop_status == DB_STATUS_CODES['Failure']:
            return repop_status
    
    if verbose:
        hist_count = select_count_from_table(connection, 'covidhistorical').iloc[0]['count']
        print(f"CovidHistorical Table populated with {hist_count} rows!")
        print("Successful initial_db_setup.")
    return DB_STATUS_CODES['Success']


def incremental_update(connection, verbose=False):
    # Updates **only** the covid table incrementally
    df_covid_total = read_csv('covid')
    update_table(connection, df_covid_total, 'covid', sort=False, verbose=verbose)
    print("Completed an incremental update")
#     repopulate_table_complete(connection, df_covid_total, 'covid', sort=False, verbose=verbose)



MAX_LOOPS = 3
GLOBAL_COUNT=0

def main_loop(connection, timeout=TIMEOUT_PERIOD, verbose=False):
    scheduler = sched.scheduler(time.time, time.sleep)

    def _worker():
        global MAX_LOOPS, GLOBAL_COUNT
        print(f"MAXLOOPS: {MAX_LOOPS}; GLOBAL_COUNT: {GLOBAL_COUNT}")
        if GLOBAL_COUNT >= MAX_LOOPS:
            print("DONE")
            return 0
        try:
            print("About to update")
            incremental_update(connection, verbose=verbose)
            print("incrementing global count")
            GLOBAL_COUNT+=1
        except Exception as e:
            print(f"Main loop worker ignores exception and continues: {e}")
            # logger.warning("main loop worker ignores exception and continues: {}".format(e))
            
        print("Scheduling next event")
        scheduler.enter(timeout, 1, _worker)    # schedule the next event

    scheduler.enter(0, 1, _worker)              # start the first event
    scheduler.run(blocking=True) #@TODO: change to true


if __name__ == '__main__':
    #heroku connection
    connection = create_connection(
        "dcegl8mv856qb8", "ndvqpnrwxtmwvu", "eec515b7f7a6c5c44d4df10499aa344d698310c1b39474bd2aefca27633fb241", "ec2-3-89-214-80.compute-1.amazonaws.com", "5432"
    )

    df_covid_total = read_csv('covid')
    df_hist_total = read_csv('covidhistorical')

    db_health = db_health_check(connection, verbose=True)
    print(db_health)
    initial_db_setup(connection, df_covid_total, df_hist_total, db_health, verbose=True)
    
    print("Finished Initial Setup")

    main_loop(connection, verbose=True)

    connection.close() # Won't be hit when fully running