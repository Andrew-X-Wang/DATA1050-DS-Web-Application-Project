"""
Bonneville Power Administration, United States Department of Energy
"""
import time
import sched
import pandas
# import logging
# import requests
from io import StringIO

# import utils
import database
from database import *

TIMEOUT_PERIOD = 10 # seconds

def db_health_check(connection, verbose=False):
    health_status = DB_STATUS_CODES['Success']
    tables = read_tables(connection, verbose)
    
    if 'covid' not in tables:
        health_status = health_status | DB_STATUS_CODES['Missing covid table']
    # Complete repopulation of 'covidhistorical' table if DNE, or not enough data in it
    if ('covidhistorical' not in tables) or (select_count_from_table(connection, 'covidhistorical').iloc[0]['count'] < DB_LIMIT):
        health_status = health_status | DB_STATUS_CODES['Missing both tables']
    
    return health_status


def initial_db_setup(connection, df_covid_total, df_hist_total, health_status, verbose=False):
    if health_status == DB_STATUS_CODES['Success']:
        return health_status
    
    if health_status & DB_STATUS_CODES['Missing historical table']: # 2 if missing
        repop_status = repopulate_table_complete(connection, df_hist_total, 'covidhistorical', verbose)
        if select_count_from_table(connection, 'covidhistorical').iloc[0]['count'] < DB_LIMIT:
            if verbose:
                print("CovidHistorical table failed to have enough rows populated.")
            repop_status = DB_STATUS_CODES['Failure']

        if repop_status == DB_STATUS_CODES['Failure']:
            return repop_status

    if health_status & DB_STATUS_CODES['Missing covid table']: # 1 if missing
        repopulate_table_complete(connection, df_covid_total, 'covid', verbose)
        if repop_status == DB_STATUS_CODES['Failure']:
            return repop_status
    
    if verbose:
        hist_count = select_count_from_table(connection, 'covidhistorical').iloc[0]['count']
        print(f"CovidHistorical Table populated with {hist_count} rows!")
        print("Successful initial_db_setup.")
    return DB_STATUS_CODES['Success']


def incremental_update(connection):
    # Updates **only** the covid table incrementally
    df_covid_total = read_csv('covid')

    repopulate_table_complete(connection, df_covid_total, 'covid')



def main_loop(connection, timeout=TIMEOUT_PERIOD):
    scheduler = sched.scheduler(time.time, time.sleep)

    def _worker():
        try:
            incremental_update(connection)
        except Exception as e:
            logger.warning("main loop worker ignores exception and continues: {}".format(e))
        scheduler.enter(timeout, 1, _worker)    # schedule the next event

    scheduler.enter(0, 1, _worker)              # start the first event
    scheduler.run(blocking=True)


if __name__ == '__main__':
    #heroku connection
    connection = create_connection(
        "dcegl8mv856qb8", "ndvqpnrwxtmwvu", "eec515b7f7a6c5c44d4df10499aa344d698310c1b39474bd2aefca27633fb241", "ec2-3-89-214-80.compute-1.amazonaws.com", "5432"
    )

    df_covid_total = read_csv('covid')
    df_hist_total = read_csv('covidhistorical')

    db_health = db_health_check(connection)
    print(db_health)
    
    ## @TODO: Initial DB Setup Not Working
    # initial_db_setup(connection, df_covid_total, df_hist_total, db_health, verbose=True)

    # main_loop(connection)


