import censusdata
from gather_data import *
import os
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--search', action='store_true',
                    help="To perform a search for variables")
parser.add_argument('--get', action='store_true',
                    help="To load variables data from the CENSUS data and store to csv")
parser.add_argument('--store', action='store_true',
                    help="To load data from csv into the database")
args = parser.parse_args()

if args.search:
    # to search for variables in CENSUS data
    vars = censusdata.search('acs5', 2018, 'label', 'geoid', tabletype='detail')
    print(f"Found {len(vars)} matching variables.")
    # prints all retrieved census data variables to file
    with open("search_results.txt", "w") as f:
        for v in vars:
            f.write(str(v)+"\n")

if args.get:
    # to download the data from the CENSUS
    df = download_data('useful_variables.txt')
    # saves the retrieved data to a csv
    df.to_csv('data.csv')

if args.store:
    conn = open_db_connection()
    cur = conn.cursor()

    command = os.popen("cat data.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | " + \
                       "csvsql -i postgresql --db-schema acs --tables as").read()

    print(command)

    # Close communication with the database
    cur.close()
    conn.close()

