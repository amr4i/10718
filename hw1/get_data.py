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
    df.to_csv('data.csv', index=False)

if args.store:
    conn = open_db_connection()
    cur = conn.cursor()

    create_command = os.popen("cat data.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/#/num/' | " + \
                       "csvsql -i postgresql --db-schema acs --tables as").read()
    print(create_command)
    cur.execute(create_command)

    # building primary index for table
    with open("data.csv", "r") as f:
        header = f.readlines()[0].split(",")

    if header[0] == 'state':
        keys = ['state']
    else:
        keys = ['us']
    if header[1] == 'county':
        keys.append('county')
    if header[2] == 'tract':
        keys.append('tract')
    if header[3] == 'block group':
        keys.append("block group")
    elif header[3] == 'block':
        keys.append('block')
    key = tuple(key)

    command = f"ALTER TABLE acs.as ADD PRIMARY KEY {key}"
    print(command)
    cur.execute(command)

    # Close communication with the database
    cur.close()
    conn.commit()
    conn.close()

