# HW1 - Data Collection and ETL

## Pre-requisites

- python3
- censusdata
- tqdm
- csvkit
- psycopg2-binary
- pyyaml


## Executing instructions
- To run the script for downloading the data and storing it in the database, run the following command. 
This will create a CSV file named `data.csv` with all the data entries, which will be uploaded to the database as well.
```
python get_data.py --get --store
```

- If you have a prebuilt `data.csv` file, you can run the following command to just read data from the CSV and store 
it to the database. 
```
python get_data.py --store
```

- To only download data and store it as CSV, without uploading it to the database, you can run
```
python get_data.py --get
```

- To search for variables, you can set the pattern to search for, and the field to search in, in the `censusdata.search` 
query in the `get_data.py` file, and run the following. This will create a file `search_results.py` containing all the 
search results.
```
python get_data.py --search
```

 
