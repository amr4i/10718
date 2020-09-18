import pandas as pd
import censusdata
from params import *
from tqdm import tqdm
from ast import literal_eval
import yaml
import psycopg2 as pg2

with open('secrets.yaml', 'r') as f:
    # loads contents of secrets.yaml into a python dictionary
    secret_config = yaml.safe_load(f.read())
    db_params = secret_config['db']
    API_KEY = secret_config['web_resource']['api_key']


def open_db_connection():
    """
    Opens connection to psql db

    :return:
        connection object
    """
    conn = pg2.connect(
        host=db_params['host'],
        port=db_params['port'],
        dbname=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password']
    )
    print(f"Connection opened to database {db_params['dbname']}")
    return conn


class geoLevel():
    """
    Geographical Granularity object that store the
    name of the geographical level to obtain the data from
    and its position in the hierarchy

    :param:
        geo : Name of the geographical level
            must be one of ['us', 'state', 'county', 'tract', 'block group', 'block']]
    """
    def __init__(self, geo):
        self.name = geo
        if geo == 'us' or geo == 'state':
            self.level = 0
        elif geo == 'county':
            self.level = 1
        elif geo == 'tract':
            self.level = 2
        elif geo == 'block group' or geo == 'block':
            self.level = 3
        else:
            raise AssertionError("geo must be one of ['us', 'state', 'county', 'tract', 'block group', 'block']]")


def get_variables(filename):
    """
    Get variables from a file, storing one variable 3-tuple per line.
    The variable 3-tuple is of the form as returned by censusdata.search()

    :param:
         filename (string) : name of the file holding the variables

    :return:
        a list of variable id names
    """
    with open(filename, 'r') as f:
       all_tuples = f.readlines()

    def oper(tuple, i):
        tuple = literal_eval(tuple)
        return tuple[i]

    vars = [oper(tuple, 0) for tuple in all_tuples]
    headers = [oper(tuple, 1) for tuple in all_tuples]
    return vars, headers


def download_data(vars):
    """
    function to download data from the ACS website

    :param:
        geo_level (geoLevel object): which geophical granularity to obtain for the data
        vars (string): a file name that holds 3-tuples of the variables,
            (in the format returned by censusdata.search()),
            where first is the variable id, and second is the variable header.
    :return:
        a pandas.DataFrame object
    """
    gl = geoLevel(geo_level_name)
    print(f"Getting {gl.name} level geographies...")
    geographies = get_censusgeos(gl)
    vars, headers = get_variables(vars)
    data = []
    print("Downloading selected variables for these geographies...")
    for geo in tqdm(geographies):
        local_data = censusdata.download(data_source, year, geo, vars, tabletype=tabletype, key=API_KEY)
        data.append(local_data)
    data = pd.concat(data)
    df.columns = headers
    return data


def _get_geo(geotype, names=None, higher_list=None):
    """
    Helper function to obtain geographies from one level to the next

    :param:
        geotype (string) : name of the geography (e.g. 'state')
        names (list of string) : names of the specific geographical location
            you want to pull the data for,
            or None if you want it for all
        higher_list (list of tuples of string):
            the list of the higher level hierarchy of geo locations
            reaching upto that level
            (e.g. if 'geo' is 'tract', then this could be
            [('state', 'Pennsylvania'), ('county', 'York County')] )

    :return:
        list of censusgeo objects
    """
    if higher_list is None:
        higher_list = []
    geo = [censusdata.censusgeo(higher_list + [(geotype, '*')])]
    if names is not None:
        all_geos = censusdata.geographies(geo[0], data_source, year, key=API_KEY)
        geo = []
        for name in names:
            geo.append(all_geos[name])

    return geo


def get_censusgeos(geo_level):
    """
    Gets the censusgeo objects for the specified geography,
    and specific names (if specified)

    :param:
        geo_level (geoLevel object): geo level at which granularity the data
            needs to be obtained at
    :return:
        a list of censusgeo objects
    """

    # to obtain natiowide data
    if geo_level.name == 'us':
        final_geo = _get_geo('us')

    # to obtain state wise data
    else:
        state_geos = _get_geo('state', state_names)
        final_geo = state_geos

    # get the county level geographies
    if geo_level.level >= 1:
        # iterate over the states
        county_geos = []
        for i in range(len(state_names)):
            state_name = state_names[i]
            if county_names is None or county_names[i] is None:
                county_state_names = None
            else:
                county_state_names = [cn+", "+state_name for cn in county_names[i]]
            geo = _get_geo('county', county_state_names, list(state_geos[i].params()))

            # Census API doesn't support using wildcards for 'county' for lower levels of hierarchy
            if geo_level.level > 1 and county_state_names is None:
                all_geos = censusdata.geographies(geo[0], data_source, year, key=API_KEY)
                geo = all_geos.values()

            county_geos += geo
        final_geo = county_geos

    # the following part could be done in a simpler manner than what is done below, but this implementation
    # allows easy extension to the cases where we might need to specify specific tracts and blocks.

    # getting all tracts or blocks or block groups
    for level in [2,3]:
        if geo_level.level >= level:
            if level == 2:
                name = 'tract'
            else:
                name = geo_level.name       # could be 'block' or 'block group'
            level_geos = []
            for i in range((len(final_geo))):
                geo = _get_geo(name, None, list(final_geo[i].params()))
                level_geos += geo
            final_geo = level_geos
        else:
            break

    return final_geo


