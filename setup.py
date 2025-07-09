"""
Sets up configuration for electricity sector aggregation
Written by Ian David Elder for the CANOE model
"""

import os
import pandas as pd
import yaml
import sqlite3



def build_schema():
    conn = sqlite3.connect(config.database_file)
    conn.executescript(open(config.schema_file, 'r').read())
    conn.commit()
    conn.close()


def instantiate_database():
    # Check if database exists or needs to be built
    if os.path.exists(config.database_file):
        if config.params['force_wipe_database']:
            print('Database wiped prior to aggregation.')
            os.remove(config.database_file)
            build_schema()
    else:
        build_schema()



class config:

    # File locations
    _this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
    input_files = _this_dir + 'input_files/'

    _instance = None # singleton pattern



    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance
        cls._instance = super(config, cls).__new__(cls, *args, **kwargs)

        cls._get_params(cls._instance)
        cls._get_files(cls._instance)

        print('Instantiated setup config.\n')

        return cls._instance

        

    def _get_params(cls):

        stream = open(config.input_files + "params.yaml", 'r')
        config.params = dict(yaml.load(stream, Loader=yaml.Loader))

        config.regions = pd.read_csv(config.input_files + 'regions.csv', index_col=0)
        config.regions['include'] = config.regions['include'].fillna(False)
        config.commodities = pd.read_csv(config.input_files + 'commodities.csv', index_col=0)
        config.commodities['include'] = config.commodities['include'].fillna(False)
        config.sectors = pd.read_csv(config.input_files + 'sectors.csv', index_col=0)
        config.sectors['include'] = config.sectors['include'].fillna(False)

        # Included regions and future periods
        config.model_periods: list = list(config.params['model_periods'])
        config.model_periods.sort()
        config.model_regions: list = config.regions.loc[(config.regions['include'])].index.unique().to_list()
        config.model_regions.sort()
            


    def _get_files(cls):

        config.schema_file = config.input_files + config.params['sqlite_schema']
        config.database_file = config._this_dir + config.params['sqlite_database']
        instantiate_database()
        


# Instantiate config on import
config()