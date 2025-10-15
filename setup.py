"""
Sets up configuration for electricity sector aggregation
Written by Ian David Elder for the CANOE model
"""

import os
import pandas as pd
import yaml
import sqlite3



def instantiate_database():
    
    # Check if database exists or needs to be built
    build_db = not os.path.exists(config.database_file)

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    # Build the database if it doesn't exist. Otherwise clear all data if forced
    if build_db: curs.executescript(open(config.schema_file, 'r').read())
    elif config.params['force_wipe_database']:
        tables = [t[0] for t in curs.execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()]
        for table in tables: curs.execute(f"DELETE FROM '{table}'")
        curs.executescript(open(config.schema_file, 'r').read())
        print("Database wiped prior to aggregation. See params.\n")

    conn.commit()

    # VACUUM operation to clean up any empty rows
    conn.execute("VACUUM;")
    conn.commit()

    conn.close()



class reference:
    """
    Stores a single reference and its attributes
    - id: the unique id for the source_id column
    - citation: the full citation to go in the DataSource table
    """

    id: str
    citation: str

    def __init__(self, id: str, citation: str):
        self.id = id
        self.citation = citation


class bibliography:
    """This class stores references and handles unique indexing"""

    references: dict[str, reference] = dict()

    def __iter__(self):
        for name, ref in self.references.items():
            yield ref

    def add(cls, name: str, citation: str) -> reference | None:
        """Add a reference to the log and return the reference object"""

        if name in cls.references:
            return cls.references[name]
        else:
            num = len(cls.references.keys()) + 1
            var = config.params['data_variant']
            id = f"{var}{num}" if num >= 10 else f"{var}0{num}" # source 01 to 99
            ref = reference(id=id, citation=citation)
            cls.references[name] = ref
            return ref
    
    def get(cls, name: str) -> reference | None:
        """Returns a reference by its semantic name"""

        if name not in cls.references:
            print(f"Tried to get a reference that had not been added yet: {name}")
            return
        else:
            return cls.references[name]



class config:

    # File locations
    _this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
    input_files = _this_dir + 'input_files/'

    refs: bibliography = bibliography()
    data_ids = set()

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
        config.database_file = config.params['sqlite_database']
        instantiate_database()


    # Gets a formatted dataset ID
    def data_id(
            sector: str = '',
            region: str = '',
        ):

        id = f"{sector}{config.params['data_variant']}{region}{config.params['data_version']}"
        config.data_ids.add(id)
        return id
        


# Instantiate config on import
config()