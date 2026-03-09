import pandas as pd
import sqlite3
from setup import config

def build():
    
    build_sectors()
    if config.params['use_dsd']: build_dsd() # only applies to electricity demand
    if config.params['build_test_model']: build_tester()
    build_metadata()

    print("\nFinished!")


def build_sectors():

    print("Adding sector processes...")

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    # Get CEF data from local file
    df_cef: pd.DataFrame = pd.read_csv(config.input_files + 'end-use-demand-2023.csv')

    # Map CEF indexing to CANOE indexing
    region_map = dict()
    for region, row in config.regions.iterrows():
        if row['include']: region_map[row['cef_region']] = region
    commodity_map = dict()
    for comm, row in config.commodities.iterrows():
        if row['include']: commodity_map[row['cef_fuel']] = comm
    sector_map = dict()
    technology_map = dict()
    for tag, row in config.sectors.iterrows():
        if row['include']:
            sector_map[row['cef_sector']] = tag
            technology_map[row['cef_sector']] = row['code']

    # Filter relevant data
    df_cef = df_cef[
        (df_cef['Scenario'] == config.params['scenario'])
        & df_cef['Sector'].isin(sector_map.keys())
        & df_cef['Region'].isin(region_map.keys())
        & df_cef['Variable'].isin(commodity_map.keys())
        & df_cef['Year'].isin(config.model_periods)
    ]

    # Convert to CANOE indexing
    df_cef['region'] = df_cef['Region'].map(region_map, na_action='ignore')
    df_cef['tag'] = df_cef['Sector'].map(sector_map, na_action='ignore')
    df_cef['tech'] = df_cef['Sector'].map(technology_map, na_action='ignore')
    df_cef['comm'] = df_cef['Variable'].map(commodity_map, na_action='ignore')
    df_cef['tech'] = df_cef['tag'] + '_' + df_cef['tech']
    df_cef['comm'] = df_cef['tag'] + '_' + df_cef['comm']
    df_cef['period'] = df_cef['Year']

    # Convert energy units and filter out tiny values
    df_cef['value'] = df_cef['Value'] * config.params['conversion_factor']
    df_cef['value'] = df_cef['value'].round(config.params['decimal_places'])

    # Get the total energy for each process in each period
    df_sum = df_cef.groupby(['region','tech','period'])['value'].sum()

    # Filter out unused or tiny energy streams
    df = df_cef.groupby(['region','tech','comm'])[['period','value']]
    to_drop = set()
    for (region, tech, comm), _df in df:
        # Drop if this commodity isn't used by this sector in this region
        if _df['value'].sum() == 0:
            to_drop.add((region, tech, comm))
            continue
        _df = _df.set_index('period')
        _df['prop'] = [value.iloc[0] / df_sum.loc[(region, tech, period)] for period, value in _df.iterrows()]
        _df['keep'] = _df['prop'] >= config.params['prop_thresh']

        # Drop if this commodity doesn't meet the proportion threshold
        if _df['keep'].sum() == 0:
            to_drop.add((region, tech, comm))
            continue
    
    df_cef = df_cef.set_index(['region','tech','comm'])
    df_cef = df_cef[~df_cef.index.isin(to_drop)]
    df_cef = df_cef.reset_index()
    
    # Group indices nicely
    df_cef = df_cef.reset_index()
    df_cef = df_cef.set_index(['region','tech','period','comm'])['value']
    df_cef = df_cef.sort_index()

    # Add whole-sector processes
    for tech in df_cef.index.get_level_values('tech').unique():

        sector = config.sectors.loc[tech.split("_")[0]]
        data_id = config.data_id(sector['code'])
        
        # Technology
        sql = (
            'REPLACE INTO Technology(tech, flag, sector, unlim_cap, annual, description, data_id) '
            f'VALUES("{tech}", "p", "{sector["sector"]}", 1, 1, "{sector["tech_desc"]}", "{data_id}")'
        )
        curs.execute(sql)

        # SectorLabel
        sql = (
            'REPLACE INTO SectorLabel(sector) '
            f'VALUES("{sector["sector"]}")'
        )
        curs.execute(sql)

        # Commodity (demand comm)
        dem_comm = f'{tech.split("_")[0]}_D_{tech.split("_")[1].lower()}'
        sql = (
            'REPLACE INTO Commodity(name, flag, description, data_id) '
            f'VALUES("{dem_comm}", "d", "({config.params["energy_units"]}) {sector["sector"]} energy demand", "{data_id}")'
        )
        curs.execute(sql)

    # Add commodities
    for comm in df_cef.index.get_level_values('comm').unique():

        sector = config.sectors.loc[comm.split("_")[0]]
        data_id = config.data_id(sector['code'])

        # Because two CEF biofuel types go to bio
        commodity = config.commodities.loc[comm.split("_")[1]]
        if isinstance(commodity, pd.DataFrame):
            commodity = commodity.iloc[0]

        desc = f'{commodity["description"]} ({sector["sector"]})'

        # Commodity (fuel comms)
        sql = (
            'REPLACE INTO Commodity(name, flag, description, data_id) '
            f'VALUES("{comm}", "{commodity["flag"]}", "({config.params["energy_units"]}) {desc}", "{data_id}")'
        )
        curs.execute(sql)

    # first vintage processes for all techs
    for region, tech, comm in df_cef.xs(config.model_periods[0], level='period').index:

        sector = config.sectors.loc[comm.split("_")[0]]['code']
        data_id = config.data_id(sector, region)
        dem_comm = f'{tech.split("_")[0]}_D_{tech.split("_")[1].lower()}'

        # Efficiency
        sql = (
            'REPLACE INTO Efficiency(region, input_comm, tech, vintage, output_comm, efficiency, data_id) '
            f'VALUES("{region}", "{comm}", "{tech}", {period_index(config.model_periods[0])}, "{dem_comm}", 1.0, "{data_id}")'
        )
        curs.execute(sql)

    ref = config.refs.add('cer', config.params['cef_reference'])

    # Demands for each sector
    for (region, tech, period), demand in df_cef.reset_index().groupby(['region','tech','period']):

        p = period_index(period)

        sector = config.sectors.loc[tech.split("_")[0]]['code']
        data_id = config.data_id(sector, region)

        demand = demand.round(config.params['decimal_places'])
        dem_tot = round(demand['value'].sum(), config.params['decimal_places'])
        dem_comm = f'{tech.split("_")[0]}_D_{tech.split("_")[1].lower()}'

        # Demand
        sql = (
            'REPLACE INTO Demand(region, period, commodity, demand, units, data_source, data_id) '
            f'VALUES("{region}", {p}, "{dem_comm}", {dem_tot}, "{config.params["energy_units"]}", "{ref.id}", "{data_id}")'
        )
        curs.execute(sql)

        # Zero out tiny proportions and renormalise
        demand['prop'] = demand['value'] / dem_tot
        demand['prop'] = demand['prop'].where(demand['prop'] > config.params['prop_thresh'], 0)
        demand['prop'] /= demand['prop'].sum()
        
        # LimitTechInputSplitAnnual
        for _, row in demand.iterrows():
            prop = row['prop']
            # prop = round(prop, config.params['decimal_places']) # causes issues on sum total not worth
            sql = (
                'REPLACE INTO '
                'LimitTechInputSplitAnnual(region, period, input_comm, tech, operator, proportion, data_source, data_id) '
                f'VALUES("{region}", {p}, "{row["comm"]}", "{tech}", "le", {prop}, "{ref.id}", "{data_id}")'
            ) # <= should, in theory, be least likely to cause infeasibility / numerical issues?
            curs.execute(sql)

    conn.commit()
    conn.close()
        

def build_dsd():

    df_dsd = pd.read_csv(config.input_files + 'dsd_electricity.csv')

    conn = sqlite3.connect(config.database_file)

    print("Adding DSDs...", end="")

    data = []
    progr = 0
    for region in config.model_regions:
        data_id = config.data_id(region)
        for tag, sector in config.sectors.iterrows():
            for period in config.model_periods:
                for _, row in df_dsd.iterrows():
                    dem_comm = f'{tag}_D_{sector["tech"].lower()}'
                    val = row[f'{region}.{tag}']
                    data.append((region, period_index(period), row['season'], row['tod'], dem_comm, val, data_id))
            progr += 1
            print(f'\rAdding DSDs... {progr/(len(config.model_regions)*len(config.sectors))*100:.0f}% complete.', end="")
    sql = (
        'REPLACE INTO DemandSpecificDistribution(region, period, season, tod, demand_name, dsd, data_id) '
        f'VALUES(?,?,?,?,?,?,?)'
    )

    print("") # newline
    conn.executemany(sql, data)

    conn.commit()
    conn.close()


def build_tester():

    df_dsd = pd.read_csv(config.input_files + 'dsd_electricity.csv')

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    # Commodity
    sql = (
        'UPDATE Commodity SET flag = "s" WHERE flag = "a"'
    )
    curs.execute(sql)
    
    # Region
    for region in config.model_regions:
        sql = f'REPLACE INTO Region(region) VALUES("{region}")'
        curs.execute(sql)

    # TimePeriod
    for i, period in enumerate(config.model_periods):

        period_idx = period_index(period)

        sql = (
            'REPLACE INTO TimePeriod(sequence, period, flag) '
            f'VALUES({i}, {period_idx}, "f")'
        )
        curs.execute(sql)

        for idx, row in df_dsd.iterrows():
            sql = (
                'REPLACE INTO TimeSeason(period, sequence, season) '
                f'VALUES({period_idx}, {idx}, "{row["season"]}")'
            )
            curs.execute(sql)
            sql = (
                'REPLACE INTO TimeSegmentFraction(period, season, tod, segfrac) '
                f'VALUES({period_idx}, "{row["season"]}", "{row["tod"]}", {1/len(df_dsd)})'
            )
            curs.execute(sql)

    sql = (
        'REPLACE INTO TimePeriod(sequence, period, flag) '
        f'VALUES({i+1}, {period_idx+5}, "f")'
    )
    curs.execute(sql)

    # Time slices
    for season in df_dsd['season'].unique():
        sql = (
            'REPLACE INTO SeasonLabel(season) '
            f'VALUES("{season}")'
        )
        curs.execute(sql)
    for idx, tod in enumerate(df_dsd['tod'].unique()):
        sql = (
            'REPLACE INTO TimeOfDay(sequence, tod) '
            f'VALUES({idx}, "{tod}")'
        )
        curs.execute(sql)

    conn.commit()
    conn.close()


def build_metadata():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    # Add all references in the bibliography to the references tables
    for reference in config.refs:
        for sector in config.sectors['code'].unique():
            curs.execute(
                f"""REPLACE INTO
                DataSource(source_id, source, data_id)
                VALUES('{reference.id}', '{reference.citation}', "{config.data_id(sector)}")"""
            )

    # Add datasets to DataSet table, waiting for other metadata
    for id in sorted(config.data_ids):
        curs.execute(
            f"""REPLACE INTO
            DataSet(data_id)
            VALUES('{id}')"""
        )

    # Check for missing data IDs
    print("Checking that all data has a dataset ID...", end="")
    tables = [t[0] for t in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    all_good = True
    for table in tables:
        cols = [c[1] for c in curs.execute(f"PRAGMA table_info({table})").fetchall()]
        if "data_id" in cols:
            bad_rows = pd.read_sql_query(f"SELECT * FROM {table} WHERE data_id is NULL", conn)
            if len(bad_rows) > 0:
                print(f"\nFound some rows missing data IDs in {table}")
                print(bad_rows)
                all_good = False

    if all_good: print(" All good!")
        
    conn.commit()
    conn.close()


def period_index(period: int) -> int:
    return period - 5

if __name__ == "__main__":
    build()