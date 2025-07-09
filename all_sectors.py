import pandas as pd
import sqlite3
from setup import config
from itertools import product

def build():
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    df_cef: pd.DataFrame = pd.read_csv('input_files/end-use-demand-2023.csv')

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
            technology_map[row['cef_sector']] = row['tech']

    # Filter data
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

    # Filter out zero or tiny streams
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

    # Add sectors
    for tech in df_cef.index.get_level_values('tech').unique():
        sector = config.sectors.loc[tech.split("_")[0]]
        
        # Technology
        sql = (
            'REPLACE INTO Technology(tech, flag, sector, unlim_cap, annual, description) '
            f'VALUES("{tech}", "p", "{sector["sector"]}", 1, 1, "{sector["tech_desc"]}")'
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
            'REPLACE INTO Commodity(name, flag, description) '
            f'VALUES("{dem_comm}", "d", "({config.params["energy_units"]}) {sector["sector"]} energy demand")'
        )
        curs.execute(sql)

    # Add commodities
    for comm in df_cef.index.get_level_values('comm').unique():
        sector = config.sectors.loc[comm.split("_")[0]]
        commodity = config.commodities.loc[comm.split("_")[1]]
        if isinstance(commodity, pd.DataFrame):
            commodity = commodity.iloc[0]
        desc = f'{commodity["description"]} ({sector["sector"]})'

        # Commodity (fuel comms)
        sql = (
            'REPLACE INTO Commodity(name, flag, description) '
            f'VALUES("{comm}", "a", "({config.params["energy_units"]}) {desc}")'
        )
        curs.execute(sql)

    # 2025 vintage processes for all techs
    for region, tech, comm in df_cef.xs(2025, level='period').index:
        dem_comm = f'{tech.split("_")[0]}_D_{tech.split("_")[1].lower()}'

        # Efficiency
        sql = (
            'REPLACE INTO Efficiency(region, input_comm, tech, vintage, output_comm, efficiency) '
            f'VALUES("{region}", "{comm}", "{tech}", 2025, "{dem_comm}", 1.0)'
        )
        curs.execute(sql)

    # Demands for each sector
    for (region, tech, period), demand in df_cef.reset_index().groupby(['region','tech','period']):

        demand = demand.round(config.params['decimal_places'])
        dem_tot = round(demand['value'].sum(), config.params['decimal_places'])
        dem_comm = f'{tech.split("_")[0]}_D_{tech.split("_")[1].lower()}'

        # Demand
        sql = (
            'REPLACE INTO Demand(region, period, commodity, demand, units) '
            f'VALUES("{region}", {period}, "{dem_comm}", {dem_tot}, "{config.params["energy_units"]}")'
        )
        curs.execute(sql)
        
        # LimitTechInputSplitAnnual
        for _, row in demand.iterrows():
            prop = row['value'] / dem_tot
            # prop = round(prop, config.params['decimal_places'])
            sql = (
                'REPLACE INTO LimitTechInputSplitAnnual(region, period, input_comm, tech, operator, proportion) '
                f'VALUES("{region}", {period}, "{row["comm"]}", "{tech}", "le", {prop})'
            ) # <= should, in theory, be least likely to cause infeasibility / numerical issues?
            curs.execute(sql)

    # DSDs (only affects electricity) TODO later?

    conn.commit()

    if config.params['build_test_model']:
        region_comms = set([tuple(rc) for rc in df_cef.reset_index()[['region','comm']].values])
        build_tester(region_comms)


def build_tester(region_comms):

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    for region in config.model_regions:
        sql = (
            'REPLACE INTO Region(region) '
            f'VALUES("{region}")'
        )
        curs.execute(sql)

    # TimePeriod
    for i, period in enumerate(config.model_periods):
        sql = (
            'REPLACE INTO TimePeriod(sequence, period, flag) '
            f'VALUES({i}, {period}, "f")'
        )
        curs.execute(sql)
        sql = (
            'REPLACE INTO TimeSeason(period, sequence, season) '
            f'VALUES({period}, 0, "S")'
        )
        curs.execute(sql)
        sql = (
            'REPLACE INTO TimeSegmentFraction(period, season, tod, segfrac) '
            f'VALUES({period}, "S", "D", 1)'
        )
        curs.execute(sql)
    sql = (
        'REPLACE INTO TimePeriod(sequence, period, flag) '
        f'VALUES({i+1}, {period+5}, "f")'
    )
    curs.execute(sql)

    # Time slices
    sql = (
        'REPLACE INTO SeasonLabel(season) '
        f'VALUES("S")'
    )
    curs.execute(sql)
    sql = (
        'REPLACE INTO TimeOfDay(sequence, tod) '
        f'VALUES(0, "D")'
    )
    curs.execute(sql)
    

    sql = (
        'REPLACE INTO Commodity(name, flag, description) '
        'VALUES("ethos", "s", "(PJ) dummy")'
    )
    curs.execute(sql)
    sql = (
        'REPLACE INTO Technology(tech, flag, sector, unlim_cap, annual) '
        'VALUES("IMPORT", "p", "import", 1, 1)'
    )
    curs.execute(sql)
    
    for region, comm in region_comms:
        sql = (
            'REPLACE INTO Efficiency(region, input_comm, tech, vintage, output_comm, efficiency) '
            f'VALUES("{region}", "ethos", "IMPORT", 2025, "{comm}", 1.0)'
        )
        curs.execute(sql)

        for period in config.model_periods:
            sql = (
                'REPLACE INTO CostVariable(region, period, tech, vintage, cost) '
                f'VALUES("{region}", {period}, "IMPORT", 2025, 1)'
            )
            curs.execute(sql)

    conn.commit()


if __name__ == "__main__":
    build()