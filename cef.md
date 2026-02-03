
# CEF Data

 Data obtained from the Canadian Energy Regulator Canada's Energy Futures report. Data is considered low resolution as the sector is treated as a black box.

# Commercial Sector
        
## Commodity

| name      | description                                  | type               | units   |
|:----------|:---------------------------------------------|:-------------------|:--------|
| C\_h2     | (PJ) hydrogen (commercial)                   | annual commodity   | nan     |
| C\_elc    | (PJ) electricity (commercial)                | annual commodity   | nan     |
| C\_oil    | (PJ) refined petroleum products (commercial) | annual commodity   | nan     |
| C\_ng     | (PJ) natural gas in the commercial sector    | annual commodity   | nan     |
| C\_bio    | (PJ) biofuel (commercial)                    | annual commodity   | nan     |
| C\_D\_com | (PJ) commercial energy demand                | demand commodity   | PJ      |
| C\_elc    | (PJ) electricity in the commercial sector    | physical commodity | nan     |

## Technology

| tech                 | description                                                    |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------|:---------------------------------------------------------------|------------:|---------:|----------:|----------:|-------:|
| C\_COM               | all commercial energy demands                                  |           1 |        1 |         0 |         0 |      0 |
| F\_C\_OIL            | Oil distribution from fuel sector to commercial sector         |           1 |        1 |         0 |         0 |      0 |
| F\_C\_NG             | Natural gas distribution from fuel sector to commercial sector |           1 |        1 |         0 |         0 |      0 |
| E\_C\_ELC            | Electricity distribution to commercial sector                  |           1 |        0 |         0 |         0 |      0 |
| F\_C\_BIO            | bioenergy distribution from fuel sector to commercial sector   |           1 |        1 |         0 |         0 |      0 |
| F\_C\_H2             | Hydrogen distribution from fuel sector to commercial sector    |           1 |        1 |         0 |         0 |      0 |

# Residential Sector
        
## Commodity

| name                 | description                                                                  | type               | units    |
|:---------------------|:-----------------------------------------------------------------------------|:-------------------|:---------|
| R\_ng                | (PJ) natural gas (residential)                                               | annual commodity   | nan      |
| R\_bio               | (PJ) biofuel (residential)                                                   | annual commodity   | nan      |
| R\_elc               | (PJ) electricity (residential)                                               | annual commodity   | nan      |
| R\_h2                | (PJ) hydrogen (residential)                                                  | annual commodity   | nan      |
| R\_oil               | (PJ) refined petroleum products (residential)                                | annual commodity   | nan      |
| R\_D\_res            | (PJ) residential energy demand                                               | demand commodity   | PJ       |

## Technology

| tech                       | description                                                                                           |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:---------------------------|:------------------------------------------------------------------------------------------------------|------------:|---------:|----------:|----------:|-------:|
| R\_RES                     | all residential energy demands                                                                        |           1 |        1 |         0 |         0 |      0 |
| F\_R\_OIL                  | Oil distribution from fuel sector to residential sector                                               |           1 |        1 |         0 |         0 |      0 |
| F\_R\_NG                   | Natural gas distribution from fuel sector to residential sector                                       |           1 |        1 |         0 |         0 |      0 |
| E\_R\_ELC                  | Electricity distribution to residential sector                                                        |           1 |        0 |         0 |         0 |      0 |
| F\_R\_BIO                  | bioenergy distribution from fuel sector to residential sector                                         |           1 |        1 |         0 |         0 |      0 |
| F\_R\_H2                   | Hydrogen distribution from fuel sector to residential sector                                          |           1 |        1 |         0 |         0 |      0 |

# Transportation Sector
    
## Commodity

| name                    | description                                                                     | type               | units   |
|:------------------------|:--------------------------------------------------------------------------------|:-------------------|:--------|
| T\_hfo                  | (PJ) heavy fuel oil (transportation)                                            | annual commodity   | nan     |
| T\_jtf                  | (PJ) aviation fuel (transportation)                                             | annual commodity   | nan     |
| T\_h2                   | (PJ) hydrogen (transportation)                                                  | annual commodity   | nan     |
| T\_dsl                  | (PJ) diesel (transportation)                                                    | annual commodity   | nan     |
| T\_gsl                  | (PJ) gasoline (transportation)                                                  | annual commodity   | nan     |
| T\_bio                  | (PJ) biofuel (transportation)                                                   | annual commodity   | nan     |
| T\_elc                  | (PJ) electricity (transportation)                                               | annual commodity   | nan     |
| T\_D\_trp               | (PJ) transportation energy demand                                               | demand commodity   | PJ      |

## Technology

| tech       | description                                                                                              |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:-----------|:---------------------------------------------------------------------------------------------------------|------------:|---------:|----------:|----------:|-------:|
| T\_TRP     | all transportation energy demands                                                                        |           1 |        1 |         0 |         0 |      0 |
| F\_T\_BIO  | bioenergy distribution from fuel sector to transportation sector                                         |           1 |        1 |         0 |         0 |      0 |
| E\_T\_ELC  | Electricity distribution to transportation sector                                                        |           1 |        0 |         0 |         0 |      0 |
| F\_T\_H2   | Hydrogen distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |
| F\_T\_HFO  | Heavy fuel oil distribution from fuel sector to transportation sector                                    |           1 |        1 |         0 |         0 |      0 |
| F\_T\_GSL  | Gasoline distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |
| F\_T\_DSL  | Diesel distribution from fuel sector to transportation sector                                            |           1 |        1 |         0 |         0 |      0 |
| F\_T\_JTF  | Jet fuel distribution from fuel sector to transportation sector                                          |           1 |        1 |         0 |         0 |      0 |


# Industrial Sector
        
## Commodity

| name           | description                                             | type               | units   |
|:---------------|:--------------------------------------------------------|:-------------------|:--------|
| I\_ng          | (PJ) natural gas (industrial)                           | annual commodity   | nan     |
| I\_hfo         | (PJ) refined petroleum products (industrial)            | annual commodity   | nan     |
| I\_bio         | (PJ) biofuel (industrial)                               | annual commodity   | nan     |
| I\_elc         | (PJ) electricity (industrial)                           | annual commodity   | nan     |
| I\_h2          | (PJ) hydrogen (industrial)                              | annual commodity   | nan     |
| I\_D\_ind      | (PJ) industrial energy demand                           | demand commodity   | PJ      |

## Technology

| tech        | description                                                              |   unlim_cap |   annual |   reserve |   curtail |   flex |
|:------------|:-------------------------------------------------------------------------|------------:|---------:|----------:|----------:|-------:|
| I\_IND      | all industrial energy demands                                            |           1 |        1 |         0 |         0 |      0 |
| F\_I\_BIO   | bioenergy distribution from fuel sector to industrial sector             |           1 |        1 |         0 |         0 |      0 |
| E\_I\_ELC   | Electricity distribution to industrial sector                            |           1 |        0 |         0 |         0 |      0 |
| F\_I\_H2    | Hydrogen distribution from fuel sector to industrial sector              |           1 |        1 |         0 |         0 |      0 |
| F\_I\_NG    | Natural gas distribution from fuel sector to industrial sector           |           1 |        1 |         0 |         0 |      0 |
| F\_I\_HFO   | Heavy fuel oil distribution from fuel sector to industrial sector        |           1 |        1 |         0 |         0 |      0 |

