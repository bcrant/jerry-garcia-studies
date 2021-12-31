import pandas as pd
import pycountry


def get_us_city_population():
    # CENSUS
    census_df = pd.read_csv('data/us_city_population.csv', encoding="ISO-8859-1")

    population_df = census_df[[
        'SUMLEV',
        'STNAME',
        'NAME',
        'CENSUS2010POP'
    ]].loc[census_df['SUMLEV'] >= 50]

    population_df['CITY'] = list(
        city.strip('city|town|borough|village').rstrip()
        for city in population_df['NAME'].tolist()
        if city.rsplit(' ') is not None
    )

    # VENUES
    venue_df = pd.read_csv('data/JerryBase_dom_enriched.csv', dtype=str)

    population_list = list()
    for idx in range(0, len(venue_df)):
        city, state = venue_df[['2021_city', '2021_state_name']].iloc[idx]
        city_population_df = population_df.loc[(population_df['STNAME'] == state) & (population_df['CITY'] == city)]
        population = [i for i in city_population_df['CENSUS2010POP'].tolist() if len(i) > 3]
        population_value = population.pop() if len(population) > 0 else 0
        population_list.append(population_value)
    venue_df['2021_city_population'] = population_list

    venue_df.to_csv('./JerryBase_dom_enriched.csv', index=0)


def test_iso():
    df = pd.read_csv('data/JerryBase_intl_enriched.csv', dtype=str)

    state_code_list = list()
    for idx in range(0, len(df)):
        _id, _sn, _cc = df[[
            'jb_id',
            '2021_state_name',
            '2021_country_iso2'
        ]].iloc[idx]

        state_code = list(
            sd.code
            for sd in list(pycountry.subdivisions)
            if sd.name == _sn and sd.country_code == _cc
        ).pop()
        state_code_list.append(state_code)
        print(_id, _sn, state_code)

    df['2021_state_code'] = state_code_list

    df.to_csv('./JerryBase_intl_enriched.csv', index=0)


if __name__ == '__main__':
    get_us_city_population()
