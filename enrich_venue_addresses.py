import requests
import pandas as pd
import usaddress
import urllib3
import urllib.parse
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fake_useragent import UserAgent
from geo_constants import DomesticISO, JerryGarcia, Scrapy
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
ua = UserAgent()


def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def executor():
    jerry_shows_df = load_jb_data()
    intl_jerry_shows_df = enrich_international_shows(jerry_shows_df)
    dom_jerry_shows_df = enrich_domestic_shows(jerry_shows_df)
    get_address_from_google()
    format_new_addresses()


def load_jb_data():
    #
    # Load JerryBase Data
    #
    print('Loading JerryBase data...')
    shows_df = pd.read_csv('data/venue_and_show_count_data.csv')
    shows_df.columns = [str('jb_' + c) for c in shows_df.columns]
    return shows_df


def enrich_international_shows(shows_df):
    #
    # [ INTERNATIONAL ] Enrich JerryBase data with International ISO Codes
    #
    print('\n\n')
    print('Begin enriching JerryBase data with International ISO-Codes...')
    #
    # Identify International Shows from invalid state codes in JerryBase Data
    #
    print('Getting unique state codes from JerryBase data...')
    shows_in_states = sorted(str(i) for i in shows_df['jb_state'].unique())

    print('Identifying invalid shows with state codes from JerryBase data...')
    invalid_us_states = tuple(set(shows_in_states) - set(DomesticISO.VALID_STATE_CODES))
    non_us_shows_df = shows_df.loc[shows_df['jb_state'].isin(invalid_us_states)].copy()

    print('Splitting poorly formatted city and state fields from JerryBase data...')
    city_clean = list()
    city_clean_state = list()
    for i in non_us_shows_df['jb_city']:
        if ',' not in i:
            city_clean.append(i)
            city_clean_state.append(None)
        else:
            city_clean.append(i.split(',')[0].strip())
            city_clean_state.append(i.split(',')[1].strip())

    non_us_shows_df['city_clean'] = city_clean
    non_us_shows_df['state_clean'] = city_clean_state

    print('Loading World Cities by Population...')
    all_major_cities = pd.read_csv('data/world_cities_by_population.csv')\
        .rename(columns={'admin_name': 'state_name'})

    # Identify Country Names and ISO-2 Codes
    print('Limiting third-party geo data to countries that Jerry played, sorting and de-duping...')
    intl_major_cities = all_major_cities.loc[all_major_cities['country'].isin(JerryGarcia.COUNTRIES_PLAYED)].copy()
    intl_major_cities.sort_values(by=['city', 'population'], ascending=False, inplace=True)
    intl_major_cities.drop_duplicates(subset=['city'], keep='first', inplace=True)

    print('Joining major international cities with JerryBase data...')
    enriched_shows_df = non_us_shows_df.merge(
        intl_major_cities,
        how='left',
        left_on='city_clean',
        right_on='city_ascii'
    )[JerryGarcia.OUTPUT_COLUMNS]
    print(enriched_shows_df.head())

    return enriched_shows_df


def enrich_domestic_shows(shows_df):
    #
    # [ DOMESTIC ] Enrich JerryBase data with US ISO Codes
    #
    print('\n\n')
    print('Begin enriching JerryBase data with United States ISO-Codes...')

    print('Identifying valid shows with state codes from JerryBase data...')
    domestic_shows_df = shows_df.loc[shows_df['jb_state'].isin(DomesticISO.VALID_STATE_CODES)].copy()
    print('domestic_shows_df', len(domestic_shows_df))

    enriched_shows_df = pd.read_csv('data/JerryBase_VenueAddresses.csv')
    print('enriched_shows_df', len(enriched_shows_df))

    dom_df = pd.merge(domestic_shows_df, enriched_shows_df, on='jb_id', how='left')
    dom_df.to_csv('./result.csv')

    return domestic_shows_df


def get_address_from_google(shows_df):
    # Get shows with no Street address but that have a Google Maps URL
    no_street_df = shows_df.loc[shows_df['jb_street'].isnull()].copy().reset_index(drop=True)
    df = no_street_df.loc[~no_street_df['jb_google_map'].isnull()].reset_index(drop=True)
    df.to_csv('./df_before.csv')

    intl_dirty_addy_list = pd.read_csv('./data/intl_dirty_addys.csv').tolist()

    enriched_addresses_list = list()
    for addy in intl_dirty_addy_list:
        url = f'https://www.google.com/search?q={urllib.parse.quote(addy)}'
        print('Address: ', addy)
        print('Making Request: ', url)

        headers = {
            "Accept-Language": "en-US, en;q=0.5",
            "User-Agent": ua.random
        }
        proxies = {
            "http": Scrapy.HTTP,
            "https": Scrapy.HTTPS,
        }
        cert = './zyte-proxy-ca.crt'

        try:
            r = requests_retry_session().get(
                url,
                headers=headers,
                proxies=proxies,
                verify=cert,
                stream=True,
                allow_redirects=True,
                timeout=30
            )

            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)

        print('Response: ', r.status_code)
        html_soup = BeautifulSoup(r.content, 'html.parser')

        div_soup = html_soup.find_all('div', {'class': 'QsDR1c'})
        div_text_list = list(
            div.find('span', {'class': 'LrzXr'}).text
            for div
            in div_soup
            if div.find('span', {'class': 'LrzXr'})
            is not None
        )
        print('div_text_list', div_text_list)

        enriched_addresses_list.append([addy, div_text_list])

    intl_dirty_df = pd.DataFrame(enriched_addresses_list, columns=['address', 'result'])
    intl_dirty_df.to_csv('data/intl_less_dirty_addys.csv')


def format_new_addresses():
    intl_dirty_addy_list = pd.read_csv('./data/intl_less_dirty_addys.csv').tolist()

    addy_dict = dict()
    for addy in intl_dirty_addy_list:
        parsed_addy_dict = dict()
        parsed_addy = usaddress.parse(addy)
        for i in parsed_addy:
            v = i[0]
            k = i[1]
            parsed_addy_dict[k] = v
        addy_dict[addy] = parsed_addy_dict.get('ZipCode')


if __name__ == "__main__":
    executor()
