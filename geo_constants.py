import os
from dotenv import load_dotenv
load_dotenv()


class DomesticISO:
    VALID_STATE_CODES = [
        'AK',
        'AL',
        'AR',
        'AZ',
        'CA',
        'CO',
        'CT',
        'DC',
        'DE',
        'FL',
        'GA',
        'HI',
        'IA',
        'ID',
        'IL',
        'IN',
        'KS',
        'KY',
        'LA',
        'MA',
        'MD',
        'ME',
        'MI',
        'MN',
        'MO',
        'MS',
        'MT',
        'NC',
        'ND',
        'NE',
        'NH',
        'NJ',
        'NM',
        'NV',
        'NY',
        'OH',
        'OK',
        'OR',
        'PA',
        'RI',
        'SC',
        'SD',
        'TN',
        'TX',
        'UT',
        'VA',
        'VT',
        'WA',
        'WI',
        'WV',
        'WY',
    ]


class JerryGarcia:
    COUNTRIES_PLAYED = [
        'Canada',
        'Denmark',
        'Egypt',
        'France',
        'Germany',
        'Jamaica',
        'Luxembourg',
        'Netherlands',
        'Spain',
        'Sweden',
        'United Kingdom'
    ]
    OUTPUT_COLUMNS = [
        'jb_name',
        'jb_street',
        'city_ascii',
        'jb_zip',
        'state_name',
        'country',
        'iso2',
        'iso3',
        'population',
        'lat',
        'lng',
        'jb_geo',
        'jb_google_map',
        'jb_gd_count',
        'jb_jg_count',
        'jb_id',
    ]


class Scrapy:
    KEY = os.getenv('ZYTE_KEY')
    HTTP = os.getenv('ZYTE_HTTP')
    HTTPS = os.getenv('ZYTE_HTTPS')
