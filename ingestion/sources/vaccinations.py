'''Retrieve and process vaccination data'''

import requests
import pandas as pd
import pandera as pa
import re
from copy import deepcopy
from marshmallow.fields import Nested, String
from datetime import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool
from copy import deepcopy

from ingestion import (
    BaseIngest,
    logging,
    SourceSchema,
    IngestSchema,
    BASE_PROCESSED_SCHEMA
)

from .utils import STATE_CODES, STATE_NAMES, LGA_LHD_MAP

BASE_URL = 'https://www.health.gov.au/'


class SourceSchema(SourceSchema):
    '''Schema for source'''
    collection = String(required=True)

class IngestSchema(IngestSchema):
    '''Schema for Ingest class config'''
    source = Nested(SourceSchema)


class Ingest(BaseIngest):

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = IngestSchema().load(cfg)

    def retrieve(self):
        '''Get raw vaccination data'''
        collection = self.cfg['source']['collection']
        url = BASE_URL + 'resources/collections/' + collection
        logging.info(f'Retrieving data from {collection}')
        response = requests.get(url)
        soup = BeautifulSoup(response.text, features="html.parser")
        table = soup.find('div', {'class': 'paragraphs-items'})
        a_tags = table.findAll('a')
        args = []
        # Get all Excel file links
        for a in a_tags:
            response = requests.get(BASE_URL + a['href'])
            soup = BeautifulSoup(response.text, features="html.parser")
            excel_file = soup.find('a', {
                'class': 'health-file__link',
                'data-filetype': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            })
            args.append((excel_file['href'], collection))
        # Download datasets in parallel
        with Pool(5) as p:
            return pd.concat(p.starmap(self._download_dataset, args))

    def _download_dataset(self, link, collection):
        '''Retrieves individual dataset given a link and collection name.

        Args:
            link (str): Link to the dataset.
            collection (str): Name of the dataset to handled specific download.
        '''
        logging.debug(f'Downloading file: {link}')
        if collection == 'covid-19-vaccination-vaccination-data':
            _df = pd.read_excel(link)
            _df = _df.dropna()
            _df = _df.set_index('Measure Name')
            _df = _df.T
        elif collection == 'covid-19-vaccination-geographic-vaccination-rates-lga':
            _df = pd.read_excel(link, header=8)
            _df = _df.drop(['Remoteness'], axis=1)
            _df.columns = ['lga_name', 'state_name', 'vax_1_%_15+', 'vax_2_%_15+', 'population_15+']
        # Get date
        date = datetime.strptime(
            re.search(r'\d{1,2}-\w+-\d{4}', link)[0],
            "%d-%B-%Y"
        )
        _df['date'] = date
        logging.debug(f'Download done!: {date}')
        return _df

    def process(self, df):
        '''Process raw vaccination data'''
        collection = self.cfg['source']['collection']
        logging.info(f'Processing data from {collection}')
        if collection == 'covid-19-vaccination-vaccination-data':
            docs = []
            for doc in df.to_dict('records'):
                for state in STATE_CODES.values():
                    _doc = {}
                    # Calculate fields
                    _doc['date'] = doc['date']
                    _doc['state_name'] = STATE_NAMES[state]
                    _doc['state_code'] = state
                    _doc['country'] = 'Australia'
                    population = doc[f'{state} - Population 16 and over']
                    vax_1_dose = doc[f'{state} - Residence state - Number of people 50 and over with 1 dose']
                    vax_2_dose = doc[f'{state} - Residence state - Number of people 16 and over fully vaccinated']
                    _doc['vax_1_dose'] = vax_1_dose
                    _doc['vax_2_dose'] = vax_2_dose
                    _doc['vax_1_%'] = vax_1_dose/population
                    _doc['vax_2_%'] = vax_2_dose/population
                    _doc['population'] = population
                    docs.append(_doc)
                for age_group in ['16-19', '20-24', '25-29', '30-34', '35-39',
                    '40-44', '45-49', '50-54', '55-59', '60-64', '65-69',
                    '70-74', '75-79', '80-84', '85-89', '90-94', '95+']:
                    _doc = {}
                    # Calculate fields
                    _doc['date'] = doc['date']
                    _doc['country'] = 'Australia'
                    _doc['age_group'] = tuple(
                        int(k) for k in age_group.replace('+', '').strip().\
                            split('-')
                    )
                    for sex in ['M', 'F']:
                        __doc = deepcopy(_doc)
                        population = doc[
                            f'Age group - {age_group} - {sex} - Population'
                        ]
                        vax_1_dose = doc[
                            f'Age group - {age_group} - {sex} - Number of people with 1 dose'
                        ]
                        vax_2_dose = doc[
                            f'Age group - {age_group} - {sex} - Number of people fully vaccinated'
                        ]
                        __doc['sex'] = 'Female' if sex == 'F' else 'Male'
                        __doc['vax_1_dose'] = vax_1_dose
                        __doc['vax_2_dose'] = vax_2_dose
                        __doc['vax_1_%'] = vax_1_dose/population
                        __doc['vax_2_%'] = vax_2_dose/population
                        __doc['population'] = population
                        docs.append(__doc)
            df = pd.DataFrame(docs)
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.state_name,
                        x.state_code,
                        x.country,
                        x.age_group,
                        x.sex,
                    ])
                ),
                axis=1
            )
        elif collection == 'covid-19-vaccination-geographic-vaccination-rates-lga':
            # Transform fields
            df = df.replace('N/A', None)
            df['vax_1_%_15+'] = df['vax_1_%_15+'].apply(
                lambda x: float(re.sub(r'[>%]', '', str(x)))
            )
            df['vax_2_%_15+'] = df['vax_2_%_15+'].apply(
                lambda x: float(re.sub(r'[>%]', '', str(x)))
            )
            df['state_code'] = df['state_name'].apply(
                lambda x: STATE_CODES.get(x)
            )
            df['population_15+'] = df['population_15+'].apply(
                lambda x: re.sub("[^0-9^.]", "", str(x))
            )
            df['population_15+'] = df['population_15+'].apply(
                lambda x: float(x) if x else None
            )
            df['vax_1_dose_15+'] = df.apply(
                lambda x: x['vax_1_%_15+']*float(x['population_15+'])/100,
                axis=1
            )
            df['vax_2_dose_15+'] = df.apply(
                lambda x: x['vax_2_%_15+']*float(x['population_15+'])/100,
                axis=1
            )
            df['vax_1_dose_15+'] = df['vax_1_dose_15+'].apply(
                lambda x: round(x) if not pd.isnull(x) else None
            )
            df['vax_2_dose_15+'] = df['vax_2_dose_15+'].apply(
                lambda x: round(x) if not pd.isnull(x) else None
            )
            # Add fields
            df['country'] = 'Australia'
            df = pd.merge(df, LGA_LHD_MAP)
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.lhd_code,
                        x.lhd_name,
                        x.lga_code,
                        x.lga_name,
                        x.state_name,
                        x.state_code,
                        x.country,
                    ])
                ),
                axis=1
            )
        # Return dataframe
        return df

    def validate(self, df):
        '''Validate data'''
        collection = self.cfg['source']['collection']
        logging.info(f'Validating data from {collection}')
        if collection == 'covid-19-vaccination-vaccination-data':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "state_name": pa.Column(str, nullable=True),
                "state_code": pa.Column(str, nullable=True),
                "country": pa.Column(str, nullable=True),
                "age_group": pa.Column(str, nullable=True),
                "sex": pa.Column(str, nullable=True),
                "dimensions_id": pa.Column(int),
                # Measures
                "vax_1_dose": pa.Column(int, nullable=True, coerce=True),
                "vax_2_dose": pa.Column(int, nullable=True, coerce=True),
                "vax_1_%": pa.Column(float, nullable=True, coerce=True),
                "vax_2_%": pa.Column(float, nullable=True, coerce=True),
                "population": pa.Column(int, nullable=True, coerce=True),
            })
            df = schema(df)
        elif collection == 'covid-19-vaccination-geographic-vaccination-rates-lga':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "lhd_code": pa.Column(str, nullable=True),
                "lhd_name": pa.Column(str, nullable=True),
                "lga_code": pa.Column(str, nullable=True),
                "lga_name": pa.Column(str, nullable=True),
                "state_name": pa.Column(str, nullable=True),
                "state_code": pa.Column(str, nullable=True),
                "country": pa.Column(str, nullable=True),
                "dimensions_id": pa.Column(int),
                # Measures
                "vax_1_dose_15+": pa.Column(float, nullable=True, coerce=True),
                "vax_2_dose_15+": pa.Column(float, nullable=True, coerce=True),
                "vax_1_%_15+": pa.Column(float, nullable=True, coerce=True),
                "vax_2_%_15+": pa.Column(float, nullable=True, coerce=True),
                "population_15+": pa.Column(float, nullable=True, coerce=True),
            })
            df = schema(df)
        return df
