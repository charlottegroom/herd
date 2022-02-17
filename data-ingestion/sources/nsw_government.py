'''Script to ingest data from NSW Government'''

import re
import requests
import pandas as pd
import pandera as pa
from datetime import datetime
from marshmallow.fields import Nested, String
from copy import deepcopy

from ingestion import (
    BaseIngest,
    logging,
    SourceSchema,
    IngestSchema,
    BASE_PROCESSED_SCHEMA
)

RESOURCES = {
    'tests_by_location': 'fb95de01-ad82-4716-ab9a-e15cf2c78556',
    'cases_by_location': '21304414-1ff1-4243-a5d2-f52778048b29',
    'cases_by_age_range': '24b34cb5-8b01-4008-9d93-d14cf5518aec',
}


class SourceSchema(SourceSchema):
    '''Schema for source'''
    resource_type = String(required=True)


class IngestSchema(IngestSchema):
    '''Schema for Ingest class config'''
    source = Nested(SourceSchema)


class Ingest(BaseIngest):

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = IngestSchema().load(cfg)

    def retrieve(self):
        '''Get raw data from NSW Government.
        '''
        resource_type = self.cfg['source']['resource_type']
        data = []
        def recursive_get_data(sql_statement=None):
            '''Recursive function to get data'''
            logging.info(f'Retrieving data from {resource_type}')
            resource_id = RESOURCES[resource_type]
            # Construct SQL statement
            if not sql_statement:
                sql_statement = f'''
                SELECT * FROM "{resource_id}"
                '''
            # Make request
            url = f'https://data.nsw.gov.au/data/api/3/action/datastore_search_sql?sql={sql_statement}'
            response = requests.get(url)
            response.raise_for_status()
            # Get json response
            j = response.json()
            records = j['result']['records']
            for record in records:
                yield record
            # Check if results are truncated
            if j['result'].get('records_truncated'):
                logging.debug('Result truncated, still retrieving...')
                # Get latest record
                last_record = records[-1]
                date_field = None
                for k in last_record.keys():
                    if 'date' in k:
                        date_field = k
                sql_statement = f'''
                SELECT * FROM "{resource_id}"
                WHERE {date_field} > '{last_record[date_field]}'
                '''
                for record in recursive_get_data(sql_statement):
                    yield record
        # Return dataframe
        data = list(recursive_get_data())
        return pd.DataFrame(data)

    def process(self, df):
        '''Processes raw data from NSW Government.
        '''
        # Drop fields
        df = df.drop(['_id', '_full_text'], axis=1)
        resource_type = self.cfg['source']['resource_type']
        logging.info(f'Retrieving data from {resource_type}')
        # Add fields
        df['state_name'] = 'New South Wales'
        df['state_code'] = 'NSW'
        df['country'] = 'Australia'
        # Rename columns
        df = df.rename(columns={
            'lhd_2010_code': 'lhd_code',
            'lhd_2010_name': 'lhd_name',
            'lga_code19': 'lga_code',
            'lga_name19': 'lga_name',
        })
        if resource_type == 'tests_by_location':
            # Rename fields
            df = df.rename(columns={
                'test_date': 'date',
                'test_count': 'tests',
            })
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.lhd_code,
                        x.lhd_name,
                        x.lga_code,
                        x.lga_name,
                        x.state_code,
                        x.state_name,
                        x.country,
                    ])
                ),
                axis=1
            )
        elif resource_type == 'cases_by_location':
            # Rename fields
            df = df.rename(columns={'notification_date': 'date'})
            # Add fields
            df['cases'] = 1
            df = df.groupby(by=[
                'date',
                'lhd_code',
                'lhd_name',
                'lga_code',
                'lga_name',
                'state_code',
                'state_name',
                'country',
            ])['cases'].count().reset_index()
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.lhd_code,
                        x.lhd_name,
                        x.lga_code,
                        x.lga_name,
                        x.state_code,
                        x.state_name,
                        x.country,
                    ])
                ),
                axis=1
            )
        elif resource_type == 'cases_by_age_range':
            # Rename fields
            df = df.rename(columns={'notification_date': 'date'})
            # Transform fields
            df['age_group'] = df['age_group'].apply(
                lambda x: tuple(re.findall(r'\d+', x))
            )
            # Add fields
            df['cases'] = 1
            df = df.groupby(
                ['date', 'age_group', 'state_code', 'state_name', 'country']
            )['cases'].count().reset_index()
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.age_group,
                        x.state_code,
                        x.state_name,
                        x.country,
                    ])
                ),
                axis=1
            )
        else:
            raise NotImplementedError(
                f'Processing for resource type {resource_type} not implemented.'
            )
        # Transform fields
        df['date'] = pd.to_datetime(df['date'])
        df = df.replace('None', None)
        # Return dataframe
        return df

    def validate(self, df):
        '''Validate data'''
        resource_type = self.cfg['source']['resource_type']
        logging.info(f'Validating data from {resource_type}')
        if resource_type == 'tests_by_location':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "lhd_code": pa.Column(str),
                "lhd_name": pa.Column(str),
                "lga_code": pa.Column(str),
                "lga_name": pa.Column(str),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                "dimensions_id": pa.Column(int),
                # Measures
                "tests": pa.Column(int, nullable=True, coerce=True),
            })
        elif resource_type == 'cases_by_location':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "lhd_code": pa.Column(str),
                "lhd_name": pa.Column(str),
                "lga_code": pa.Column(str),
                "lga_name": pa.Column(str),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                "dimensions_id": pa.Column(int),
                # Measures
                "cases": pa.Column(int, nullable=True, coerce=True),
            })
            df = schema(df)
        elif resource_type == 'cases_by_age_range':
            schema = deepcopy(BASE_PROCESSED_SCHEMA)
            schema.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                "age_group": pa.Column(str),
                "dimensions_id": pa.Column(int),
                # Measures
                "cases": pa.Column(int, nullable=True, coerce=True),
            })
            df = schema(df)
        return df
