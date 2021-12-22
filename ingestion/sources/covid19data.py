'''Data downloaded from COVID-19 Data GitHub repo'''

import requests
import pandas as pd
import pandera as pa
import io
from datetime import datetime
from marshmallow.fields import Nested, String

from ingestion.ingest import BaseIngest, logging
from ingestion.configuration import SourceSchema, IngestSchema

from .utils import STATE_NAMES


class SourceSchema(SourceSchema):
    '''Schema for source'''
    filename = String(required=True)


class IngestSchema(IngestSchema):
    '''Schema for Ingest class config'''
    source = Nested(SourceSchema)


class COVIDAUIngest(BaseIngest):
    '''Ingestion of COVID AU data'''

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = IngestSchema().load(cfg)

    def retrieve(self):
        '''Retrieve data'''
        # Parse config
        filename = self.cfg['source']['filename']
        logging.info(f'Retrieving data from {filename}')
        # Make request
        response = requests.get(
            f'https://raw.githubusercontent.com/M3IT/COVID-19_Data/master/Data/{filename}'
        )
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        df['date'] = pd.to_datetime(df['date'])
        # Return dataframe
        return df

    def process(self, df):
        '''Process and validate data'''
        filename = self.cfg['source']['filename']
        logging.info(f'Processing data from {filename}')
        if filename == 'COVID_AU_state.csv':
            # Rename fields
            df = df.rename(columns={
                'state': 'state_name',
                'state_abbrev': 'state_code',
            })
            # Add fields
            df['country'] = 'Australia'
        elif filename == 'COVID_AU_deaths.csv':
            # Rename fields
            df = df.rename(columns={
                'state': 'state_code',
                'gender': 'sex',
            })
            # Transform fields
            df['state_name'] = df['state_code'].apply(lambda x: STATE_NAMES[x])
            df['age_group'] = df.apply(
                lambda x: tuple(int(d) for d in x['age_bracket'].split('-')) \
                    if not pd.isnull(x['age_bracket']) else None,
                axis=1
            )
            df['sex'] = df['sex'].replace('*', '')
            # Add fields
            df['deaths'] = 1
            df = df.groupby(by=[
                'date',
                'state_name',
                'state_code',
                'age_group',
                'sex',
            ])['deaths'].count().reset_index()
            df['country'] = 'Australia'
        # Return dataframe
        return df

    def validate(self, df):
        filename = self.cfg['source']['filename']
        logging.info(f'Validating data from {filename}')
        if filename == 'COVID_AU_state.csv':
            schema = pa.DataFrameSchema({
                # Dimensions
                "date": pa.Column(datetime),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                # Measures
                "confirmed": pa.Column(int, nullable=True, coerce=True),
                "deaths": pa.Column(int, nullable=True, coerce=True),
                "tests": pa.Column(int, nullable=True, coerce=True),
                "positives": pa.Column(int, nullable=True, coerce=True),
                "recovered": pa.Column(int, nullable=True, coerce=True),
                "hosp": pa.Column(int, nullable=True, coerce=True),
                "icu": pa.Column(int, nullable=True, coerce=True),
                "vent": pa.Column(int, nullable=True, coerce=True),
                "vaccines": pa.Column(int, nullable=True, coerce=True),
            },
            # Filter out columns not specified
            strict='filter',
            )
            df = schema(df)
        elif filename == 'COVID_AU_deaths.csv':
            schema = pa.DataFrameSchema({
                # Dimensions
                "date": pa.Column(datetime),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                "age_group": pa.Column(str),
                "sex": pa.Column(str),
                # Measures
                "deaths": pa.Column(int, nullable=True, coerce=True),
            },
            # Filter out columns not specified
            strict='filter',
            )
            df = schema(df)
        return df
