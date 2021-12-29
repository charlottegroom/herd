'''Data downloaded from COVID-19 Data GitHub repo'''

import requests
import pandas as pd
import pandera as pa
import io
from datetime import datetime
from marshmallow.fields import Nested, String

from ingestion import (
    BaseIngest,
    logging,
    SourceSchema,
    IngestSchema,
    BASE_PROCESSED_SCHEMA
)

from .utils import STATE_NAMES


class SourceSchema(SourceSchema):
    '''Schema for source'''
    filename = String(required=True)


class IngestSchema(IngestSchema):
    '''Schema for Ingest class config'''
    source = Nested(SourceSchema)


class Ingest(BaseIngest):
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
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.state_name,
                        x.state_code,
                        x.country,
                    ])
                ),
                axis=1
            )
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
            df['sex'] = df['sex'].apply(lambda x: x.replace('*', '') if not pd.isnull(x) else 'Unknown')
            df['age'] = df.apply(lambda x: x['age'] if not pd.isnull(x['age']) else x['age_group'][0] if x['age_group'] is not None else None, axis=1)
            # Add fields
            df['deaths'] = 1
            df['country'] = 'Australia'
            df['dimensions_id'] = df.apply(
                lambda x: hash(
                    tuple([
                        x.date,
                        x.state_name,
                        x.state_code,
                        # x.age_group,
                        x.age,
                        x.sex,
                        x.country
                    ])
                ),
                axis=1
            )
        # Return dataframe
        return df

    def validate(self, df):
        filename = self.cfg['source']['filename']
        logging.info(f'Validating data from {filename}')
        if filename == 'COVID_AU_state.csv':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
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
                "hosp_cum": pa.Column(int, nullable=True, coerce=True),
                "icu_cum": pa.Column(int, nullable=True, coerce=True),
                "vent_cum": pa.Column(int, nullable=True, coerce=True),
            })
            df = schema(df)
        elif filename == 'COVID_AU_deaths.csv':
            schema = BASE_PROCESSED_SCHEMA.add_columns({
                # Dimensions
                "date": pa.Column(datetime),
                "state_name": pa.Column(str),
                "state_code": pa.Column(str),
                "country": pa.Column(str),
                "age_group": pa.Column(object, nullable=True, coerce=True),
                "sex": pa.Column(str, nullable=True, coerce=True),
                # Measures
                "deaths": pa.Column(int, nullable=True, coerce=True),
                "age": pa.Column(float, nullable=True, coerce=True),
            })
            df = schema(df)
        return df
