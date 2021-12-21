'''Data downloaded from COVID-19 Data GitHub repo'''

from marshmallow.fields import String
import requests
import pandas as pd
from datetime import datetime
import io
from .utils import STATE_NAMES
from ingestion import Source, SourceSchema


class SourceSchema(SourceSchema):
    '''Schema for source'''
    filename = String(required=True)


class COVIDAUSource(Source):
    '''Ingestion of COVID AU data'''

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = SourceSchema().load(cfg)

    def retrieve(self):
        '''Retrieve data'''
        # Parse config
        filename = self.cfg['filename']
        # Make request
        response = requests.get(
            f'https://raw.githubusercontent.com/M3IT/COVID-19_Data/master/Data/{filename}'
        )
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        df['date'] = pd.to_datetime(df['date'])
        # Return docs
        return df.to_dict('records')

    def process(self):
        '''Process data'''
        # Retrieve data
        df = pd.DataFrame(self.retrieve())
        filename = self.cfg['filename']
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
            df['death_count'] = 1
            df = df.groupby(by=[
                'date',
                'state_name',
                'state_code',
                'age_group',
                'sex',
            ])['death_count'].count().reset_index()
            df['country'] = 'Australia'
        # Return docs
        return df.to_dict('records')
