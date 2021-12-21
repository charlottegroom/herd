'''Script to ingest data from NSW Government'''

import re
from marshmallow.fields import String
import requests
import pandas as pd

from ingestion import Source, SourceSchema


RESOURCES = {
    'tests_by_location': 'fb95de01-ad82-4716-ab9a-e15cf2c78556',
    'cases_by_location': '21304414-1ff1-4243-a5d2-f52778048b29',
    'cases_by_age_range': '24b34cb5-8b01-4008-9d93-d14cf5518aec',
}


class SourceSchema(SourceSchema):
    '''Schema for source'''
    resource_type = String(required=True)


class NSWGovSource(Source):

    def __init__(self, cfg):
        super().__init__(cfg)
        self.cfg = SourceSchema().load(cfg)

    def retrieve(self):
        '''Get raw data from NSW Government.
        '''
        resource_type = self.cfg['resource_type']
        def recursive_get_data(sql_statement=None):
            '''Recursive function to get data'''
            print(f'Retrieving data from {resource_type}')
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
                print('Result truncated, still retrieving...')
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
        # Return docs
        return list(recursive_get_data())

    def process(self):
        '''Processes raw data from NSW Government.
        '''
        # Retrieve data
        df = pd.DataFrame(self.retrieve())
        # Drop fields
        df = df.drop(['_id', '_full_text'], axis=1)
        resource_type = self.cfg['resource_type']
        if resource_type == 'tests_by_location':
            # Rename fields
            df = df.rename(columns={'test_date': 'date'})
        elif resource_type == 'cases_by_location':
            # Rename fields
            df = df.rename(columns={'notification_date': 'date'})
            # Add fields
            df['case_count'] = 1
            df = df.groupby(by=[
                'date',
                'lhd_2010_code',
                'lhd_2010_name',
                'lga_code19',
                'lga_name19'
            ])['case_count'].count().reset_index()
        elif resource_type == 'cases_by_age_range':
            # Rename fields
            df = df.rename(columns={'notification_date': 'date'})
            # Transform fields
            df['age_group'] = df['age_group'].apply(
                lambda x: tuple(re.findall(r'\d+', x))
            )
            # Add fields
            df['case_count'] = 1
            df = df.groupby(
                ['date', 'age_group']
            )['case_count'].count().reset_index()
        else:
            raise NotImplementedError(
                f'Processing for resource type {resource_type} not implemented.'
            )
        # Add fields
        df['date'] = pd.to_datetime(df['date'])
        df['state_name'] = 'New South Wales'
        df['state_code'] = 'NSW'
        # Transform fields
        df = df.replace('None', None)
        # Rename columns
        df = df.rename(columns={
            'lhd_2010_code': 'lhd_code',
            'lhd_2010_name': 'lhd_name',
            'lga_code19': 'lga_code',
            'lga_name19': 'lga_name',
        })
        # Return docs
        return df.to_dict('records')
