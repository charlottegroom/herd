'''Base class for data source ingestion'''

from datetime import datetime
import os
from sqlalchemy import create_engine
import pandera as pa
import logging

from .configuration import IngestSchema


class BaseIngest():
    '''Base data ingestion class'''
    def __init__(self, cfg):
        self.cfg = IngestSchema().load(cfg)

    def retrieve(self):
        '''Retrieve raw data from source.
        Returns dataframe.
        '''
        raise NotImplementedError('Data not retrieved.')

    def process(self, df):
        '''Process raw data.
        Returns dataframe.
        '''
        return df

    def validate(self, df):
        '''Validate processed data'''
        # Infer schema
        schema = pa.infer_schema(df)
        # Validate data
        return schema(df)

    def save(self, df, name=None):
        '''Save to a specified data sink.

        kwargs:
            name (str): Name of the sink collection to save to.
        '''
        sink_cfg = self.cfg.get('sink')
        if sink_cfg is None:
            logging.warning('No sink configuration provided.')
            return df
        # Add metadata
        df['saved_date'] = datetime.utcnow().date()
        df = df.drop_duplicates()
        # Parse config
        if not name:
            name = sink_cfg['name']
        mode = sink_cfg.get('mode')
        chunksize = sink_cfg.get('chunksize')
        type = sink_cfg.get('type')
        logging.info(f'Saving data to {type}...')
        # Save to csv
        if type == 'csv':
            # Ensure csv file name
            if name[-4:] != '.csv':
                name += '.csv'
            mode = 'a' if mode == 'append' else 'w'
            df.to_csv(name, mode=mode, chunksize=chunksize)
        # Save to PostgreSQL
        elif type == 'postgres':
            # Connect to database
            uri = os.environ.get('POSTGRESQL')
            engine = create_engine(uri)
            df.to_sql(name, engine, if_exists=mode, chunksize=chunksize)
