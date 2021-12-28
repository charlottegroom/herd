'''Base class for data source ingestion'''

from datetime import datetime, date, time, timedelta
import os
import sqlalchemy
from sqlalchemy import create_engine, ARRAY
import pandera as pa
import logging
import pandas.api.types as ptypes

from .configuration import IngestSchema

_type_py2sql_dict = {
 int: sqlalchemy.sql.sqltypes.BigInteger,
 str: sqlalchemy.sql.sqltypes.Unicode,
 float: sqlalchemy.sql.sqltypes.Float,
 datetime: sqlalchemy.sql.sqltypes.DateTime,
 bytes: sqlalchemy.sql.sqltypes.LargeBinary,
 bool: sqlalchemy.sql.sqltypes.Boolean,
 date: sqlalchemy.sql.sqltypes.Date,
 time: sqlalchemy.sql.sqltypes.Time,
 timedelta: sqlalchemy.sql.sqltypes.Interval,
 list: sqlalchemy.sql.sqltypes.ARRAY,
 tuple: sqlalchemy.sql.sqltypes.ARRAY,
 dict: sqlalchemy.sql.sqltypes.JSON
}


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
        _type = sink_cfg.get('type')
        logging.info(f'Saving data to {_type}...')
        # Save to csv
        if _type == 'csv':
            # Ensure csv file name
            if name[-4:] != '.csv':
                name += '.csv'
            mode = 'a' if mode == 'append' else 'w'
            df.to_csv(name, mode=mode, chunksize=chunksize)
        # Save to PostgreSQL
        elif _type == 'postgres':
            # Connect to database
            uri = os.environ.get('POSTGRESQL')
            engine = create_engine(uri)
            # Convert dtypes
            dtypes = {}
            for col in df.columns:
                if ptypes.is_object_dtype(df[col].dtype):
                    _df = df[col].dropna()
                    x = _df.iloc[0]
                    if isinstance(x, tuple):
                        t = _type_py2sql_dict.get(type(x[0]))
                        if t is not None:
                            dtypes[col] = ARRAY(t)
            df.to_sql(
                name,
                engine,
                if_exists=mode,
                chunksize=chunksize,
                index=False,
                dtype=dtypes
            )
