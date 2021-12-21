'''Base class for data sinks'''

import pandas as pd
from .configuration import SinkSchema

class Sink():
    '''Base data ingestion class'''
    def __init__(self, cfg):
        self.cfg = SinkSchema().load(cfg)

    def save(self, docs, name=None):
        '''Sink data'''
        if not name:
            name = self.cfg['name']
        if self.cfg['type'] == 'csv':
            if name[-4:] != '.csv':
                name += '.csv'
            if self.cfg['type'] == 'csv':
                df = pd.DataFrame(docs)
                df.to_csv(name)
        else:
            raise NotImplementedError(
                f'Save to sink {self.cfg["type"]} not implemented.'
            )
