'''Base class for data source retrieval and processing'''

from .configuration import SourceSchema

class Source():
    '''Base data ingestion class'''
    def __init__(self, cfg):
        self.cfg = SourceSchema().load(cfg)

    def retrieve(self):
        '''Get raw data from source. Returns list of dictionaries.
        '''
        raise NotImplementedError

    def process(self):
        '''Process raw data into desired format. Returns list of dictionaries.
        '''
        return self.retrieve()
