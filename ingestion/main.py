'''Main Script'''
import argparse
import yaml
from marshmallow.fields import Nested, String
from marshmallow.schema import Schema
from importlib import import_module
from ingestion import IngestSchema, logging

class Configuration(Schema):
    module = String(required=True)
    cfg = Nested(IngestSchema, required=True)

FILEPATH = 'ingestion/data/{name}'

def main(args):
    # Get config
    with open(args.config) as f:
        config = yaml.safe_load(f)
    for i in config:
        module = import_module(f'.{i["module"]}', package='sources')
        # Ensure that the Ingest class inherits from the BaseIngest
        engine = module.Ingest(i['cfg'])
        name = i['cfg']['sink']['name']
        # Retrieve
        df = engine.retrieve()
        # Process
        df = engine.process(df)
        # Validate
        df = engine.validate(df)
        # Save
        engine.save(df, name=name)

if __name__=='__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='Path to config file.', required=True)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    # Set logging level
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    # Run main logic
    main(args)
