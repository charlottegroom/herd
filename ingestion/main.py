'''Script to merge and sink datasets'''

import pandas as pd

from sources.nsw_government import NSWGovIngest
from sources.vaccinations import VaxIngest
from sources.covid19data import COVIDAUIngest

FILEPATH = 'ingestion/data/{name}'

def main():
    # Construct dataset ingestion engines
    ingestion_engines = [
        {
            'engine': COVIDAUIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_au_data',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'filename': 'COVID_AU_state.csv',
                },
            },
        },
        {
            'engine': COVIDAUIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_au_death_data',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'filename': 'COVID_AU_deaths.csv',
                },
            },
        },
        {
            'engine': VaxIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_au_vaccination_data',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'collection': 'covid-19-vaccination-vaccination-data',
                },
            },
        },
        {
            'engine': VaxIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_vaccination_by_lga',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'collection': 'covid-19-vaccination-geographic-vaccination-rates-lga'
                },
            },
        },
        {
            'engine': NSWGovIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_nsw_tests_by_location',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'resource_type': 'tests_by_location'
                },
            },
        },
        {
            'engine': NSWGovIngest,
            'cfg': {
                'sink': {
                    'type': 'csv',
                    'name': 'covid_nsw_cases_by_location',
                    'chunksize': 100,
                    'mode': 'replace',
                },
                'source': {
                    'resource_type': 'cases_by_location'
                },
            },
        },
    ]

    for i in ingestion_engines:
        engine = i['engine'](i['cfg'])
        name = i['cfg']['sink']['name']
        # Sink individual dataset
        df = engine.retrieve()
        df = engine.process(df)
        df = engine.validate(df)
        engine.save(df, name=FILEPATH.format(name=name))

if __name__=='__main__':
    main()
