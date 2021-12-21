'''Script to merge and sink datasets'''

import pandas as pd

from sources.nsw_government import NSWGovSource
from sources.vaccinations import VaxSource
from sources.covid19data import COVIDAUSource

from ingestion import Sink

def retrieve_and_merge_datasets(engines, dimensions, measures):
    '''Retrieve and merge datasets.

    Args:
        engines (dict): Dict of dataset engines to retrieve and merge.
        dimensions (list): List of fields that are dimensions.
        measures (list): List of fields that are measures.
    '''
    # Retrieve datasets
    datasets = []
    for key, engine in engines.items():
        docs = engine.process()
        # Sink individual dataset
        sink.save(docs, name=key)
        # Append dataset
        datasets.append(pd.DataFrame(docs))
    # Merge datasets
    for i, df in enumerate(datasets):
        # Add blank dimensions
        for col in dimensions:
            if col not in list(df.columns) and col not in measures:
                df[col] = None
        if i == 0:
            _df = df
            continue
        _df = pd.merge(df, _df, how='outer')
    return _df.to_dict('records')

# Construct sink
sink_cfg = {
    'type': 'csv',
}
sink = Sink(sink_cfg)

# ------------------------------------------------------------------ #
# --------------------------- State Data --------------------------- #
# ------------------------------------------------------------------ #

STATE_DIMENSIONS = [
    'date',
    'state_name',
    'state_code',
    'country',
    'age_group',
    'sex',
]

STATE_MEASURES = [
    'confirmed',
    'confirmed_cum',
    'deaths',
    'deaths_cum',
    'tests',
    'tests_cum',
    'positives',
    'positives_cum',
    'recovered',
    'recovered_cum',
    'hosp',
    'hosp_cum',
    'icu',
    'icu_cum',
    'vent',
    'vent_cum',
    'vaccines',
    'vaccines_cum',
    'vax_1_dose',
    'vax_1_%',
    'vax_2_dose',
    'vax_2_%',
    'population',
    'death_count',
]

# Construct dataset ingestion engines
state_ingestion_engines = {
    'covid_au_data': COVIDAUSource({
        'filename': 'COVID_AU_state.csv',
    }),
    'covid_au_death_data': COVIDAUSource({
        'filename': 'COVID_AU_deaths.csv',
    }),
    'covid_au_vaccination_data': VaxSource({
        'collection': 'covid-19-vaccination-vaccination-data',
    }),
}

docs = retrieve_and_merge_datasets(
    state_ingestion_engines,
    STATE_DIMENSIONS,
    STATE_MEASURES
)

sink.save(docs, name='states')

# ------------------------------------------------------------------ #
# --------------------------- State Data --------------------------- #
# ------------------------------------------------------------------ #

LGA_DIMENSIONS = [
    'lhd_code',
    'lhd_name',
    'lga_code',
    'lga_name',
    'state'
]

LGA_MEASURES = [
    'test_count',
    'case_count',
    'vax_1_%_15+',
    'vax_2_%_15+',
    'vax_1_dose_15+',
    'vax_2_dose_15+',
    'population_15+',
    'population_15+',
]

# Construct ingestion engines
lga_ingestion_engines = {
    'covid_nsw_tests_by_location': NSWGovSource({
        'resource_type': 'tests_by_location'
    }),
    'covid_nsw_cases_by_location': NSWGovSource({
        'resource_type': 'cases_by_location'
    }),
    'covid_vaccination_by_lga': VaxSource({
        'collection': 'covid-19-vaccination-geographic-vaccination-rates-lga'
    }),
}

docs = retrieve_and_merge_datasets(
    lga_ingestion_engines,
    LGA_DIMENSIONS,
    LGA_MEASURES,
)
sink.save(docs, name='lga')
