'''Util variables and functions'''

from .nsw_government import NSWGovIngest

STATE_CODES = {
    'New South Wales': 'NSW',
    'Victoria': 'VIC',
    'Queensland': 'QLD',
    'Tasmania': 'TAS',
    'Western Australia': 'WA',
    'South Australia': 'SA',
    'Northern Territory': 'NT',
    'Australian Capital Territory': 'ACT',
}

STATE_NAMES = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland',
    'TAS': 'Tasmania',
    'WA': 'Western Australia',
    'SA': 'South Australia',
    'NT': 'Northern Territory',
    'ACT': 'Australian Capital Territory',
}

def retrieve_lga_lhd_map():
    ingestion = NSWGovIngest({
        'source': {
            'resource_type': 'cases_by_location',
        },
    })
    df = ingestion.retrieve()
    df = ingestion.process(df)
    return df.drop_duplicates(
        ['lhd_code','lhd_name', 'lga_code', 'lga_name']
    )[['lhd_code','lhd_name', 'lga_code', 'lga_name']]

LGA_LHD_MAP = retrieve_lga_lhd_map()
