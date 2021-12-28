# Ingestion

The ingestion folder includes the ingestion module class for general data source ingestion into a specified data sink, as well as implementation classes for specific sources relevant to the project (COVID-19). The data is sinked into csvs within the folder [herd/ingestion/data](herd/ingestion/data) and are periodically re-retrieved.

## Ingestion Module

The ingestion module contains a base class for data source retrieval and data sinks. The data ingestion process consists of the following general steps:
1. **Retrieve** (raw) data from source into a machine readable format (python dictionaries).
2. **Process** data by the following methods:
    1. Add fields;
    2. Remove fields;
    3. Transform fields;
    4. Rename fields;
    5. Aggregate over fields;
3. **Validate** data via a defined schema;
4. **Save** the data into a designated data sink, typically csv or database.

The `BaseIngest` class contains a method for each of the generic steps above.

All data sources are different and will require specific retrieval and processing methods.

Therefore, the `BaseIngest` base class must be extended per data source and the following methods implemented:

```python
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
```

> NOTE: The default validate method uses the inferred dataframe schema from the pandera module. This can be overwritten for specific validation.

The `save` method contains logic to save the dataframe to different data sinks. This method can be extended for other sink types. Current sinks supported are csv and postgres.

In this way, the `BaseIngest` object is modular, extensible and configurable for many different types of data sources and sinks.

> NOTE: The name of the ingestion class per source module *must* be named `Ingest` for the main logic to run.

### Configuration

The `BaseIngest` base class needs to be configured for both the source and the sink locations. See example configuration below.

```json
{
    "source": {
        "collection": "covid-19-data",
    } ,
    "sink": {
        "type": "csv",
        "name": "covid-19",
        "mode": "replace",
        "chunksize": 100,
    }
}
```

The source field has a blank configuration Marshmallow *Schema* that needs to be overwritten in order to validate the configuration fields specific to the source.
The sink field may have different configuration field per sink `type`. The universal sink fields are `name`, `mode` and `chunksize`.

## Data Sources

The *herd* project seeks to display the COVID-19 statistics for Australia, Australian States & Territories, and even at a LGA/LHD level if available, in order to provide perspective on the pandemic state of affairs.

The following data **dimensions** are sought:
- Date
- LGA
- LHD
- State
- Country (Australia)
- Age (if available)
- Sex (if available)
- Vaccination status (if available)

The following data **measures** are sought:
- Cases
- Deaths
- Recovered
- Hospitalisations
- ICU
- Ventilated
- Tests
- Vaccination 1st Dose
- Vaccination 2nd Dose
- Population (for % calculations)

The following data sources were found, covering the above measures:

| Source                                         | Dataset                                                   | Format       | Frequency    | Link                                                         | Dimensions                                                   | Measures                                                     | Ingested |
| ---------------------------------------------- | --------------------------------------------------------- | ------------ | ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | -------- |
| NSW Government                                 | NSW COVID-19 tests by location                            | CSV          | 1D           | https://data.nsw.gov.au/search/dataset/ds-nsw-ckan-60616720-3c60-4c52-b499-751f31e3b132/details?q= | Date, Postcode, LGA, LHD                                     | Test Count                                                   | Y        |
| NSW Government                                 | NSW COVID-19 cases by location                            | CSV          | 1D           | https://data.nsw.gov.au/search/dataset/ds-nsw-ckan-aefcde60-3b0c-4bc0-9af1-6fe652944ec2/details?q= | Date, Postcode, LGA, LHD                                     | Case Count (Aggregated)                                      | Y        |
| NSW Government                                 | NSW COVID-19 cases by age range                           | CSV          | 1D           | https://data.nsw.gov.au/search/dataset/ds-nsw-ckan-3dc5dc39-40b4-4ee9-8ec6-2d862a916dcf/details?q= | Location (NSW), Date, Age Group                              | Case Count (Aggregated)                                      | N        |
| NSW Government                                 | COVID-19 weekly surveillance reports                      | PDF          | 1W           | https://www.health.nsw.gov.au/Infectious/covid-19/Pages/weekly-reports.aspx | Location (NSW), Date, Age, Sex, Vaccination Status, Source Acquired, Indigenous, Health Care Workers, Correctional Setting, Aged Care workers | Case, Hospitalisations, Deaths                               | N        |
| NSW Government: Agency for Clinical Innovation | COVID-19 Critical Intelligence Unit: COVID-19 Monitor     | PDF          | 1W           | https://aci.health.nsw.gov.au/covid-19/critical-intelligence-unit/monitor | Location (NSW), Vaccination Status                           | Average Daily Cases/Deaths, Hospitalisation, ICU, Rates Per Million | N        |
| Australian Government                          | COVID-19 vaccination – Geographic vaccination rates – LGA | CSV          | 1W           | https://www.health.gov.au/resources/collections/covid-19-vaccination-geographic-vaccination-rates-lga | LGA, State, Date                                             | Dose 1 %, Dose 2 %, Population                               | Y        |
| covidlive                                      | (Different per state)                                     | HTML         | 1D (depends) | https://covidlive.com.au/nsw                                 | States                                                       | Cases, Vaccinations, Tests, Hospitalised                     | N        |
| COVID-19 Data                                  | COVID-19 Data, Deaths                                     | CSV (GitHub) | 1D           | https://github.com/M3IT/COVID-19_Data                        | Date, State, Age Group (deaths)                              | Cases, Deaths, Hospitalisations, ICU, Vent, Vaccination      | Y        |
| Australian Government                          | COVID-19 vaccination – vaccination data                   | CSV          | 1D           | https://www.health.gov.au/resources/collections/covid-19-vaccination-vaccination-data | Date, State, Age Group, Sex                                  | 1st Dose, 2nd Dose                                           | Y        |

## Main
The main.py script can be configured to save the specified data sources to the specified data sink.

On GitHub, this csv is updated (from the full dataset) daily using a cronjob and the changes are committed to the GitHub repository via the bash script update_and_commit.sh.

### Configuration
The main script is configured in the following way:

```yaml
- module: covid19data
  cfg:
    sink:
      type: csv
      name: ingestion/data/covid_au_data
      chunksize: 100
      mode: replace
    source:
      filename: COVID_AU_state.csv
- module: vaccinations
  cfg:
    sink:
      type: csv
      name: ingestion/data/covid_au_vaccination_data
      chunksize: 100
      mode: replace
    source:
      collection: covid-19-vaccination-vaccination-data
```
where the field `module` refers to the name of the script which houses the `Ingest` class to be used to ingest data from, and the field `cfg` is the configuration of the `Ingest` class as per the `IngestSchema` marshmallow schema.

## Development

Install dependencies (within the ingestion folder):
```sh
$ python3.8 -m virtualenv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```

To add a new data source, extend the ingestion Source base class within a script in the sources folder. Incorporate Source engine into the sink.py script as desired.
