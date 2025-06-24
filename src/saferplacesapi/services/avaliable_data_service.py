# -----------------------------------------------------------------------------

import os
import logging
import datetime

import numpy as np
import pandas as pd
import geopandas as gpd

import duckdb

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils
from saferplacesapi import _utils


# -----------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'Avaliable Data Service',
    },
    'description': {
        'en': 'This process returns avaliable data given optional filters of time ranges and providers'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['safer process'],
    'inputs': {
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },
        'providers': {
            'title': 'Providers',
            'description': 'Dictionary of providers to filter the avaliable data. If not provided, all providers will be considered. Key is the provider name, value is a list of variables required for that provider.',
            'schema': {
                'type': 'object',
            },
        },
        'time_range': {
            'title': 'Time Range',
            'description': 'Time range for which you want to retrieve data. It should be a list of two datetime strings in ISO format.',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'string',
                    'format': 'date-time'
                },
                'minItems': 2,
                'maxItems': 2
            }
        },
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode. Can be valued as true or false',
            'schema': {
            }
        }
    },
    'outputs': {
        'status': {
            'title': 'status',
            'description': 'Staus of the process execution [OK or KO]',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "token": "D1rected_T0ken",
            "debug": True,
            "providers": {
                "ARPAE_realtime": ["B13011", "B22070"],
                "ADRIAC": "sea_level__points",
                "SWANEMR": "ALL"
            },
            "time_range": [
                "2025-06-18T08:00:00",
                None
            ]
        }
    }
}

# -----------------------------------------------------------------------------


class AvaliableDataService(BaseProcessor):
    """ Avaliable Data Service Process """


    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.bucket_source = f'{_s3_utils._base_bucket}/__avaliable-data__'

        # FIXME: This is absolutely prone to bugs and hard to mantain → time of use a Names class manager is arrived
        self.avaliable_providers = {
            'DPC': [ 'SRI' ],
            'ARPAE_realtime': [ 'B13011', 'B13215', 'B22001', 'B22037', 'B22070' ],
            'Meteoblue': [ 'precipitation' ],
            'ICON_2I': [ 'precipitation' ],
            'SWANEMR': [ 'wave_height', 'wave_height__points' ],
            'ADRIAC': [ 'sea_level', 'sea_level__points' ],
        }



    def validate_parameters(self, data):
        token = data.get('token', None)
        providers = data.get('providers', None)
        time_range = data.get('time_range', None)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: wrong token')
        
        if providers is None:
            providers = self.avaliable_providers
        elif isinstance(providers, str):
            if providers not in self.avaliable_providers:
                raise ProcessorExecuteError('Invalid provider name provided. Please choose from the available providers: ' + ', '.join(self.avaliable_providers.keys()))
            providers = { providers: self.avaliable_providers[providers] }
        elif isinstance(providers, list):
            if any(type(provider) is not str for provider in providers):
                raise ProcessorExecuteError('Invalid provider name provided. Please provide a list of strings.')
            if any(provider not in self.avaliable_providers for provider in providers):
                raise ProcessorExecuteError('Invalid provider name provided. Please choose from the available providers: ' + ', '.join(self.avaliable_providers.keys()))
            providers = { provider: self.avaliable_providers[provider] for provider in providers if provider in self.avaliable_providers }
        elif isinstance(providers, dict):
            for provider,variables in providers.items():
                if provider not in self.avaliable_providers:
                    raise ProcessorExecuteError('Invalid provider name provided. Please choose from the available providers: ' + ', '.join(self.avaliable_providers.keys()))
                if type(variables) is str:
                    if variables == 'ALL':
                        variables = self.avaliable_providers[provider]
                    else:
                        variables = [variables]
                    providers[provider] = variables
                elif type(variables) is not list:
                    raise ProcessorExecuteError(f'Invalid variables for provider {provider}. Please provide a list of strings or "ALL".')
                if any(variable not in self.avaliable_providers[provider] for variable in variables):
                    raise ProcessorExecuteError(f'Invalid variable for provider {provider}. Please choose from the available variables: ' + ', '.join(self.avaliable_providers[provider]))
        else:
            raise ProcessorExecuteError('Invalid providers parameter. Please provide a list of strings, a dictionary or a single string.')
        
        if time_range is not None:
            if type(time_range) is not list:
                raise ProcessorExecuteError('Invalid time_range parameter. Please provide a list of two datetime strings in ISO format.')
            if len(time_range) != 2:
                raise ProcessorExecuteError('Invalid time_range parameter. Please provide a list of two datetime strings in ISO format.')
            start_time, end_time = time_range
            if start_time is not None:
                if type(start_time) is not str:
                    raise ProcessorExecuteError('Invalid start_time parameter. Please provide a datetime string in ISO format.')
                try:
                    start_time = datetime.datetime.fromisoformat(start_time)
                except ValueError:
                    raise ProcessorExecuteError('Invalid start_time parameter. Please provide a datetime string in ISO format.')
            if end_time is not None:
                if type(end_time) is not str:
                    raise ProcessorExecuteError('Invalid end_time parameter. Please provide a datetime string in ISO format.')
                try:
                    end_time = datetime.datetime.fromisoformat(end_time)
                except ValueError:
                    raise ProcessorExecuteError('Invalid end_time parameter. Please provide a datetime string in ISO format.')
            if start_time is not None and end_time is not None:
                if start_time >= end_time:
                    raise ProcessorExecuteError('Invalid time_range parameter. The start time must be before the end time.')
            time_range = [start_time, end_time]

        return providers, time_range

            

    def query_avaliable_data(self, providers, time_range):
        """
        Query the avaliable data from DuckDB based on the provided providers and time range exploiting hive-partitioning structure of the bucket-source folder.
        """

        # TODO: This is a temporary solution, it should be replaced with a more robust query that can handle multiple providers and time ranges (USE WHERE CLAUSE).
        q = f"""
            SELECT *
            FROM read_json('{self.bucket_source}/year==*/month==*/day==*/provider==*/*.json')
            ORDER BY date_time DESC, provider ASC
        """
        out = duckdb.query(q).df()

        # Parse date_time column
        out['date_time'] = pd.to_datetime(out['date_time'])

        # Filter by providers
        pv_filters = []
        for provider,variables in providers.items():
            pv_filter = (out['provider'] == provider) & (out['variable'].isin(variables))
            pv_filters.append(pv_filter)
        if pv_filters:
            out = out[np.logical_or.reduce(pv_filters)]
        
        # Filter by time range
        if time_range is not None:
            start_time, end_time = time_range
            if start_time is not None:
                out = out[out['date_time'] >= start_time]
            if end_time is not None:
                out = out[out['date_time'] <= end_time]
        else:
            out = out.sort_values(by=['date_time', 'provider'], ascending=[False, True]).reset_index(drop=True)
            out = out.groupby(by=['provider','variable'])[out.columns].aggregate({ col: 'first' for col in out.columns }).reset_index(drop=True)

        # Sort by date_time and provider
        avaliable_data = out.sort_values(by=['date_time', 'provider'], ascending=[False, True]).reset_index(drop=True)

        return avaliable_data



    def prepare_output(self, avaliable_data):
        """
        Prepare the output data in the required format.
        """

        avaliable_data['date_time'] = avaliable_data['date_time'].apply(lambda x: x.isoformat())
        avaliable_data_list = avaliable_data.to_dict(orient='records')
        return avaliable_data_list
    


    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            # DOC: Validate parameters
            providers, time_range = self.validate_parameters(data)

            # DOC: DuckDB query avaliable data
            avaliable_data = self.query_avaliable_data(providers, time_range)

            # DOC: Prepare output
            avaliable_data_out = self.prepare_output(avaliable_data)
            
            outputs = {
                'status': 'OK',
                'avaliable_data': avaliable_data_out
            }
            
        except _processes_utils.Handle200Exception as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': 'KO',
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        return mimetype, outputs


    def __repr__(self):
        return f'<AvaliableDataService> {self.name}'
        