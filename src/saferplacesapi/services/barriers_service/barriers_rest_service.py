# -----------------------------------------------------------------------------

import os
import json
import time
import math
import logging
import datetime
import requests
from enum import Enum

import jsonschema
import pandas as pd
import geopandas as gpd

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils

from .barriers_collection_manager import BarriersCollectionManager

# -----------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'Barrier REST Service Process',
    },
    'description': {
        'en': 'This is a REST service process implementation to handel barriers data',
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
        'mode': {
            'title': 'mode',
            'description': 'REST Service mode. Can be valued as "GET" or "POST"',
            'schema': {
                'type': 'string',
                'enum': ['get', 'post']
            }
        },
        'rest_data': {
            'title': 'rest_data',
            'description': 'REST data to be processed. When mode is GET, it must contain the cookie_code. When mode is POST, it must contain the cookie_code and the barrier data.',
            'schema': {
                'type': 'object'
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
            "token": "ABC123XYZ666",
            "debug": True,
            "mode": "GET",
            "rest_data": {
                "cookie_code": "1234567890"
            }
        }
    }
}

# -----------------------------------------------------------------------------

class RestMode(str, Enum):
    """ Enum for REST modes """
    
    GET = 'GET'
    POST = 'POST'
    
    @classmethod
    def from_str(cls, alias):
        if alias.upper() in cls.__members__:
            return cls[alias.upper()]
        return None



class BarrierRestService(BaseProcessor):
    """ Barrier REST Service Process """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.barriers_collection_manager = BarriersCollectionManager()
        
        
        
    def validate_parameters(self, data):
        token = data.get('token', None)
        mode = data.get('mode', None)
        rest_data = data.get('rest_data', None)
        
        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: wrong token')
        
        if mode is None:
            raise ProcessorExecuteError('Mode is required')
        if type(mode) is not str:
            raise ProcessorExecuteError('Mode must be a string')
        mode = RestMode.from_str(mode)
        if mode is None:
            raise ProcessorExecuteError('Mode must be "GET" or "POST"')
        
        return mode, rest_data
    
    
        
    def execute_get(self, data): 
        cookie_code = data.get('cookie_code', None)
        
        if cookie_code is None:
            raise ProcessorExecuteError('cookie_code is required in GET mode')
        if type(cookie_code) is not str:
            raise ProcessorExecuteError('cookie_code must be a string')
    
        geojson_barrier = self.barriers_collection_manager.get_barrier(cookie_code)
        
        return geojson_barrier
    
    
    
    def execute_post(self, data):
        cookie_code = data.get('cookie_code', None)
        barrier = data.get('barrier', None)
        
        if cookie_code is None:
            raise ProcessorExecuteError('cookie_code is required in GET mode')
        if type(cookie_code) is not str:
            raise ProcessorExecuteError('cookie_code must be a string')
        
        if barrier is None:
            raise ProcessorExecuteError('barrier is required in POST mode')
        if type(barrier) is not dict:
            raise ProcessorExecuteError('barrier must be a dict')
        if not self.barriers_collection_manager.validate_geojson(barrier):
            raise ProcessorExecuteError('barrier is not a valid geojson. See https://geojson.org/schema/GeoJSON.json for more schema details.')
            
        barrier_uri = self.barriers_collection_manager.post_barrier(cookie_code, barrier)
        
        post_output = {
            'barrier_uri': barrier_uri
        }
        return post_output
        
                   
    
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            mode, rest_data = self.validate_parameters(data)
            
            
            outputs = None
            if mode == RestMode.GET:
                # DOC: We provie a geojson feature collection with all the barriers related to a cookie_code ( a user )
                outputs = self.execute_get(rest_data)
            elif mode == RestMode.POST:
                # DOC: We save (overwrite) a geojson feature collection with all the barriers related to a cookie_code ( a user ).
                rest_output = self.execute_post(rest_data)
            
            if outputs is None:
                outputs = {
                    'status': 'OK',
                    'output': rest_output
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
        return f'<BarrierRestService> {self.name}'