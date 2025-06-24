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

from safer_buildings.main import compute_flood as safer_buildings_compute_flood 

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
        'en': 'Safer Buildings Service',
    },
    'description': {
        'en': 'This process computes flooded buildings based on water depth raster data.',
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
        'waterdepth_uri': {
            'title': 'Water Depth Raster URI',
            'description': 'URI of the water depth raster file in S3. It must be a valid S3 URI (e.g., s3://bucket-name/path/to/file.tif)',
            'schema': {
                'type': 'string',
                'format': 'uri'
            }
        },
        'waterdepth_threshold': {
            'title': 'Water Depth Threshold',
            'description': 'Threshold value for water depth to consider a building as flooded. It must be a float. Units are in meters.',
            'schema': {
                'type': 'number',
                'minimum': 0.0
            }
        },
        'bbox': {
            'title': 'Bounding Box',
            'description': 'Bounding box to filter the barriers. It must be a list of 4 floats [minx, miny, maxx, maxy]',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'number'
                },
                'minItems': 4,
                'maxItems': 4
            }
        },
        # 'buildings_provider': {
        #     'title': 'Buildings Provider',
        #     'description': 'Provider code for the buildings data. It must be a string. Default is "RER-REST/28/31/40". That are codes for "Strutture Industriali e Produttive", "Sistema Insediativo", "Strutture Sanitarie"',
        #     'schema': {
        #         'type': 'string',
        #         'default': 'RER-REST/28/31/40',
        #     }
        # },
        'output_geojson': {
            'title': 'Output GeoJSON',
            'description': 'Whether to output or not the entire flooded buildings feature collection as a GeoJSON file. Default is False.',
            'schema': {
                'type': 'boolean',
                'default': False
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
            "waterdepth_uri": "s3://saferplaces-data/waterdepths/2023-10-01/waterdepth.tif",
            "waterdepth_threshold": 0.5,
            "bbox": [-12.0, 37.0, 15.0, 47.0]
        }
    }
}

# -----------------------------------------------------------------------------


class SaferBuildingsService(BaseProcessor):
    """ Safer Buildings Process """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self._data_folder = os.path.join(os.getcwd(), 'SaferBuildingsService')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        
        self.bucket_destination = f'{_s3_utils._base_bucket}/SaferBuildingsService'

        # DOC: Default buildings provider is set to a specific set of codes from RER-REST (All possible types of buildings)
        self.buildings_provider = 'RER-REST/11/13/14/15/17/18/19/20/21/22/23/24/25/27/29/30/32/33/34/35/36/37/38/39/40/98/99/100/101/102/103/104'
        
        # DOC: Default buildings URI contains buildings retrived from 'buildings_provider' intersecated if possible with overture buildings for the Directed Project default area
        self.buildings_uri = f'{self.bucket_destination}/Data/buildings-default-area__rer-rest_overture.geojson'
        


    def validate_parameters(self, data):
        token = data.get('token', None)
        waterdepth_uri = data.get('waterdepth_uri', None)
        waterdepth_threshold = data.get('waterdepth_threshold', None)
        bbox = data.get('bbox', None)
        output_geojson = data.get('output_geojson', False)
        # buildings_provider = data.get('buildings_provider', 'RER-REST/28/31/40')
        
        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'ACCESS DENIED: wrong token')
        
        if waterdepth_uri is None:
            raise ProcessorExecuteError('waterdepth_uri is required')
        if type(waterdepth_uri) is not str:
            raise ProcessorExecuteError('waterdepth_uri must be a string')
        if not _s3_utils.iss3(waterdepth_uri):
            raise ProcessorExecuteError('waterdepth_uri must be a valid s3 uri')
        
        waterdepth_filename = os.path.join(self._data_folder, _utils.justfname(waterdepth_uri))
        download = _s3_utils.s3_download(waterdepth_uri, waterdepth_filename)
        if download is None:
            raise ProcessorExecuteError(f'waterdepth uri {waterdepth_uri} not found')
        
        if type(output_geojson) is not bool:
            raise ProcessorExecuteError('output_geojson must be a boolean')
        
        # DOC: 'waterdepth_threshold', 'bbox', 'buildings_provider' args will be validated by safer-building package execution
        
        return waterdepth_uri, waterdepth_filename, waterdepth_threshold, bbox, output_geojson #, buildings_provider
    
    
    
    def save_to_s3_bucket(self, flooded_buildings_fc, fc_filename):
        flooded_buildings_fc_filename = os.path.join(self._data_folder, fc_filename)
        with open(flooded_buildings_fc_filename, 'w') as f:
            json.dump(flooded_buildings_fc, f)
        flooded_buildings_fc_s3_uri = os.path.join(self.bucket_destination, os.path.basename(flooded_buildings_fc_filename))
        _s3_utils.s3_upload(flooded_buildings_fc_filename, flooded_buildings_fc_s3_uri)
        return flooded_buildings_fc_s3_uri
                   

    
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            # DOC: Validate parameters
            waterdepth_uri, waterdepth_filename, waterdepth_threshold, bbox, output_geojson = self.validate_parameters(data)
            
            # DOC: Execute the safer-buildings compute_flood function
            flooded_buildings_fc = safer_buildings_compute_flood(
                waterdepth_filename = waterdepth_filename,
                buildings_filename = self.buildings_uri,
                wd_thresh = waterdepth_threshold,
                bbox = bbox,
                out = _utils.forceext(waterdepth_filename, 'geojson'),
                t_srs = None,
                provider = self.buildings_provider,
                feature_filters = None,
                only_flood = False,
                compute_stats = False,
                compute_summary = True
            )
            
            # DOC: Save the flooded buildings feature collection to S3
            flooded_buildings_fc_s3_uri = self.save_to_s3_bucket(flooded_buildings_fc, f'{_utils.juststem(waterdepth_uri)}__flooded_buildings.geojson')
            
            # DOC: Prepare the output
            if output_geojson:
                safer_buildings_output = { 'output': flooded_buildings_fc }
            else:
                safer_buildings_output = { 'summary': flooded_buildings_fc['metadata']['summary'] }

            # DOC: Return values
            outputs = {
                'status': 'OK',
                's3_uri': flooded_buildings_fc_s3_uri,
                ** safer_buildings_output
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
        return f'<SaferBuildingsService> {self.name}'