# -----------------------------------------------------------------------------

import os
import json
import time
import math
import logging
import datetime
import requests

import asyncio
import aiohttp

import numpy as np
import pandas as pd

import xarray as xr

from gdal2numpy.module_Numpy2GTiff import Numpy2GTiffMultiBanda

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils

# -----------------------------------------------------------------------------


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'Meteoblue Precipitation Retriever Process',
    },
    'description': {
        'en': 'Collect Precipitations data from Meteoblue API'
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
        'service': {
            'title': 'Meteoblue forecast service',
            'description': 'The Meteoblue forecast service to use. Possible values are "basic-5min" and "basic-1h". If no service is provided, the default value is "basic-5min"',
            'schema': {
                'type': 'string',
                'enum': ['basic-5min', 'basic-1h']
            }
        },
        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 crs. If no latitude range is provided, all latitudes will be returned',
            'schema': {
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 crs. If no longitude range is provided, all longitudes will be returned',
            'schema': {
            }
        },
        'grid_res': {
            'title': 'Grid resolution',
            'description': 'The grid resolution in meters. The grid resolution must be a multiple of 100. If no grid resolution is provided, the default value is 1000 meters',
            'schema': {
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format.', # TODO: complete based on Meteoblue doc avaliability
            'schema': {
            }
        },   
        'time_delta': {
            'title': 'Time delta',
            'description': 'The time delta in minutes. The time delta must be a multiple of 5. If no time delta is provided, the default value is 5 minutes', # TODO: Will depend on Meteoblue service (5min / 1h)
            'schema': {
            }
        },
        'strict_time_range': {
            'title': 'Strict time range',
            'description': 'Enable strict time range to check data avaliability until requested end time. Can be valued as true or false. Default is false',
            'schema': {
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "netcdf", "json", "dataframe"',
            'schema': {
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
                'type': 'string',
                'enum': ['OK', 'KO']
            }
        },
        'collected_data': {
            'title': 'Collected data',
            'description': 'Reference to the collected data. Each entry contains the date and the S3 URI of the collected data',
            'type': 'array',
            'schema': {
                'type': 'object',
                'properties': {
                    'date': {
                        'type': 'string'
                    },
                    'S3_uri': {
                        'type': 'string'
                    }
                }
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "forecast_run": ["2025-02-26T00:00:00", "2025-02-26T12:00:00"]
        }
    }
}

# -----------------------------------------------------------------------------

class MeteobluePrecipitationRetrieverProcessor(BaseProcessor):
    """Meteoblue Precipitation Ingestor process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'Meteoblue'
        self.variable_name = 'precipitation'
        
        self.meteoblue_services = {
            'basic-5min': {
                'api_url': 'https://my.meteoblue.com/packages/basic-5min',
                'response_data_key': 'data_xmin',
                'init_time_frequency': f'{6}h',
                'data_folder': os.path.join(os.getcwd(), f'{self.dataset_name}_basic-5min_ingested_data'),
                'bucket_destination': f'{_s3_utils._base_bucket}/{self.dataset_name}/basic-5min/{self.variable_name}'
            },
            'basic-1h': {
                'api_url': 'https://my.meteoblue.com/packages/basic-1h',
                'response_data_key': 'data_1h',
                'init_time_frequency': f'{6}h',
                'data_folder': os.path.join(os.getcwd(), f'{self.dataset_name}_basic-1h_ingested_data'),
                'bucket_destination': f'{_s3_utils._base_bucket}/{self.dataset_name}/basic-1h/{self.variable_name}'
            }
        }
        
        for ms in self.meteoblue_services.values():
            if ms.get('data_folder', None) is not None and not os.path.exists(ms['data_folder']):
                os.makedirs(ms['data_folder'], exist_ok=True)
        
        
        
    def validate_parameters(self, data):
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data)
        
        service = data.get('service', 'basic-5min')
        if type(service) is not str:
            raise ProcessorExecuteError('Service must be a string')
        if service not in self.meteoblue_services.keys():
            raise ProcessorExecuteError(f'Service {service} not available. Available services are {list(self.meteoblue_services.keys())}')
        
        grid_res = data.get('grid_res', 1000)
        if type(grid_res) is not int:
            raise ProcessorExecuteError('Grid resolution must be an integer')
        if grid_res % 100 != 0:
            raise ProcessorExecuteError('Grid resolution must be a multiple of 100')
        if grid_res < 100:
            raise ProcessorExecuteError('Grid resolution must be greater than 100')
        
        today_date = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if time_start < today_date.replace(hour=0, minute=0, second=0, microsecond=0):
            raise ProcessorExecuteError('Time start must be in the future')
        if time_end is not None:
            if time_end < today_date.replace(hour=0, minute=0, second=0, microsecond=0):
                raise ProcessorExecuteError('Time end must be in the future')
            if time_end > today_date.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=7) and strict_time_range:    # REF: https://docs.meteoblue.com/en/weather-apis/forecast-api/forecast-data#basic
                raise _processes_utils.Handle200Exception(
                    _processes_utils.Handle200Exception.SKIPPED, 
                    'No data available for the requested time range. Range is limited to 7 days. Unset strict_time_range to false to get maximum available data'
                )        
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)
        
        time_delta = data.get('time_delta', None)
        if time_delta is not None and type(time_delta) is not int:
            raise ProcessorExecuteError('Time delta must be an integer')
        if service=='basic-5min':
            time_delta = 5 if time_delta is None else time_delta
            if time_delta % 5 != 0:
                raise ProcessorExecuteError('Time delta must be a multiple of 5 minutes')
        if service=='basic-1h':
            time_delta = 60 if time_delta is None else time_delta
            if time_delta % 60 != 0:
                raise ProcessorExecuteError('Time delta must be a multiple of 60 minutes')

        return service, lat_range, long_range, grid_res, time_start, time_end, time_delta, strict_time_range, out_format
    
    
    
    def generate_grid_points(self, bbox, grid_res):
        lon_list = np.round(np.linspace(bbox[0], bbox[2], int((bbox[2]-bbox[0]) // (grid_res*1e-5))), 3)
        lat_list = np.round(np.linspace(bbox[1], bbox[3], int((bbox[3]-bbox[1]) // (grid_res*1e-5))), 3)
        coords = [ (lon, lat) for lat in lat_list for lon in lon_list ]
        return coords
    
    
    
    async def fetch_and_process(self, session, base_url, params, data_key):
        semaphore = asyncio.Semaphore(10)
        async with semaphore:
            async with session.get(base_url, params=params) as response:
                status_code = response.status
                if status_code == 200:
                    out = await response.json()
                    da = xr.DataArray(
                        data = [[out[data_key]['precipitation']]],
                        dims = ["lat", "lon", "time"],
                        coords=dict(
                            lat = [params['lat']],
                            lon = [params['lon']],
                            time = [datetime.datetime.fromisoformat(dt) for dt in out[data_key]['time']]
                        )
                    )
                    return da
                else:
                    out = await response.json()
                    print(out)
    
    
    async def run_requests(self, base_url, requests_params, data_key):
        responses = []
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_and_process(session, base_url, params, data_key) for params in requests_params]
            responses.extend(await asyncio.gather(*tasks))
        return responses
    
    
    
    def get_init_dataset_name(self, service):
        init_time = pd.Timestamp(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)).floor(self.meteoblue_services[service]['init_time_frequency'])
        init_dataset_name = f"{self.dataset_name}__{self.variable_name}__{init_time.isoformat(timespec='seconds')}.nc"
        return init_dataset_name
    
    
    def check_s3_data_availability(self, service):
        """
        Meteoblue api are oriented to XY Points, then we need to multiple call to get area coverage (grid).
        Meteoblue API are also limited to max call by day. 
        What we want to do is as we get the data save it with an init time and lead time with arbitrary frequency (eg: 6-12-24h).
        When a time range is requested, I compute the requested init time and lead time, I observe if they have already been downloaded and if so I do not make calls to the Meteoblue APIs.
        """
        s3_init_dataset_name = self.get_init_dataset_name(service)
        dataset_file = _s3_utils.list_s3_files(self.meteoblue_services[service]['bucket_destination'], filename_prefix=s3_init_dataset_name)
        dataset_file = os.path.basename(dataset_file[0]) if len(dataset_file) > 0 else None
        if dataset_file is not None:
            s3_dataset_uri = os.path.join(self.meteoblue_services[service]['bucket_destination'], dataset_file)
            dataset_path = os.path.join(self.meteoblue_services[service]['data_folder'], dataset_file)
            _s3_utils.s3_download(s3_dataset_uri, dataset_path)
            dataset = xr.open_dataset(dataset_path)
            return dataset
        return None
            
        
        
    def retrieve_data(self, service, lat_range, long_range, grid_res):
        dataset = self.check_s3_data_availability(service)
        if dataset is None:
            # 1. Generate grid points
            grid_coords = self.generate_grid_points([long_range[0], lat_range[0], long_range[1], lat_range[1]], grid_res)
            # 2. Generate tile url params
            base_params = {
                'format': 'json',
                'apikey': os.getenv('METEOBLUE_API_KEY'),
            }
            requests_params = [
                base_params | { 'lon': lon, 'lat': lat }
                for (lon,lat) in grid_coords
            ]
            # 3. Async requests to the API
            coverages = asyncio.run(self.run_requests(self.meteoblue_services[service]['api_url'], requests_params, data_key = self.meteoblue_services[service]['response_data_key']))
            # 4. Dataset creation - concat by coords
            dataset = xr.combine_by_coords(coverages).to_dataset(name=self.variable_name)
            dataset[f'{self.variable_name}__CUMSUM'] = dataset[self.variable_name].cumsum(dim='time', skipna=True)
            dataset = _processes_utils.ds2float32(dataset)
            # 5. Save dataset to S3 bucket
            s3_init_dataset_name = self.get_init_dataset_name(service)
            dataset_path = os.path.join(self.meteoblue_services[service]['data_folder'], s3_init_dataset_name)
            dataset.to_netcdf(dataset_path, engine='netcdf4')
            dataset_s3_uri = os.path.join(self.meteoblue_services[service]['bucket_destination'], s3_init_dataset_name)
            _s3_utils.s3_upload(dataset_path, dataset_s3_uri)
        return dataset
        
       
        
    def create_timestamp_raster(self, service, dataset):
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')) for ts in dataset.time.values]
        
        merged_raster_filename = _processes_utils.get_raster_filename(
            self.dataset_name, self.variable_name, 
            None,
            None,
            (dataset.time.values[0], None)
        )
        merged_raster_filepath = os.path.join(self.meteoblue_services[service]['data_folder'], merged_raster_filename)
        
        xmin, xmax = dataset.lon.min().item(), dataset.lon.max().item()
        ymin, ymax = dataset.lat.min().item(), dataset.lat.max().item()
        nx, ny = dataset.dims['lon'], dataset.dims['lat']
        pixel_size_x = (xmax - xmin) / nx
        pixel_size_y = (ymax - ymin) / ny

        data = dataset.sortby('lat', ascending=False)[self.variable_name].values
        geotransform = (xmin, pixel_size_x, 0, ymax, 0, -pixel_size_y)
        projection = dataset.attrs.get('crs', 'EPSG:4326')
        
        Numpy2GTiffMultiBanda(
            data,
            geotransform,
            projection,
            merged_raster_filepath,
            format="COG",
            save_nodata_as=-9999.0,
            metadata={
                'band_names': [ts.isoformat() for ts in timestamps],
                'type': 'rainfall',
                'um': 'mm'
            }
        )
    
        return merged_raster_filepath
        
    
    def update_available_data(self, dataset, s3_uri):
        _ = _processes_utils.update_avaliable_data(
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.min().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=0)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=0)[self.variable_name].mean(skipna=True).item()
            }
        )
        _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.min().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=0)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=0)[self.variable_name].mean(skipna=True).item()
            }
        )
    
        
    def execute(self, data):
        mimetype = 'application/json'

        outputs = {}
        try:
            # Validate parameters
            service, lat_range, long_range, grid_res, time_start, time_end, time_delta, strict_time_range, out_format = self.validate_parameters(data)
            
            # Retrieve data from Meteoblue API
            dataset = self.retrieve_data(service, lat_range, long_range, grid_res)
            
            # Query based on spatio-temporal range + resampling in given time delta
            query_dataset = _processes_utils.dataset_query(dataset, lat_range, long_range, [time_start, time_end])
            query_dataset = query_dataset.resample(time=f'{time_delta}min', skipna=True).sum()
            
            # Save to S3 Bucket - Merged timestamp multiband raster
            merged_raster_filepath = self.create_timestamp_raster(service, query_dataset)
            merged_raster_s3_uri = _processes_utils.save_to_s3_bucket(self.meteoblue_services[service]['bucket_destination'], merged_raster_filepath)
            
            # Update available data
            self.update_available_data(query_dataset, merged_raster_s3_uri)
            
            # Prepare output dataset
            out_data = dict()
            if out_format is not None:
                out_dataset = _processes_utils.datasets_to_out_format(query_dataset, out_format, to_iso_format_columns=['time'])
                out_data = {'data': out_dataset}
            
            # Return values
            outputs = {
                'status': 'OK',
                
                's3_uri': merged_raster_s3_uri,
                
                **out_data
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
        
        _processes_utils.garbage_folders(self.meteoblue_services['basic-5min']['data_folder'], self.meteoblue_services['basic-1h']['data_folder'])
        
        return mimetype, outputs



    def __repr__(self):
        return f'<MeteobluePrecipitationRetrieverProcessor> {self.name}'