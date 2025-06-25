# -----------------------------------------------------------------------------

import os
import json
import time
import logging
import datetime
import requests
import operator

import numpy as np
import xarray as xr
import rioxarray as rxr

from rasterio.enums import Resampling

from shapely.geometry import box
import geopandas as gpd

from gdal2numpy.module_Numpy2GTiff import Numpy2GTiffMultiBanda

from flask import request
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
        'en': 'NowRadar Rainfall Process',
    },
    'description': {
        'en': 'NowRadar Rainfall forecast data from Hypermeteo API'
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
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format from current date time to maximum next t3 hours. If no time range is provided, all times will be returned',
            'schema': {
            }
        },   
        'time_delta': {
            'title': 'Time delta',
            'description': 'The time delta in minutes. The time delta must be a multiple of 5. If no time delta is provided, the default value is 5 minutes',
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
            }
        },
        's3_uri': {
            'title': 'S3 Uri',
            'description': 'S3 Uri of the merged timestamp multiband raster',
            'schema': {
            }
        },
        'data':{
            'title': 'data',
            'description': 'The dataset in specified out_format',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "lat_range": [43.8, 44.2],
            "long_range": [12.4, 12.9],
            "time_range": ["2025-03-18T10:00:00.000", "2025-03-18T11:00:00.000"],
            "out_format": "json"
        }
    }
}

class NowRadarRainfallProcessor(BaseProcessor):
    """NowRadar Rainfall Processor class"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'NOWRADAR_ITA_1KM_5MIN'
        self.variable_name = 'rainrate'
        
        self._data_provider_auth_service = 'https://api.hypermeteo.com/auth-b2b/authenticate'
        self._data_provider_service = "https://api.hypermeteo.com/b2b-binary/ogc/geoserver/wcs"     
        self._data_folder = os.path.join(os.getcwd(), f'{self.dataset_name}__{self.variable_name}')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}' 
        
    
    def hypermeteo_authentication(self):
        body = {
            "username": os.getenv("HYPERMETEO_USERNAME"),
            "password": os.getenv("HYPERMETEO_PASSWORD")
        }
        response = requests.post(self._data_provider_auth_service, json=body)
        if response.ok:
            auth_token = response.json()['token']
            return auth_token
        else:
            print(f'[{response.status_code}] - Error during authentication. Server response: {response.content.decode("utf-8")}')
            return None
        
    
    def validate_parameters(self, data):
        
        auth_token = self.hypermeteo_authentication()
        if auth_token is None:
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.DENIED, 'Error during hypermeteo authentication')
        
        time_delta = data.get('time_delta', 5)
        if type(time_delta) is not int:
            raise ProcessorExecuteError('Time delta must be an integer')
        if time_delta % 5 != 0:
            raise ProcessorExecuteError('Time delta must be a multiple of 5')
        
        lat_range, long_range, time_start, time_end, _, out_format = _processes_utils.validate_parameters(data)
        
        if time_end is not None:
            if time_start - time_end > datetime.timedelta(hours=3):
                raise ProcessorExecuteError('Time range must be less than or equal to 3 hours')
            
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)

        return auth_token, lat_range, long_range, time_start, time_end, time_delta, out_format
    
    
    def hypermeteo_request(self, auth_token, lat_range, long_range, time_start, time_end=None):
        lat_subset = f'Lat({lat_range[0]},{lat_range[1]})'
        long_subset = f'Long({long_range[0]},{long_range[1]})'
        time_subset = f'time("{time_start.isoformat(timespec="milliseconds")}Z","{time_end.isoformat(timespec="milliseconds")}Z")' if time_end is not None else f'time("{time_start.isoformat(timespec="milliseconds")}Z")'

        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
        params = {
            "request": "GetCoverage",
            "service": "WCS",
            "version": "2.0.0",
            "coverageId": f"{self.dataset_name}__{self.variable_name}",
            "format": "application/x-netcdf",
            "subset": [long_subset, lat_subset, time_subset]
        }

        response = requests.get(self._data_provider_service, headers=headers, params=params)
        return response
    
    
    # def retrieve_data(self, auth_token, lat_range, long_range, time_start, time_end=None, timedelta_minutes=5):

    #     delta_hours = timedelta_minutes // 60
    #     delta_minutes = (timedelta_minutes - (delta_hours*60))

    #     time_start = time_start.replace(
    #         minute = (time_start.minute // delta_minutes) * delta_minutes if delta_hours==0 else 0,
    #         second = 0,
    #         microsecond = 0
    #     )

    #     time_end = time_end.replace(
    #         hour = time_end.hour,
    #         minute = ((time_end.minute // delta_minutes) * delta_minutes) if delta_hours==0 else 0,
    #         second = 0,
    #         microsecond = 0
    #     )
        
    #     datetime_list = []
    #     if time_end is not None:
    #         current_time = time_start
    #         while current_time <= time_end:
    #             datetime_list.append(current_time)
    #             current_time += datetime.timedelta(hours=delta_hours, minutes=delta_minutes)
    #     else:
    #         datetime_list.append(time_start)

    #     filename_list = []
    #     for moment in datetime_list:
            
    #         # TODO: Check if avaliable in S3
            
    #         response = self.hypermeteo_request(
    #             auth_token = auth_token,
    #             lat_range = lat_range,
    #             long_range = long_range,
    #             time_start = moment,
    #             time_end = None
    #         )

    #         netcdf_filename = _processes_utils.get_netcdf_filename(self.dataset_name, self.variable_name, lat_range, long_range, moment)
    #         tmp_filename = os.path.join(self._data_folder, netcdf_filename)
    #         if response.ok:
    #             _processes_utils.write_file_from_response(response, tmp_filename)
    #             filename_list.append(tmp_filename)
    #         else:
    #             print(f'[{response.status_code}] - Error during data retrieving (file "{tmp_filename}"). Server response: {response.content.decode("utf-8")}')

    #     return filename_list
    
    
    # def build_dataset_for_files(self, data_filenames):
    #     dss = [xr.open_dataset(df) for df in data_filenames]
    #     ds = xr.concat(dss, dim='time')
    #     ds = ds.assign_coords(
    #         lat=np.round(ds.lat.values, 4),
    #         lon=np.round(ds.lon.values, 4),
    #     )
    #     ds = ds.sortby('time')
    #     values = ds[self.variable_name]
    #     ds = ds.drop_dims('runtime')
    #     ds[self.variable_name] = values[0]
    #     ds = ds.drop_vars('runtime')
    #     return ds
    
    def retrieve_data(self, auth_token, lat_range, long_range, time_start, time_end=None, timedelta_minutes=5):

        delta_hours = timedelta_minutes // 60
        delta_minutes = (timedelta_minutes - (delta_hours * 60))

        time_start = time_start.replace(
            minute=(time_start.minute // delta_minutes) * delta_minutes if delta_hours == 0 else 0,
            second=0,
            microsecond=0
        )

        time_end = time_end.replace(
            hour=time_end.hour,
            minute=((time_end.minute // delta_minutes) * delta_minutes) if delta_hours == 0 else 0,
            second=0,
            microsecond=0
        ) if time_end is not None else None
        
        hypermeteo_response = self.hypermeteo_request(
            auth_token=auth_token,
            lat_range=lat_range,
            long_range=long_range,
            time_start=time_start,
            time_end=time_end
        )
        
        if not hypermeteo_response.ok:
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested time range')
            
        netcdf_filename = _processes_utils.get_netcdf_filename(
            dataset_name = self.dataset_name, 
            variable_name = self.variable_name, 
            lat_range = lat_range,
            lon_range = long_range, 
            time_value = time_start
        )
        tmp_filename = os.path.join(self._data_folder, netcdf_filename)
        _processes_utils.write_file_from_response(hypermeteo_response, tmp_filename)
        
        og_ds = xr.open_dataset(tmp_filename)
        ds = xr.Dataset(
            {
                self.variable_name: (["time", "lat", "lon"], og_ds[self.variable_name].values[0])   # DOC: Select data along the first element (always 1-dim related to model runtime)
            },
            coords={
                "time": og_ds.time,
                "lat": og_ds.lat.round(6),
                "lon": og_ds.lon.round(6)
            }
        )
        ds = ds.sortby(['time', 'lat', 'lon'])
        ds[self.variable_name] = xr.where(ds[self.variable_name] < 0, 0, ds[self.variable_name])
        ds = ds.resample(time=f'{timedelta_minutes}T').sum()
        return ds
            
        
    
    def create_timestamp_raster(self, dataset):
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')) for ts in dataset.time.values]
        
        merged_raster_filename = _processes_utils.get_raster_filename(
            self.dataset_name, self.variable_name, 
            None,
            None,
            (dataset.time.values[0], None)
        )
        merged_raster_filepath = os.path.join(self._data_folder, merged_raster_filename)
        
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
            auth_token, lat_range, long_range, time_start, time_end, time_delta, out_format = self.validate_parameters(data)
            
            # TODO: Check if strict time range is needed (then make a ping to service)
            
            timeserie_dataset = self.retrieve_data(
                auth_token = auth_token,
                lat_range = lat_range,
                long_range = long_range,
                time_start = time_start,
                time_end = time_end,
                timedelta_minutes = time_delta
            )
            
            # Compute cumulative sum
            timeserie_dataset = _processes_utils.compute_cumsum(timeserie_dataset, self.variable_name)
            
            # Query based on spatio-temporal range
            query_dataset = _processes_utils.dataset_query(timeserie_dataset, lat_range, long_range, [time_start, time_end])
            
            # Save to S3 Bucket - Merged timestamp multiband raster
            merged_raster_filepath = self.create_timestamp_raster(query_dataset)
            merged_raster_s3_uri = _processes_utils.save_to_s3_bucket(self.bucket_destination, merged_raster_filepath)
            
            # Update available data
            self.update_available_data(query_dataset, merged_raster_s3_uri)
                        
            # Prepare output dataset
            out_data = dict()
            if out_format is not None:
                out_dataset = _processes_utils.datasets_to_out_format(query_dataset, out_format, to_iso_format_columns=['time'])
                out_data = {'data': out_dataset}
                        
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
        
        _processes_utils.garbage_folders(self._data_folder)
        
        return mimetype, outputs

    def __repr__(self):
        return f'<NowRadarRainfallProcessor> {self.name}'