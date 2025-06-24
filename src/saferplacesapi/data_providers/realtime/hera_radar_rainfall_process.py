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
        'en': 'Radar Rainfall Process',
    },
    'description': {
        'en': 'Radar Rainfall data from Hypermeteo API'
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
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least one week ago. If no time range is provided, all times will be returned',
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

# -----------------------------------------------------------------------------

class HeraRadarRainfallProcessor(BaseProcessor):
    """Hera Radar Rainfall process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'RADAR_HERA_150M_5MIN'
        self.variable_name = 'rainrate'
        
        self._data_provider_auth_service = 'https://api.hypermeteo.com/auth-b2b/authenticate'
        self._data_provider_service = "https://api.hypermeteo.com/b2b-binary/ogc/geoserver/wcs"     
        self._data_folder = os.path.join(os.getcwd(), 'hypermeteo_retrieved_data')
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
        
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data)
        
        if time_end is not None:
            if time_start - time_end > datetime.timedelta(hours=6):
                raise ProcessorExecuteError('Time range must be less than 6 hours')
            
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)

        return auth_token, lat_range, long_range, time_start, time_end, time_delta, strict_time_range, out_format
     
        
    def hypermeteo_request(self, auth_token, lat_range, long_range, time_start, time_end=None, data_variable='RADAR_HERA_150M_5MIN__rainrate'):
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
            "coverageId": data_variable,
            "format": "application/x-netcdf",
            "subset": [long_subset, lat_subset, time_subset]
        }
        response = requests.get(self._data_provider_service, headers=headers, params=params)
        return response
    
    
    def ping_avaliable_datetime(self, auth_token, lat_range, long_range, moment, data_variable='RADAR_HERA_150M_5MIN__rainrate'):
        response = self.hypermeteo_request(
            auth_token = auth_token,
            lat_range = lat_range,
            long_range = long_range,
            time_start = moment,
            time_end = None,
            data_variable = data_variable
        )
        if response.headers['Content-Type'] == 'application/x-netcdf':
            return True
        return False

            
    def retrieve_data(self, auth_token, lat_range, long_range, time_start, time_end=None, time_delta=5, data_variable='RADAR_HERA_150M_5MIN__rainrate'):
        delta_hours = time_delta // 60
        delta_minutes = (time_delta - (delta_hours*60))

        datetime_list = []
        if time_end is not None:
            current_time = time_start
            while current_time <= time_end:
                datetime_list.append(current_time)
                current_time += datetime.timedelta(hours=delta_hours, minutes=delta_minutes)
            if datetime_list[-1] != time_end:
                datetime_list.append(time_end)
        else:
            datetime_list.append(time_start)
                
        s3_prep_filepaths = _s3_utils.list_s3_files(self.bucket_destination, filename_prefix=f'{self.dataset_name}__{self.variable_name}')
        s3_prep_filenames = [os.path.basename(s3_filepath) for s3_filepath in s3_prep_filepaths]
        
        s3_prep_list = []
        filename_list = []
        
        raster_filenames = []
        for moment in datetime_list:
            
            raster_filename = _processes_utils.get_raster_filename(self.dataset_name, self.variable_name, lat_range, long_range, moment)
            raster_filenames.append(raster_filename)
            if raster_filename in s3_prep_filenames:
                s3_prep_list.append(os.path.join(self.bucket_destination, raster_filename))
            else:
                response = self.hypermeteo_request(
                    auth_token = auth_token,
                    lat_range = lat_range,
                    long_range = long_range,
                    time_start = moment,
                    time_end = None,
                    data_variable = data_variable
                )
                
                netcdf_filename = _processes_utils.get_netcdf_filename(self.dataset_name, self.variable_name, lat_range, long_range, moment)
                tmp_filename = f'{self._data_folder}/{netcdf_filename}'
                if response.ok:
                    _processes_utils.write_file_from_response(response, tmp_filename)
                    filename_list.append(tmp_filename)
                else:
                    print(f'[{response.status_code}] - Error during data retrieving (file "{tmp_filename}"). Server response: {response.content.decode("utf-8")}')
        
        return s3_prep_list, filename_list
    
    
    def concat_rasters_to_netcdf(self, raster_paths):
        raster_paths = [raster_paths] if type(raster_paths) not in (list, tuple) else raster_paths
        
        # Check if file is from s3
        for irp,rp in enumerate(raster_paths):
            if _s3_utils.iss3(rp):
                raster_paths[irp] = os.path.join(self._data_folder, os.path.basename(rp))
                _s3_utils.s3_download(rp, raster_paths[irp])
        
        raster_paths = sorted(raster_paths, key=lambda rp: os.path.basename(rp).split('__')[-1].replace('.cog','').replace('.tif',''))
        
        band_timestamps = [os.path.basename(rp).split('__')[-1].replace('.cog','').replace('.tif','') for rp in raster_paths]
        rasters = [rxr.open_rasterio(raster, masked=True) for raster in raster_paths]
        
        bounds = gpd.GeoDataFrame(geometry=[box(*raster.rio.bounds()) for raster in rasters]).total_bounds
        
        n_lat = np.mean([raster.rio.shape[0] for raster in rasters]).astype(np.int32)
        n_lon = np.mean([raster.rio.shape[1] for raster in rasters]).astype(np.int32)
        common_lon = np.linspace(bounds[0], bounds[2], n_lon)
        common_lat = np.linspace(bounds[1], bounds[3], n_lat)
        for r_idx,raster in enumerate(rasters):
            if len(raster.x.values) != len(common_lon) or len(raster.y.values) != len(common_lat) or \
                any(raster.x.values != common_lon) or any(raster.y.values != common_lat):
                rasters[r_idx] = raster.interp(x=common_lon, y=common_lat, method='nearest')
        
        rasters_dataset = xr.concat(rasters, dim="time")
        rasters_dataset["time"] = [datetime.datetime.fromisoformat(bts) for bts in band_timestamps]
        rasters_dataset = rasters_dataset.rename({'x': 'lon', 'y': 'lat'})
        rasters_dataset = rasters_dataset.assign_coords(
            lat=np.round(rasters_dataset.lat.values, 6),
            lon=np.round(rasters_dataset.lon.values, 6),
        )
        rasters_dataset = rasters_dataset.sortby('time')
        rasters_dataset = rasters_dataset.to_dataset(name=self.variable_name)
        rasters_dataset = rasters_dataset.isel(band=0).drop_vars('band') # drop dim band
        return rasters_dataset        
    
    
    def create_timestamp_raster(self, dataset):
        timestamps = [datetime.datetime.fromisoformat(str(ts).replace('.000000000','')) for ts in dataset.time.values]
        
        merged_raster_filename = _processes_utils.get_raster_filename(
            self.dataset_name, self.variable_name, 
            None,
            None,
            (None, dataset.time.values[-1])
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
            datetimes=dataset.time.max().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=-1)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=-1)[self.variable_name].mean(skipna=True).item()
            },
        )
        _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
            provider=self.dataset_name,
            variable=self.variable_name,
            datetimes=dataset.time.max().item(),
            s3_uris=s3_uri,
            kw_features={
                'max': dataset.isel(time=-1)[self.variable_name].max(skipna=True).item(),
                'mean': dataset.isel(time=-1)[self.variable_name].mean(skipna=True).item()
            },
        )
    

    def execute(self, data):
        mimetype = 'application/json'
        

        outputs = {}
        try:
            auth_token, lat_range, long_range, time_start, time_end, time_delta, strict_time_range, out_format = self.validate_parameters(data)
            
            # Check if data is avaliable for the requested time range
            if strict_time_range:
                is_data_avaliable = self.ping_avaliable_datetime(auth_token, lat_range, long_range, time_end)
                if not is_data_avaliable:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested time range')
            
            # Retrieve data from Hypermeteo API and S3 Bucket if already exists
            s3_prep_filenames, data_filenames = self.retrieve_data(
                auth_token = auth_token,
                lat_range = lat_range,
                long_range = long_range,
                time_start = time_start,
                time_end = time_end,
                time_delta = time_delta     # ???: Maybe time_delta should be used in resmapling queird subset otherwis we will miss data if it is not the minimum (5min)
            )
            
            # Prepare hyeprmeteo retrieved dataset + s3 upload single timestamp tif
            retieved_dataset = None            
            if len(data_filenames) > 0:
                retieved_dataset = _processes_utils.concat_netcdf(data_filenames)
                raster_filepaths = _processes_utils.dataset_to_rasters(retieved_dataset, dataset_name=self.dataset_name, variable_name=self.variable_name, out_folder=os.path.join(self._data_folder, 'raster_cogs'))
                _processes_utils.save_to_s3_bucket(self.bucket_destination, raster_filepaths)
            
            # Retrive s3 raster + concat and interp like retrieved ones
            s3_prep_dataset = None
            if len(s3_prep_filenames) > 0:
                s3_prep_dataset = self.concat_rasters_to_netcdf(s3_prep_filenames)
                if retieved_dataset is not None:
                    s3_prep_dataset = s3_prep_dataset.interp(lat=retieved_dataset.lat, lon=retieved_dataset.lon, method='nearest')
            
            
            # Aggregate all datasets (if any)
            timeserie_dataset = _processes_utils.concat_netcdf([ds for ds in [s3_prep_dataset, retieved_dataset] if ds is not None])
            
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
        
        _processes_utils.garbage_filepaths(data_filenames, s3_prep_filenames, merged_raster_filepath)
        
        return mimetype, outputs

    def __repr__(self):
        return f'<HeraRadarRainfallProcessor> {self.name}'
    
# -----------------------------------------------------------------------------