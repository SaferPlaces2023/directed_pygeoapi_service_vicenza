# -----------------------------------------------------------------------------

import os
import json
import time
import gzip
import pickle
import logging
import datetime
import requests

import numpy as np
import pandas as pd

from scipy.spatial import Delaunay
from scipy.interpolate import LinearNDInterpolator

import xarray as xr

from flask import request
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
        'en': 'Adriac Sea Level Ingestor Process',
    },
    'description': {
        'en': 'Collect Sea Level data from Adriac'
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
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode',
            'schema': {
            }
        },
        'strict_time_range': {
            'title': 'Strict time range',
            'description': 'Enable strict time range to check data avaliability until requested end time. Can be valued as true or false. Default is false',
            'schema': {
            }
        },
        'forecast_run': {
            'title': 'Forecast run',
            'description': 'ADRIAC forecast runs (optional). If not provided, all the available forecast runs from current date will be considered. The forecast run must be a valid ISO format date string at hour 00:00:00 related to at least one week ago',
            'schema': {
                'type': 'iso-string or list of iso-string',
                'format': 'YYYY-MM-DDTHH:MM:SS or [YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS, ...]'
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
            "forecast_run": ["2025-03-18T00:00:00", "2025-03-18T12:00:00"]
        }
    }
}

# -----------------------------------------------------------------------------

class AdriacSeaLevelIngestorProcessor(BaseProcessor):
    """Adriac sea level ingestor process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'ADRIAC'
        self.variable_name = 'sea_level'
        
        self._data_provider_service = 'https://dati-simc.arpae.it/opendata/adriac/'
        
        self._data_folder = os.path.join(os.getcwd(), f'{self.dataset_name}_ingested_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}/{self.variable_name}' 
            
        self.load_adriac_ref_coords()
            
            
    def load_adriac_ref_coords(self):
        adriac_ref_coords_folder = os.path.join(os.getcwd(), 'src', 'saferplacesapi', 'data_providers', 'forecast', 'adriac_ref_coords')
        with open(os.path.join(adriac_ref_coords_folder, 'adriac_lat_rho_epgs4326.pickle'), 'rb') as fn:
            lat_rho = pickle.load(fn)
        with open(os.path.join(adriac_ref_coords_folder, 'adriac_lon_rho_epgs4326.pickle'), 'rb') as fn:
            lon_rho = pickle.load(fn)
        self.adriac_ref_coords = (lat_rho, lon_rho)
           
           
    def get_avaliable_forecast_runs(self):
        run_date = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        runs = [ run_date ]
        for _ in range(7):  # INFO: try one week back
            run_date -= datetime.timedelta(days=1)
            runs.append(run_date)
        runs = runs[::-1]
        request_file_names = self.get_adriac_data_filenames(runs)
        runs = [run for run, rf in zip(runs, request_file_names) if requests.head(f'{self._data_provider_service}/{rf}').status_code == 200]
        return runs


    def validate_parameters(self, data):
        strict_time_range = data.get('strict_time_range', False)
        requested_forecast_run = data.get('forecast_run', None)
        
        if strict_time_range is not None:
            if type(strict_time_range) is not bool:
                raise ProcessorExecuteError('strict_time_range must be a boolean')        
        
        if requested_forecast_run is not None:
            if type(requested_forecast_run) not in [str, list]:
                raise ProcessorExecuteError('Invalid input format for forecast_run parameter')
            if type(requested_forecast_run) == str:
                requested_forecast_run = [requested_forecast_run]
            for irfr, rfr in enumerate(requested_forecast_run):
                try:
                    rfr = datetime.datetime.fromisoformat(rfr)
                    if rfr.hour != 0 or rfr.minute != 0 or rfr.second != 0 or rfr.microsecond != 0:
                        raise ProcessorExecuteError(f'Invalid forecast run "{rfr.isoformat()}". Must be a valid 12h interval')
                    requested_forecast_run[irfr] = rfr
                except Exception as err:
                    raise ProcessorExecuteError(f'Invalid forecast run "{rfr}. Must be a valid ISO format date string')
        else:
            requested_forecast_run = self.get_avaliable_forecast_runs()
        
        return strict_time_range, requested_forecast_run


    def ping_avaliable_runs(self, forecast_datetime_runs):
        request_file_names = self.get_adriac_data_filenames(forecast_datetime_runs)
        for rf in request_file_names:
            response = requests.head(f'{self._data_provider_service}/{rf}')
            if response.status_code != 200:
                return False
        return True
    
 
    def get_adriac_data_filenames(self, forecast_datetime_runs):
        request_files = []        
        for fdr in forecast_datetime_runs:
            datetime_run_file = f'{fdr.year}{fdr.month:02d}{fdr.day:02d}_adriac_1km_qck_sl_fc.nc.gz'
            request_files.append(datetime_run_file)
        return request_files
    
    
    def download_and_extract_adriac_data(self, forecast_datetime_runs):
        request_file_names = self.get_adriac_data_filenames(forecast_datetime_runs)
        
        adriac_file_paths = []
        for rf in request_file_names:
            response = requests.get(f'{self._data_provider_service}/{rf}', stream=True)
            if response.status_code == 200:
                rf_filename = os.path.join(self._data_folder, rf)
                with open(rf_filename, "wb") as gz_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        gz_file.write(chunk)
                adriac_file_paths.append(rf_filename)
            else:
                print(f'Request error {response.status_code} with file "{rf}"')        
        
        adriac_extracted_file_paths = [afp_gz.replace('.gz', '') for afp_gz in adriac_file_paths]
        for afp_gz,afp_nc in zip(adriac_file_paths, adriac_extracted_file_paths):
            with gzip.open(afp_gz, 'rb') as f_in:
                with open(afp_nc, 'wb') as f_out:
                    f_out.write(f_in.read())
        
        return adriac_extracted_file_paths
    
    
    def adriac_time_concat(self, adriac_extracted_file_paths):
        # se ci sono altri dataset giornalieri sucessivi prendo solo prime 24 h (forecast ogni 10 min) altrimenti tutto il forecast disponibile (3 giorni)
        datasets = [xr.open_dataset(afp).isel(ocean_time=list(range(6*24 if ids<len(adriac_extracted_file_paths)-1 else 6*24*3))) for ids,afp in enumerate(adriac_extracted_file_paths)]
        timeserie_dataset = xr.concat(datasets, dim='ocean_time')
        timeserie_dataset = timeserie_dataset.sortby('ocean_time')
        return timeserie_dataset
    
    
    def adriac_transform_to_epgs3426(self, timeserie_dataset):
        lat_rho, lon_rho = self.adriac_ref_coords
        
        # Limiti geografici della nuova griglia (regolare)
        lat_min, lat_max = lat_rho.min(), lat_rho.max()
        lon_min, lon_max = lon_rho.min(), lon_rho.max()

        # Risoluzione desiderata della nuova griglia
        num_lat = 752  # Numero di punti in latitudine
        num_lon = 272  # Numero di punti in longitudine

        # Crea la griglia regolare
        lat_reg = np.linspace(lat_min, lat_max, num_lat)
        lon_reg = np.linspace(lon_min, lon_max, num_lon)
        lon_reg_mesh, lat_reg_mesh = np.meshgrid(lon_reg, lat_reg)
        
        # Dati da riproiettare
        original_data = timeserie_dataset.sea_level.to_numpy()
        
        points = np.column_stack((lat_rho.ravel(), lon_rho.ravel()))
        dest_points = np.column_stack((lat_reg_mesh.ravel(), lon_reg_mesh.ravel()))
        triangulation = Delaunay(points)

        cube_values = original_data.reshape(original_data.shape[0], -1)
        
        def interpolate_cube(triangulation, points, values, dest_points):
            interpolators = LinearNDInterpolator(triangulation, values.T)
            interpolated = interpolators(dest_points)
            return interpolated

        interpolated_cube = interpolate_cube(triangulation, points, cube_values, dest_points)
        interpolated_cube = interpolated_cube.T.reshape(original_data.shape[0], *lat_reg_mesh.shape)
        
        interpolated_dataset = xr.Dataset(
            {
                self.variable_name: (("time", "lat", "lon"), interpolated_cube)
            },
            coords={
                "time": timeserie_dataset.ocean_time.values,
                "lat": lat_reg,
                "lon": lon_reg
            }
        )
        interpolated_dataset = interpolated_dataset.assign_coords(
            lat=np.round(interpolated_dataset.lat.values, 6),
            lon=np.round(interpolated_dataset.lon.values, 6),
        )        
        interpolated_dataset = interpolated_dataset.sortby(['time', 'lat', 'lon'])
        
        return interpolated_dataset
            
            
    def upload_single_date_datasets(self, dataset):
        # Split ataset in multiple datasets by date
        dates = sorted(list(set(dataset.time.dt.date.values)))
        date_datasets = []
        for date in dates:
            subset = dataset.sel(time=dataset.time.dt.date == date)
            date_datasets.append((date, subset))
        
        # Upload datasets to S3 bucket
        date_dataset_uris = []
        for dt, ds in date_datasets:
            fn = f'{self.dataset_name}__{self.variable_name}__{dt}.nc'
            fp = os.path.join(self._data_folder, fn)
            ds.to_netcdf(fp)
            uri = os.path.join(self.bucket_destination, fn)
            _s3_utils.s3_upload(fp, uri)
            date_dataset_uris.append((dt, uri))
            
        return date_dataset_uris
        
    

    def execute(self, data):
        mimetype = 'application/json'
        
        outputs = {}
        try:
            strict_time_range, forecast_datetime_runs = self.validate_parameters(data)
            
            if strict_time_range:
                are_runs_avaliable = self.ping_avaliable_runs(forecast_datetime_runs)
                if not are_runs_avaliable:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested forecast runs')
            else: 
                forecast_datetime_runs = [fdr for fdr in forecast_datetime_runs if self.ping_avaliable_runs([fdr])]
            
            adriac_file_paths = self.download_and_extract_adriac_data(forecast_datetime_runs)
            
            timeserie_dataset = self.adriac_time_concat(adriac_file_paths)
            
            timeserie_dataset = self.adriac_transform_to_epgs3426(timeserie_dataset)
            
            date_dataset_uris = self.upload_single_date_datasets(timeserie_dataset)
            
            collected_data_info = [
                {
                    'date': dt.isoformat(), 
                    's3_uri': uri
                }
                for dt,uri in date_dataset_uris
            ]
            
            outputs = {
                'status': 'OK',
                'collected_data': collected_data_info
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
        
        _processes_utils.garbage_filepaths(adriac_file_paths)
        
        return mimetype, outputs

    def __repr__(self):
        return f'<AdriacSeaLevelIngestorProcessor> {self.name}'