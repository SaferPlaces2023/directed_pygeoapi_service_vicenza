# -----------------------------------------------------------------------------

import os
import json
import time
import logging
import datetime
import requests

import numpy as np
import pandas as pd

from shapely.geometry import Point # incompatibile con numpy 2.2.0 (?) -> usare geopandas.points_from_xy
import geopandas as gpd

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
        'en': 'ARPAE realtime data Process',
    },
    'description': {
        'en': 'Realtime data from ARPAE'
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
        'strict_time_range': {
            'title': 'Strict time range',
            'description': 'Enable strict time range to check data avaliability until requested end time. Can be valued as true or false. Default is false',
            'schema': {
            }
        },
        'variable_codes': {
            'title': 'Variable codes',
            'description': 'List of variable codes. Possible values are "B13011" (total_precipitation), "B13215" (river_level), "B13226" (river_discharge), "B22037" (tidal_national_ref), "B22070" (wave_height_significative), "B22073" (wave_height_max), "B22001" (wave_direction). If no variable code is provided, all variables will be returned',
            'schema': {
            }
        },
        'station_names': {
            'title': 'Station names',
            'description': 'Dictionary of station names. The dictionary keys must be variable codes and the values must be lists of station names. If no station names are provided, all stations will be returned',
            'schema': {
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "geojson" or "dataframe". "geojson" is default and preferable.',
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
            'description': 'S3 Uri of the station geojson file',
            'schema': {
            }
        },
        'type': {
            'title': 'Output type',
            'description': 'The output type. Value when "out_format"="geojson" is "FeatureCollection"',
            'schema': {
            }
        },
        'features': {
            'title': 'Features',
            'description': 'Present when "out_format"="geojson" and it is a list of geo-features with properties',
            'schema': {
            }
        },
        'metadata': {
            'title': 'Metadata',
            'description': 'Metdata of the measures. They contains code-name, related readble-name, unit and type',
            'schema': {
            }
        },
        'crs': {
            'title': 'Coordinate Reference System',
            'description': 'Coordinate Reference System of the output features, valued according to geojson standards. It is always "urn:ogc:def:crs:OGC:1.3:CRS84" (EPSG:4326)',
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
            "time_range": ["2025-03-18T09:00:00.000", "2025-03-18T14:00:00.000"], # strictly needed nearly current date
            "variable_codes": [
                "B13011",   # 'total_precipitation',
                "B13215",   # 'river_level',
                "B13226",   # 'river_discharge',
                "B22037",   # 'tidal_national_ref',
                "B22070",   # 'wave_height_significative'
                "B22001"    # 'wave_direction'
            ],
            "station_names": {  # INFO: filtering by station names will be based on case insensitive exact matching, if station names list related to a var is empty all data will be returned
                "B22037": [ "Cattolica Porto radar" ]
            },
            "out_format": "geojson"
        }
    }
}

# -----------------------------------------------------------------------------

class ArpaeRealtimeProcessor(BaseProcessor):
    """Arpae Realtime wave process plugin"""

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        
        self.dataset_name = 'ARPAE_realtime'
        
        self._data_provider_service_meteo = 'https://dati-simc.arpae.it/opendata/osservati/meteo/realtime/realtime.jsonl'
        self._data_provider_service_river = 'https://dati-simc.arpae.it/opendata/osservati/portata_istantanea/portata_istantanea.json'
        
        self._data_folder = os.path.join(os.getcwd(), 'arpae_realtime_retrieved_data')
        if not os.path.exists(self._data_folder):
            os.makedirs(self._data_folder)
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.dataset_name}'
        
        self.info_dict_code2name = {
            'B01019': 'station_name',
            'B01194': 'report_mnemonic',
            'B07030': 'station_height',

            'B05001': 'latitude',
            'B06001': 'longitude',

            'B04001': 'year',
            'B04002': 'month',
            'B04003': 'day',
            'B04004': 'hour',
            'B04005': 'minute',
            'B04006': 'second',
        }
        self.info_dict_name2code = {v:k for k,v in self.info_dict_code2name.items()}
        
        self.var_dict_code2name = {
            'B13011': 'total_precipitation',
            # 'B12101': 'temperature',
            # 'B10004': 'pressure',
            # 'B13003': 'relative_humidity',

            'B13215': 'river_level',
            'B13226': 'river_discharge', # unica a trovarsi in _data_provider_service_river

            'B22037': 'tidal_national_ref',
            # 'B22038': 'tidal_local_ref', # manca

            'B22070': 'wave_height_significative',
            'B22073': 'wave_height_max',
            'B22001': 'wave_direction'
        }
        self.var_dict_name2code = {v:k for k,v in self.var_dict_code2name.items()}
        self.var_dict_code2name_cumsum = {
            'B13011__CUMSUM': 'total_precipitation_cumsum',
            'B13226__CUMSUM': 'river_discharge_cumsum'
        }
        
        
        
    def validate_parameters(self, data):
        variable_codes = data.get('variable_codes', None)
        if variable_codes is None:
            raise ProcessorExecuteError('variable_codes parameter is required')
        elif type(variable_codes) is not list:
            raise ProcessorExecuteError('variable_codes parameter must be a list')
        elif len(variable_codes) < 1:
            raise ProcessorExecuteError('variable_codes parameter must be a list of at least 1 element')
        elif not all([type(x) is str for x in variable_codes]):
            raise ProcessorExecuteError('variable_codes parameter must be a list of strings') 
        elif not all([var_code in self.var_dict_code2name for var_code in variable_codes]):
            raise ProcessorExecuteError('variable_codes parameter must be a list of valid variable codes')
        
        station_names = data.get('station_names', None)
        if station_names is not None:
            if type(station_names) is not dict:
                raise ProcessorExecuteError('station_names parameter must be a dictionary')
            elif not all([k in self.var_dict_code2name for k in station_names.keys()]):
                raise ProcessorExecuteError('station_names parameter keys must be valid variable codes')
            elif not all([type(v) is list for v in station_names.values()]):
                raise ProcessorExecuteError('station_names parameter values must be lists')
            elif not all([all([type(x) is str for x in v]) for v in station_names.values()]):
                raise ProcessorExecuteError('station_names parameter values must be lists of strings')        
        
        lat_range, long_range, time_start, time_end, strict_time_range, out_format = _processes_utils.validate_parameters(data, out_type='vector')
        return variable_codes, station_names, lat_range, long_range, time_start, time_end, strict_time_range, out_format
    
    
    def request_arape_data(self, service_provider, out_file):
        try:
            response = requests.get(service_provider)
            response.raise_for_status()
            with open(out_file, "w", encoding="utf-8") as file:
                file.write(response.text)
        except requests.exceptions.RequestException as e:
            print(f"Errore durante il download del file: {e}")
        with open(out_file, "r", encoding="utf-8") as json_file:
            records = [json.loads(jl) for jl in json_file ]
        return records
    
    
    def parse_arpae_data(self, arpae_data):
        # Process info data part (data array at index 0)
        df_infos = pd.DataFrame([record['data'][0]['vars'] for record in arpae_data for _ in range(len(record['data'])-1)])

        missing_info_codes = [info_code for info_code in list(self.info_dict_code2name.keys()) if info_code not in df_infos.columns]
        present_info_codes = [info_code for info_code in list(self.info_dict_code2name.keys()) if info_code not in missing_info_codes]
        
        df_infos = df_infos[[info_code for info_code in present_info_codes]]

        for info_code in present_info_codes:
            df_infos[info_code] = df_infos[info_code].apply(
                lambda value:
                    value['v'] if value!=None and type(value) is dict and 'v' in value
                    else value if value!=None
                    else None
            )

        # Process variables data part (data array from index 1 to end)
        var_deafult_codes = ['timerange', 'level']
        df_vars = pd.DataFrame([v['vars'] | {v_default: v[v_default] for v_default in var_deafult_codes} for record in arpae_data for v in record['data'][1:]])

        missing_var_codes = [var_code for var_code in list(self.var_dict_code2name.keys()) if var_code not in df_vars.columns]
        present_var_codes = [var_code for var_code in list(self.var_dict_code2name.keys()) if var_code not in missing_var_codes]
        
        df_vars = df_vars[[var_code for var_code in present_var_codes] + var_deafult_codes]

        for var_code in present_var_codes:
            df_vars[var_code] = df_vars[var_code].apply(
                lambda value:
                    value['v'] if value!=None and type(value) is dict and 'v' in value
                    else value if value!=None
                    else None
            )

        # Concat
        df = pd.concat((df_infos, df_vars), axis=1)
        # Rename columns (no renames needed, try to be pure)
        # df = df.rename(columns = self.info_dict_code2name | self.var_dict_code2name)
        # Setting datetime
        df['date_time'] = df.apply(
            lambda record: datetime.datetime(
                record[self.info_dict_name2code['year']],
                record[self.info_dict_name2code['month']],
                record[self.info_dict_name2code['day']],
                record[self.info_dict_name2code['hour']],
                record[self.info_dict_name2code['minute']],
                record[self.info_dict_name2code['second']]
            ), axis=1
        )
        df = df.drop([
            self.info_dict_name2code['year'],
            self.info_dict_name2code['month'],
            self.info_dict_name2code['day'],
            self.info_dict_name2code['hour'],
            self.info_dict_name2code['minute'],
            self.info_dict_name2code['second']
        ], axis=1)
        # Create geometry
        df['geometry'] = df.apply(
            lambda record: Point(
                record[self.info_dict_name2code['longitude']], 
                record[self.info_dict_name2code['latitude']]
            ), axis=1
        )
        # To GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        return gdf
    
    
    def join_arpae_data(self, arpae_meteo_gdf, arpae_river_gdf):
        arpae_meteo_gdf['level_key'] = arpae_meteo_gdf['level'].apply(lambda v: '_'.join([str(x) for x in v]))
        arpae_meteo_gdf['timerange_key'] = arpae_meteo_gdf['timerange'].apply(lambda v: '_'.join([str(x) for x in v]))

        arpae_river_gdf['level_key'] = arpae_river_gdf['level'].apply(lambda v: '_'.join([str(x) for x in v]))
        arpae_river_gdf['timerange_key'] = arpae_river_gdf['timerange'].apply(lambda v: '_'.join([str(x) for x in v]))

        # full outer join inclusivo
        arpae_gdf = arpae_meteo_gdf.merge(
            arpae_river_gdf,
            on=[
                self.info_dict_name2code['station_name'],
                self.info_dict_name2code['report_mnemonic'],
                'date_time',
                'timerange_key',
                'level_key',
                'geometry'
            ], 
            how='outer',
            suffixes=('', '_gdf2')
        )

        columns_to_update = [col for col in arpae_river_gdf.columns if col in arpae_meteo_gdf.columns]
        for col in columns_to_update:
            col_gdf2 = f"{col}_gdf2"
            if col_gdf2 in arpae_gdf.columns:
                arpae_gdf[col] = arpae_gdf[col].combine_first(arpae_gdf[f"{col}_gdf2"])

        columns_to_drop = [col for col in arpae_gdf.columns if col.endswith('_gdf2')] + ['level_key', 'timerange_key']
        arpae_gdf = arpae_gdf.drop(columns=columns_to_drop)
        
        return arpae_gdf
    
    
    def download_arape_data(self, variable_codes):
        arpae_meteo_gdf, arpae_river_gdf = None, None
        if len([var_code for var_code in variable_codes if var_code!=self.var_dict_name2code['river_discharge']]) > 0: # meteo data
            arpae_meteo_data = self.request_arape_data(self._data_provider_service_meteo, os.path.join(self._data_folder, 'arpae_meteo_data.json'))
            arpae_meteo_gdf = self.parse_arpae_data(arpae_meteo_data)
        if self.var_dict_name2code['river_discharge'] in variable_codes: # river data
            arpae_river_data = self.request_arape_data(self._data_provider_service_river, os.path.join(self._data_folder, 'arpae_river_data.json'))
            arpae_river_gdf = self.parse_arpae_data(arpae_river_data)
        if  arpae_meteo_gdf is not None and arpae_river_gdf is not None:
            arpae_gdf = self.join_arpae_data(arpae_meteo_gdf, arpae_river_gdf)
            return arpae_gdf
        elif arpae_meteo_gdf is not None:
            return arpae_meteo_gdf
        elif arpae_river_gdf is not None:
            return arpae_river_gdf
        
        
    def ping_variables_avaliable_datetime(self, arpae_geodataframe, variable_codes, time_end):
        avaliable_variable_codes = []
        for var_code in variable_codes:
            if var_code in arpae_geodataframe.columns:
                gdf_var = arpae_geodataframe[arpae_geodataframe[var_code].notnull()]
                if not gdf_var.empty and gdf_var.date_time.max() >= time_end:
                    avaliable_variable_codes.append(var_code)
        return avaliable_variable_codes
    
    
    def process_arpae_data_variables(self, arpae_gdf, variable_codes):
        def arpae_subset_by_var_name(gdf, var_code):
            arpae_subset = gdf[gdf[var_code].notnull()]
            arpae_subset = arpae_subset.drop(columns=[vc for vc in list(self.var_dict_code2name.keys()) if vc!=var_code and vc in gdf.columns])    
            arpae_subset = arpae_subset.sort_values(by=['geometry', 'date_time'])
            return arpae_subset
        
        arape_variables_geodataframes = {}
        for var_code in variable_codes:
            if var_code in self.var_dict_code2name:
                if var_code in arpae_gdf.columns:
                    
                    gdf_var = arpae_subset_by_var_name(arpae_gdf, var_code)
                    if self.var_dict_code2name[var_code] == 'total_precipitation': # B13011
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [1,0,900])] # cumulative data every 15 minutes
                        data_var[f'{var_code}__CUMSUM'] = data_var.groupby('geometry')[var_code].cumsum()
                    elif self.var_dict_code2name[var_code] == 'river_level': # B13215
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [254,0,0])] # istantaneous data (every 15 or 30 minutes)
                    elif self.var_dict_code2name[var_code] == 'river_discharge': # B13226
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [254,0,0])] # istantaneous data (every 15 or 30 minutes)
                        data_var[f'{var_code}__CUMSUM'] = data_var.groupby('geometry')[var_code].cumsum()
                    elif self.var_dict_code2name[var_code] == 'tidal_national_ref': # B22037
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [0,0,600])] # average data every 10 minutes
                    elif self.var_dict_code2name[var_code] == 'wave_height_significative': # B22070
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [0,0,1800])] # average data every 30 minutes
                    elif self.var_dict_code2name[var_code] == 'wave_height_max': # B22073
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [0,0,1800])] # average data every 30 minutes
                    elif self.var_dict_code2name[var_code] == 'wave_direction': # B22001
                        data_var = gdf_var[gdf_var.timerange.apply(lambda trv: trv == [0,0,1800])] # average data every 30 minutes
                    
                    arape_variables_geodataframes[var_code] = data_var
                else:
                    LOGGER.warning(f"Variable code {var_code} not found arpae data")
                    arape_variables_geodataframes[var_code] = None
            else:
                LOGGER.warning(f"Variable code {var_code} not listed in {self.__repr__} variable codes")
                arape_variables_geodataframes[var_code] = None
                
        return arape_variables_geodataframes
    
    
    def create_var_stations_geojson(self, variable_geodataframes):
        variable_geojson_filepaths = {}
        for var_code, dataset in variable_geodataframes.items():
            if not dataset.empty:
                gdf_var = dataset.copy()
                features = []
                gdf_gr_st = gdf_var.groupby(self.info_dict_name2code['station_name'])
                for idx, (station, station_gdf) in enumerate(gdf_gr_st):
                    feature = {
                        "id": _utils.string_to_int_id(station), # INFO: Base62 conversion (biunivocal relation)
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                station_gdf[self.info_dict_name2code['longitude']].iloc[0],
                                station_gdf[self.info_dict_name2code['latitude']].iloc[0],
                            ]
                        },
                        "properties": {
                            self.info_dict_name2code['station_name']: station,
                            self.info_dict_name2code['report_mnemonic']: station_gdf[self.info_dict_name2code['report_mnemonic']].iloc[0],
                            self.info_dict_name2code['station_height']: station_gdf[self.info_dict_name2code['station_height']].iloc[0],
                            var_code: station_gdf.apply(lambda row: [row['date_time'].isoformat(), row[var_code]], axis=1).to_list()
                        }
                    }
                    features.append(feature)
                station_geojson = {
                    "type": "FeatureCollection",
                    "features": features,
                    'metadata': {
                        'field': [meta for meta in self.build_metadata() if meta['@name'] in self.info_dict_code2name or meta['@name'] == var_code]
                    },
                    'crs': self.build_crs()
                }
                station_geojson_filename = _processes_utils.get_geojson_filename(self.dataset_name, var_code, None, None, (None, gdf_var.date_time.max()))
                station_geojson_filepath = os.path.join(self._data_folder, station_geojson_filename)
                with open(station_geojson_filepath, 'w') as f:
                    json.dump(station_geojson, f)
                variable_geojson_filepaths[var_code] = station_geojson_filepath
        return variable_geojson_filepaths
    
    
    def arpae_variable_query_geodataframes(self, variable_geodataframes, lat_range, long_range, time_range, station_names=None):
        variable_query_geodataframes = {}
        for var_code, geodataframe in variable_geodataframes.items():
            if geodataframe is not None:
                # Filter by lat, long, time ranges
                query_geodataframe = _processes_utils.geodataframe_query(geodataframe, lat_range, long_range, time_range)
                # Filter by station names
                if station_names is not None and var_code in station_names and len(station_names[var_code]) > 0:
                    var_code_station_names = station_names[var_code]
                    var_code_station_names = [v.lower() for v in var_code_station_names]
                    query_geodataframe = query_geodataframe[query_geodataframe[self.info_dict_name2code['station_name']].str.lower().isin(var_code_station_names)]                
                variable_query_geodataframes[var_code] = query_geodataframe
            else:
                variable_query_geodataframes[var_code] = None
        return variable_query_geodataframes
    
    
    def arpae_variable_geodataframes_to_out_format(self, variable_geodataframes, out_format):
        if out_format == 'geojson':
            variables = [var for var in list(variable_geodataframes.keys()) if variable_geodataframes[var] is not None]
            tmp_dfs = {}
            for var_code in variables:
                df = variable_geodataframes[var_code].copy()
                df['level'] = df['level'].apply(lambda lvl: '_'.join([str(v) for v in lvl]))
                df['timerange'] = df['timerange'].apply(lambda tr: '_'.join([str(v) for v in tr]))
                df['date_time'] = df['date_time'].apply(lambda dt: dt.isoformat())   
                tmp_dfs[var_code] = df         
            merged_df = tmp_dfs[variables[0]]
            for var_code in variables[1:]:
                merged_df = merged_df.merge(
                    tmp_dfs[var_code], how='outer', 
                    on=[
                        self.info_dict_name2code['station_name'],
                        self.info_dict_name2code['report_mnemonic'],
                        self.info_dict_name2code['station_height'],
                        self.info_dict_name2code['latitude'],
                        self.info_dict_name2code['longitude'],
                        'geometry', 
                        'level', 
                        'timerange',
                        'date_time'
                    ], 
                    suffixes=('', '_')
                )
            df_g_stations = merged_df.groupby(self.info_dict_name2code['station_name'])
            feature_collection = []
            for st_idx, (st, df_st) in enumerate(df_g_stations):
                nan_cols = df_st.columns[df_st.isna().all()]
                df_st.drop(columns=nan_cols, inplace=True)
                feature = {
                    "id": st_idx,
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            df_st[self.info_dict_name2code['longitude']].iloc[0],
                            df_st[self.info_dict_name2code['latitude']].iloc[0]
                        ]
                    },
                    "properties": {
                        self.info_dict_name2code['station_name']: st,
                        self.info_dict_name2code['report_mnemonic']: df_st[self.info_dict_name2code['report_mnemonic']].iloc[0],
                        self.info_dict_name2code['station_height']: df_st[self.info_dict_name2code['station_height']].iloc[0],
                        ** {
                            var_code: [(dt,val) for dt,val in list(zip(df_st['date_time'], df_st[var_code])) if not (np.isnan(val) or val is None)] if var_code in df_st.columns and not df_st[var_code].isna().all() else None
                            for var_code in variables
                        }
                    }
                }
                feature_collection.append(feature)
            return feature_collection            
        else:
            variable_out_formatted_geodataframes = {}
            for var_code, geodataframe in variable_geodataframes.items():
                if geodataframe is not None:
                    out_formatted_geodataframe = _processes_utils.datasets_to_out_format(geodataframe, out_format, to_iso_format_columns=['date_time'])
                    variable_out_formatted_geodataframes[var_code] = out_formatted_geodataframe
                else:
                    variable_out_formatted_geodataframes[var_code] = None
            return variable_out_formatted_geodataframes
        
        
    def build_metadata(self):
        info_metadata = [
            {
                "@name": self.info_dict_name2code['station_name'],
                "@alias": "station_name"
            }, 
            {
                "@name": self.info_dict_name2code['report_mnemonic'],
                "@alias": "report_mnemonic"
            }, 
            {
                "@name": self.info_dict_name2code['station_height'],
                "@alias": "station_height"
            }
        ]        
        variable_metadata = [
            {
                '@name': self.var_dict_name2code['total_precipitation'],
                '@alias': 'total_precipitation',
                '@unit': 'mm',
                '@type': 'rainfall'
            },
            {
                '@name': self.var_dict_name2code['river_level'],
                '@alias': 'river_level',
                '@unit': 'm',
                '@type': 'level'
            },
            {
                '@name': self.var_dict_name2code['river_discharge'],
                '@alias': 'river_discharge',
                '@unit': 'm**3/s',
                '@type': 'discharge'
            },
            {
                '@name': self.var_dict_name2code['tidal_national_ref'],
                '@alias': 'tidal_national_ref',
                '@unit': 'm',
                '@type': 'tidal'
            },
            {
                '@name': self.var_dict_name2code['wave_height_significative'],
                '@alias': 'wave_height_significative',
                '@unit': 'm',
                '@type': 'tidal'
            },
            {
                '@name': self.var_dict_name2code['wave_height_max'],
                '@alias': 'wave_height_max',
                '@unit': 'm',
                '@type': 'tidal'
            },
            {
                '@name': self.var_dict_name2code['wave_direction'],
                '@alias': 'wave_direction',
                '@unit': 'degree',
                '@type': 'wave_direction'
            }
        ]
        field_metadata = info_metadata + variable_metadata
        return field_metadata
        
    def build_crs(self):
        return {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
            }
        }
    
    
    def update_available_data(self, geojson_filepaths, s3_uris):
        
        non_none_vals = lambda vals: list(filter(lambda x: x is not None, vals))
        last_properties_vals = lambda feature_collection, var_code: non_none_vals([f['properties'][var_code][-1][1] for f in feature_collection['features']])
        
        for var_code, geojson_filepath in geojson_filepaths.items():
            if geojson_filepath is not None:
                with open(geojson_filepath, 'r') as f:
                    feature_collection = json.load(f)
                if self.var_dict_code2name[var_code] == 'total_precipitation': # B13011
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }               
                elif self.var_dict_code2name[var_code] == 'river_level': # B13215
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }                
                elif self.var_dict_code2name[var_code] == 'river_discharge': # B13226
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }       
                elif self.var_dict_code2name[var_code] == 'tidal_national_ref': # B22037
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }
                elif self.var_dict_code2name[var_code] == 'wave_height_significative': # B22070
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }
                elif self.var_dict_code2name[var_code] == 'wave_height_max': # B22073
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }
                elif self.var_dict_code2name[var_code] == 'wave_direction': # B22001
                    kw_features = {
                        'max': np.nanmax(last_properties_vals(feature_collection, var_code)).item(),
                        'mean': np.nanmean(last_properties_vals(feature_collection, var_code)).item(),
                    }
                else:
                    kw_features = dict()

                _ = _processes_utils.update_avaliable_data(
                    provider=self.dataset_name,
                    variable=var_code,
                    datetimes=datetime.datetime.fromisoformat(max([f['properties'][var_code][-1][0] for f in feature_collection['features']])),
                    s3_uris=s3_uris[var_code],
                    kw_features=kw_features
                )
                _ = _processes_utils.update_avaliable_data_HIVE(        # DOC: Shoud be the only and final way
                    provider=self.dataset_name,
                    variable=var_code,
                    datetimes=datetime.datetime.fromisoformat(max([f['properties'][var_code][-1][0] for f in feature_collection['features']])),
                    s3_uris=s3_uris[var_code],
                    kw_features=kw_features
                )
    
    
    def execute(self, data):
        mimetype = 'application/json'
        
        
        outputs = {}
        try:
            variable_codes, station_names, lat_range, long_range, time_start, time_end, strict_time_range, out_format = self.validate_parameters(data)
            
            # Retrieve data from ARPAE
            arpae_geodataframe = self.download_arape_data(variable_codes)
            
            # Check if data is available
            if strict_time_range:
                avaliable_variable_codes = self.ping_variables_avaliable_datetime(arpae_geodataframe, variable_codes, time_end)
                if len(avaliable_variable_codes) == 0:
                    raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, 'No data available for the requested time range')
                unvaliable_variable_codes_info = { 'unvaliable_variable_codes': [var_code for var_code in variable_codes if var_code not in avaliable_variable_codes] }
            else:
                avaliable_variable_codes = variable_codes
                unvaliable_variable_codes_info = {}
            
            # Aggregate all datasets
            arpae_variable_geodataframes = self.process_arpae_data_variables(arpae_geodataframe, avaliable_variable_codes)
            
            # Query based on spatio-temporal range
            arpae_variable_query_geodataframes = self.arpae_variable_query_geodataframes(arpae_variable_geodataframes, lat_range, long_range, [time_start, time_end], station_names)
            
            # Generate station geojson files + upload to s3
            var_station_geojson_filepaths = self.create_var_stations_geojson(arpae_variable_query_geodataframes)
            var_station_geojson_s3_uris = {var_code: _processes_utils.save_to_s3_bucket(os.path.join(self.bucket_destination, var_code), var_station_geojson_filepath) for var_code, var_station_geojson_filepath in var_station_geojson_filepaths.items()}
            
            # Update available data
            self.update_available_data(var_station_geojson_filepaths, var_station_geojson_s3_uris)
            
            # Prepare output datasets
            out_data = dict()
            if out_format is not None:
                arpae_variable_query_geodataframes = self.arpae_variable_geodataframes_to_out_format(arpae_variable_query_geodataframes, out_format)
                if out_format == 'geojson':
                    out_data = {
                        "type": "FeatureCollection",
                        "features": arpae_variable_query_geodataframes
                    }
                else:
                    out_data = {'geodataframes': arpae_variable_query_geodataframes}
                out_data['metadata'] = {
                    'field': self.build_metadata()
                }
                out_data['crs'] = self.build_crs()
            
            outputs = {
                'status': 'OK',
                
                's3_uris': var_station_geojson_s3_uris,
                
                **unvaliable_variable_codes_info,
                
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
        
        _processes_utils.garbage_filepaths(list(var_station_geojson_filepaths.values()))
        
        return mimetype, outputs

    def __repr__(self):
        return f'<ArpaeRealtimeProcessor> {self.name}'