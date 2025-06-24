import os
import json
import jsonschema
import datetime
import requests

import pandas as pd
import geopandas as gpd

from saferplacesapi import _processes_utils
from saferplacesapi import _s3_utils
from saferplacesapi import _utils


class BarriersCollectionManager():
    """
    Class to manage the barriers collection.
    """

    def __init__(self):
        self.bucket_folder = 'Barriers'
        self.bucket_destination = f'{_s3_utils._base_bucket}/{self.bucket_folder}'
        
        self.data_folder = os.path.join(os.getcwd(), self.bucket_folder)
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        
        
        
    def validate_geojson(self, geojson):
        schema_url = "https://geojson.org/schema/GeoJSON.json"
        geojson_schema = requests.get(schema_url).json()
        try:
            jsonschema.validate(instance=geojson, schema=geojson_schema)
            return True
        except jsonschema.exceptions.ValidationError as e:
            return False
        
    
    def read_json(self, filepath):
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
        
        
        
    def get_barrier(self, barrier_id):
        """
        Get a barrier by its ID.
        """
        
        filename = f'{barrier_id}.geojson'
        filepath = os.path.join(self.data_folder, filename)
        fileuri = f'{self.bucket_destination}/{filename}'
        
        filepath = _s3_utils.s3_download(uri = fileuri, fileout=filepath)
        
        if filepath is None:
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, f'Barrier {barrier_id} not found in S3 bucket.')
        
        barrier = self.read_json(filepath)
           
        return barrier
    
    
    def post_barrier(self, barrier_id, barrier):
        """
        Post a barrier to the collection.
        """
        
        filename = f'{barrier_id}.geojson'
        filepath = os.path.join(self.data_folder, filename)
        
        # Save the barrier to a file
        with open(filepath, 'w') as f:
            json.dump(barrier, f)
        
        # Upload the file to S3
        fileuri = f'{self.bucket_destination}/{filename}'
        upload_ok = _s3_utils.s3_upload(filepath, fileuri)
        
        if not upload_ok:
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, f'Failed to upload barrier {barrier_id} to S3 bucket.')
        
        os.remove(filepath)
        return fileuri
    
    
    def delete_barrier(self, barrier_id):
        """
        Delete a barrier from the collection.
        """
        
        filename = f'{barrier_id}.geojson'
        delete_ok = _s3_utils.delete_s3_object(f'{self.bucket_destination}/{filename}')
        
        if not delete_ok:
            raise _processes_utils.Handle200Exception(_processes_utils.Handle200Exception.SKIPPED, f'Failed to delete barrier {barrier_id} from S3 bucket.')
        
        return True
    
    
    def update_barrier(self, barrier_id, barrier):
        """
        Update a barrier in the collection.
        """
        
        update_ok = self.post_barrier(barrier_id, barrier)
        return update_ok