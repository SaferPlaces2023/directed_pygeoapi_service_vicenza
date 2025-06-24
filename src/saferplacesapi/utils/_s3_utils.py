import os
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import hashlib
from tempfile import tempdir

import logging

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)  # Se usi S3, anche questo può essere utile

LOGGER = logging.getLogger(__name__)


from . import _utils as utils


_base_bucket = f"s3://{os.environ.get('DEFAULT_BUCKET')}/{os.environ.get('DEFAULT_REMOTE_DIR')}"


def iss3(filename):
    """
    iss3
    """
    return filename and isinstance(filename, str) and \
        (filename.startswith("s3:/") or filename.startswith("/vsis3/"))


def get_bucket_name_key(uri):
    """
    get_bucket_name_key - get bucket name and key name from uri
    """
    bucket_name, key_name = None, None
    if not uri:
        pass
    elif uri.startswith("s3://"):
        # s3://saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("s3:/"):
        # s3:/saferplaces.co/tests/rimini/dem.tif
        _, bucket_name, key_name = uri.split("/", 2)
    elif uri.startswith("/vsis3/"):
        # /vsis3/saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("https://s3.amazonaws.com/"):
        _, _, bucket_name, key_name = uri.split("/", 3)
    else:
        bucket_name, key_name = None, uri
    return bucket_name, key_name


def etag(filename, client=None, chunk_size=8 * 1024 * 1024):
    """
    calculates a multipart upload etag for amazon s3
    Arguments:
    filename   -- The file to calculate the etag for
    """
    if filename and os.path.isfile(filename):
        md5 = []
        with open(filename, 'rb') as fp:
            while True:
                data = fp.read(chunk_size)
                if not data:
                    break
                md5.append(hashlib.md5(data))
        if len(md5) == 1:
            return f"{md5[0].hexdigest()}"
        digests = b''.join(m.digest() for m in md5)
        digests_md5 = hashlib.md5(digests)
        return f"{digests_md5.hexdigest()}-{len(md5)}"

    elif filename and iss3(filename):
        uri = filename
        ETag = ""
        try:
            bucket_name, key_name = get_bucket_name_key(uri)
            if bucket_name and key_name:
                client = get_client(client)
                ETag = client.head_object(Bucket=bucket_name, Key=key_name)[
                    'ETag'][1:-1]
        except ClientError as ex:
            # print(f"ETAG:{ex}")
            ETag = ""
        except NoCredentialsError as ex:
            print(ex)
            ETag = ""
        return ETag
    else:
        return ""


def get_client(client=None):
    """
    get_client
    """
    return client if client else boto3.client('s3', region_name='us-east-1')


def tempname4S3(uri):
    """
    tempname4S3
    """
    dest_folder = tempdir("s3")
    uri = uri if uri else ""
    if uri.startswith("s3://"):
        tmp = uri.replace("s3://", dest_folder + "/")
    if uri.startswith("s3:/"):
        tmp = uri.replace("s3:/", dest_folder + "/")
    elif uri.startswith("/vsis3/"):
        tmp = uri.replace("/vsis3/", dest_folder + "/")
    else:
        _, path = os.path.splitdrive(uri)
        tmp = utils.normpath(dest_folder + "/" + path)
    
    os.makedirs(utils.justpath(tmp), exist_ok=True)
    return tmp


def s3_equals(file1, file2, client=None):
    """
    s3_equals - check if s3 object is equals to local file
    """
    etag1 = etag(file1, client)
    etag2 = etag(file2, client)
    if etag1 and etag2:
        return etag1 == etag2
    return False


def s3_download(uri, fileout=None, remove_src=False, client=None):
    """
    Download a file from an S3 bucket
    """
    bucket_name, key = get_bucket_name_key(uri)
    if bucket_name:
        try:
            # check the cache
            client = get_client(client)

            if key and not key.endswith("/"):

                if not fileout:
                    fileout = tempname4S3(uri)

                if os.path.isdir(fileout):
                    fileout = f"{fileout}/{utils.justfname(key)}"

                if os.path.isfile(fileout) and s3_equals(uri, fileout, client):
                    print(f"using cached file {fileout}")
                else:
                    # Download the file
                    print(f"downloading {uri} into {fileout}...")
                    os.makedirs(utils.justpath(fileout), exist_ok=True)
                    client.download_file(
                        Filename=fileout, Bucket=bucket_name, Key=key)
                    if remove_src:
                        client.delete_object(Bucket=bucket_name, Key=key)
            else:
                objects = client.list_objects_v2(
                    Bucket=bucket_name, Prefix=key)['Contents']
                for obj in objects:
                    pathname = obj['Key']
                    if not pathname.endswith("/"):
                        dst = fileout
                        pathname = pathname.replace(key, "")
                        s3_download(f"{uri.rstrip('/')}/{pathname}",
                                    f"{dst}/{pathname}", client)

        except ClientError as ex:
            print(ex)
            return None
        except NoCredentialsError as ex:
            print(ex)
            return None

    return fileout if os.path.isfile(fileout) else None


def s3_upload(filename, uri, remove_src=False, client=None):
    """
    Upload a file to an S3 bucket
    Examples: s3_upload(filename, "s3://saferplaces.co/a/rimini/lidar_rimini_building_2.tif")
    """

    # Upload the file
    try:
        bucket_name, key = get_bucket_name_key(uri)
        if bucket_name and key and filename and os.path.isfile(filename):
            client = get_client(client)
            if s3_equals(uri, filename, client):
                print(f"file {filename} already uploaded")
            else:
                print(f"uploading {filename} into {bucket_name}/{key}...")

                extra_args = {}

                client.upload_file(Filename=filename,
                                   Bucket=bucket_name, Key=key,
                                   ExtraArgs=extra_args)


            if remove_src:
                print(f"removing {filename}")
                os.unlink(filename)  # unlink and not ogr_remove!!!
            return filename

    except ClientError as ex:
        return str(ex)
    except NoCredentialsError as ex:
        return str(ex)

    return False


def list_s3_files(s3_uri, filename_prefix="", client=None, retrieve_properties=[]):
    """
    Elenca tutti i file in un bucket S3 dato il suo URI, filtrando per un prefisso specifico.

    :param s3_uri: URI S3 del bucket (es. "s3://mio-bucket")
    :param filename_prefix: Prefisso dei file da cercare (es. "dataset-name__variable-name")
    :return: Lista completa di filename presenti nel bucket con il prefisso specificato.
    """
    parsed_uri = urlparse(s3_uri)
    bucket_name = parsed_uri.netloc
    prefix = os.path.join(
        s3_uri[s3_uri.index(urlparse(s3_uri).netloc) + len(urlparse(s3_uri).netloc) + 1 : ],
        filename_prefix
    ).replace('\\', '/')

    client = get_client(client)
    paginator = client.get_paginator("list_objects_v2")

    file_list = []
    
    if len(retrieve_properties) > 0:
        avaliable_properties = [
            'Key',               # Full path of the object in the bucket.
            'LastModified',      # Date and time of the last modification (type datetime).
            'ETag',              # Hash MD5 of the object content (useful for integrity checks).
            'Size',              # Size of the file in bytes.
            'StorageClass',      # Storage class (e.g., STANDARD, GLACIER, etc.).
            'Owner',             # Owner of the object (if RequestPayer is set to requester).
        ]
        retrieve_properties = [prop for prop in retrieve_properties if prop in avaliable_properties]
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_info = {'Key': obj['Key']} | {prop: obj.get(prop) for prop in retrieve_properties}
                    file_list.append(file_info)
    else:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                file_list.extend([obj["Key"] for obj in page["Contents"]])
    
    return file_list


def generate_presigned_url(uri, expiration=3600, client=None):
    """
    Genera un URL prefirmato per un oggetto S3 privato.

    :param uri: URI dell'oggetto S3 da cui generare il pre-signed URL
    :return: URL prefirmato
    """
    bucket_name, key = get_bucket_name_key(uri)
    client = get_client(client)
    try:
        url = client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Errore nella generazione del pre-signed URL: {e}")
        return None
    
    
def copy_s3_object(source_uri, destination_uri, client=None):
    """
    Copia un oggetto S3 da una posizione a un'altra.

    :param source_uri: URI dell'oggetto S3 sorgente
    :param destination_uri: URI dell'oggetto S3 di destinazione
    :return: True se la copia ha avuto successo, False altrimenti
    """
    source_bucket_name, source_key = get_bucket_name_key(source_uri)
    destination_bucket_name, destination_key = get_bucket_name_key(destination_uri)
    
    client = get_client(client)
    
    try:
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': source_key
        }
        client.copy_object(
            CopySource = copy_source,
            Bucket = destination_bucket_name,
            Key = destination_key
        )
        return True
    except Exception as e:
        print(f"Errore nella copia dell'oggetto S3: {e}")
        return False
    
    
def delete_s3_object(uri, client=None):
    """
    Elimina un oggetto S3.

    :param uri: URI dell'oggetto S3 da eliminare
    :return: True se l'eliminazione ha avuto successo, False altrimenti
    """
    bucket_name, key = get_bucket_name_key(uri)
    
    client = get_client(client)
    
    try:
        client.delete_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        LOGGER.info(f"Errore nell'eliminazione dell'oggetto S3: {e}")
        return False
    
    
def move_s3_object(source_uri, destination_uri, client=None):
    """
    Sposta un oggetto S3 da una posizione a un'altra.

    :param source_uri: URI dell'oggetto S3 sorgente
    :param destination_uri: URI dell'oggetto S3 di destinazione
    :return: True se lo spostamento ha avuto successo, False altrimenti
    """
    if copy_s3_object(source_uri, destination_uri, client):
        return delete_s3_object(source_uri, client)
    return False