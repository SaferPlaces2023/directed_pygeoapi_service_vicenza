import os
import base64
import datetime


def forceext(pathname, newext):
    """
    forceext
    """
    root, _ = os.path.splitext(normpath(pathname))
    pathname = root + ("." + newext if len(newext.strip()) > 0 else "")
    return normpath(pathname)


def justfname(pathname):
    """
    justfname - returns the basename
    """
    return normpath(os.path.basename(normpath(pathname)))



def justpath(pathname, n=1):
    """
    justpath
    """
    for _ in range(n):
        pathname, _ = os.path.split(normpath(pathname))
    if pathname == "":
        return "."
    return normpath(pathname)

    

def juststem(pathname):
    """
    juststem
    """
    pathname = os.path.basename(pathname)
    root, _ = os.path.splitext(pathname)
    return root



def justext(pathname):
    """
    justext
    """
    pathname = os.path.basename(normpath(pathname))
    _, ext = os.path.splitext(pathname)
    return ext.lstrip(".")

    

def normpath(pathname):
    """
    normpath
    """
    if not pathname:
        return ""
    pathname = os.path.normpath(pathname.replace("\\", "/")).replace("\\", "/")
    # patch for s3:// and http:// https://
    pathname = pathname.replace(":/", "://")
    return pathname


datetime_to_millis = lambda date_time: int(date_time.timestamp() * 1000)

millis_to_datetime = lambda millis: datetime.datetime.fromtimestamp(millis / 1000.0)



def string_to_int_id(s):
    return int.from_bytes(s.encode(), 'big')

def int_id_to_string(i):
    return i.to_bytes((i.bit_length() + 7) // 8, 'big').decode()