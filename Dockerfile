FROM geopython/pygeoapi:0.20.0

ENV PYGEOAPI_CONFIG=config.yml

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
        cron \
        nano \
        # curl \
        git \
        # gdal-bin \
        # python3-gdal \
    && rm -rf /var/lib/apt/lists/*

COPY config.yml local.config.yml
COPY . .


RUN pip install . \
    && rm -rf ./process

# entrypoint.sh and scheduler.sh EOL must be UNIX-style (LF). If not you can occur in the following error: exec /entrypoint.sh: no such file or directory

RUN chmod +x /pygeoapi/entrypoint.sh 

ENTRYPOINT ["/pygeoapi/entrypoint.sh"]
