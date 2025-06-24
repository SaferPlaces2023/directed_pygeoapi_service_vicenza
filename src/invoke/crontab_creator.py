import os
import pandas as pd
from crontab import CronTab

from saferplacesapi import _s3_utils


INVOKE_INGESTOR_PATH = os.path.dirname(os.path.abspath(__file__))

CRONEJOBS = pd.DataFrame([
    {
        'description': 'HERA',
        'schedule': '*/5 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'hera-radar-rainfall-process.json')
        ],
    },
    {
        'description': 'DPC',
        'schedule': '*/5 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'dpc-radar-rainfall-process.json')
        ],
    },
    {
        'description': 'ARPAE - B13011 (total_precipitation)',
        'schedule': '*/15 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B13011.json')
        ],
    },
    {
        'description': 'ARPAE - B13215 (river_level)',
        'schedule': '*/15 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B13215.json')
        ],
    },
    # {
    #     'description': 'ARPAE - B13226 (river_discharge)',    # DOC: This is useless, no river-discharge data is available inside selected bbox (there are 6-7 stations all along Po river)
    #     'schedule': '*/15 * * * *',
    #     'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
    #     'args': [ 
    #         os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B13226.json')
    #     ],
    # },
    {
        'description': 'ARPAE - B22037 (tidal_national_ref)',
        'schedule': '*/10 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B22037.json')
        ],
    },
    {
        'description': 'ARPAE - B22070 (wave_height_significative)',
        'schedule': '*/30 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B22070.json')
        ],
    },
    {
        'description': 'ARPAE - B22001 (wave_direction)',
        'schedule': '*/30 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'arpae-realtime-process-B22001.json')
        ],
    },
    {
        'description': 'ICON-INGESTOR',
        'schedule': '0 */12 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'icon2i-precipitation-ingestor-process.json')
        ],
    },
    {
        'description': 'ICON-RETRIEVER',
        'schedule': '0 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'icon2i-precipitation-retriever-process.json')
        ],
    },
    {
        'description': 'SWANEMR-INGESTOR',
        'schedule': '0 1 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'swanemr-waveheight-ingestor-process.json')
        ],
    },
    {
        'description': 'SWANEMR-RETRIEVER',
        'schedule': '0 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'swanemr-waveheight-retriever-process.json')
        ],
    },
    {
        'description': 'ADRIAC-INGESTOR (hour to 01)',
        'schedule': '0 1 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'adriac-sealevel-ingestor-process.json')
        ],
    },
    {
        'description': 'ADRIAC-RETRIEVER',
        'schedule': '*/10 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'adriac-sealevel-retriever-process.json')
        ],
    },
    {
        'description': 'METEOBLUE - basic-5min',
        'schedule': '*/5 * * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'meteoblue-precipitation-retriever-process.json')
        ],
    },
    # {
    #     'description': 'NOWRADAR',
    #     'schedule': '*/5 * * * *',
    #     'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
    #     'args': [ 
    #         os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'nowradar-precipitation-process.json')
    #     ],
    # },
    
    {
        'description': 'BUCKET-CLEANER',
        'schedule': '0 0 * * *',
        'script': os.path.join(INVOKE_INGESTOR_PATH, 'invoke_ingestor.py'),
        'args': [ 
            os.path.join(INVOKE_INGESTOR_PATH, 'invocation_data', 'bucket-cleaner-service.json')
        ],
    }
])


def build_command(cronjob):
    """
    Build the command to run the Python script with arguments.
    """
    
    command = f'python3 {cronjob.script} {" ".join(cronjob.args)}'
    return command


def set_cronjob(cron, cronjob):
    """
    Write the cron job to the crontab.
    """
    
    command = build_command(cronjob)
    
    job = cron.new(command=command, comment=cronjob.description)
    job.setall(cronjob.schedule)
    for env_key,env_val in os.environ.items():
        job.env[env_key] = env_val
    
    print(f'Wrote cron job: {cronjob.description} - {cronjob.schedule}')


if __name__ == "__main__":
    
    try:
    
        cron = CronTab(user='root')
        
        for index, cronejob in CRONEJOBS.iterrows():
            set_cronjob(cron, cronejob)
            
        cron.write()
        print('Crontab written successfully.')
    
    except Exception as e:
        print(f'Error writing crontab: {e}')

