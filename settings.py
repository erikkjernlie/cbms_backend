import base64
import logging

from cryptography.fernet import Fernet

HOST = '0.0.0.0'
PORT = 1337

UDP_ADDR = ('0.0.0.0', 7331)

KAFKA_SERVER = 'localhost:9092'

# DATA_SOURCE_NAME = 'Driver=SQLite;Database=sqlite.db'

FMU_DIR = 'files/fmus'
FMU_MODEL_DIR = 'files/fmu_models'
MODEL_DIR = 'files/models'
DATASOURCE_DIR = 'files/datasources'
BLUEPRINT_DIR = 'files/blueprints'
PROCESSOR_DIR = 'files/processors'
FMM_FILES_DIR = 'files/fmm_files'
FMU_FILES_DIR = 'files/fmu_files'
PROJECT_DIR = 'files/projects'


SECRET_KEY = 'RJueaGk_wxvgOonaSHJebXi-uJcxqQP07bCkl9WgApQ='  # base64.urlsafe_b64decode(Fernet.generate_key())

PASSWORD = ''

LOG_FILE = ''  # Will print log to stderr if no file is specified  TODO: will most likely be problems with writing to files from processes
LOG_LEVEL = logging.INFO
