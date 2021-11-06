"""
Launcher.py

Created On: 8/1/2021
Updated On: 1/3/2021
Created by : e44972

This module is the starting point of the different source systems to DCM data extraction.
"""

# ===============================================================================
# import statements
# ===============================================================================

import traceback
import argparse
import configparser
import logging.config
import time
import os


from os.path import join
from pickle import TRUE
from top_basis import extractbasis
from top_premium import extractpremium
from as400_opas_loader import loader
from aliptodcm_interim_loader import alip_to_dcm_loader
from merge import load_merge
from precalc import precalc_loader
import commons.resources


ARG_PARCER = argparse.ArgumentParser(description='Loading')
ARG_PARCER.add_argument('--conf-dir', required=True, dest='conf_dir',
                        metavar='', help='Path to config folder')
ARG_PARCER.add_argument('--conf-file', required=False, dest='conf_file',
                        metavar='', help='Path to config folder')
ARG_PARCER.add_argument('--source-sys', dest='source_sys')
ARG_PARCER.add_argument('--cycle-date', required=False, dest='cycle_date',
                        metavar='', help='Cycle date')
ARG_PARCER.add_argument('--environment', required=False, dest='environment', metavar='',
                        help='Server environment: values are dev, qa, stage, prod')

ARGS = ARG_PARCER.parse_args()

DO_DCM_LOADER = ARGS.source_sys
CYCLE_DATE = ARGS.cycle_date
CONF_DIR = ARGS.conf_dir
CONF_FILE = ARGS.conf_file
CURR_ENV = ARGS.environment


assert CONF_DIR, '--conf-dir can not be left blank'
assert CONF_FILE, '--conf-file can not be left blank'
assert CYCLE_DATE, '--cycle-date can not be left blank'

RUN_ID = str(int(time.time()))
CONF_FILE_NAME = join(CONF_DIR, CONF_FILE)
CONFIG = configparser.ConfigParser()
CONFIG.optionxform = str
CONFIG.read(CONF_FILE_NAME)

RESOURCES_CONFIG_FILE = CONFIG['Settings'][CURR_ENV]
RESOURCE_MANAGER = commons.resources.ResourceManager(RESOURCES_CONFIG_FILE)

LOG_DIR = CONFIG['DIRECTORIES']['log_dir']


def init_logging():
    """Initializing logging"""
    log_config = configparser.ConfigParser()
    log_config.optionxform = str
    log_config.read(join(CONF_DIR, 'main.log.conf'))
    log_config['DEFAULT']['log_dir'] = LOG_DIR
    logging.config.fileConfig(log_config)


init_logging()
APP_LOGGER = logging.getLogger('AppLogger')
APP_LOGGER.info('Starting Loader')
APP_LOGGER.info('Cycle Date is %s, run id is %s', CYCLE_DATE, RUN_ID)

try:
    if DO_DCM_LOADER == "alip":
        APP_LOGGER.info('Starting ALIP To DCM data transfer')
        alip_to_dcm_loader(RESOURCE_MANAGER, CONFIG, CONF_FILE_NAME, APP_LOGGER, CYCLE_DATE)
        APP_LOGGER.info('ALIP To DCM data transfer completed')
    elif DO_DCM_LOADER == "opas":
        APP_LOGGER.info('Starting OPAS To DCM data transfer')
        loader(RESOURCE_MANAGER, CONFIG, DO_DCM_LOADER, APP_LOGGER)
        APP_LOGGER.info('OPAS To DCM data transfer completed')
    elif DO_DCM_LOADER == "as400":
        APP_LOGGER.info('Starting AS400 To DCM data transfer')
        loader(RESOURCE_MANAGER, CONFIG, DO_DCM_LOADER, APP_LOGGER)
        APP_LOGGER.info('AS400 To DCM data transfer completed')
    elif DO_DCM_LOADER == "merge":
        APP_LOGGER.info('Merge alip,as400,opas')
        load_merge(RESOURCE_MANAGER, CONFIG)
        APP_LOGGER.info('Merge completed')
    elif DO_DCM_LOADER == "pre_calc":
        APP_LOGGER.info('Run Pre calc program')
        precalc_loader(RESOURCE_MANAGER, CONFIG, DO_DCM_LOADER, APP_LOGGER)
        APP_LOGGER.info('pre calc loader completed')
    elif DO_DCM_LOADER == 'premium':
        APP_LOGGER.info('Starting Top Premium data extract process')
        extractpremium(RESOURCE_MANAGER, CONFIG, CONF_FILE_NAME, APP_LOGGER, CYCLE_DATE, CURR_ENV)
        APP_LOGGER.info(' Top Premium data extract is completed')
    elif  DO_DCM_LOADER == 'basis':
        APP_LOGGER.info('Starting Top Basis data extract process')
        extractbasis(RESOURCE_MANAGER, CONFIG, CONF_FILE_NAME, APP_LOGGER, CYCLE_DATE, CURR_ENV)
        APP_LOGGER.info('Top Basis extract is completed')
        

except Exception as exc_error:
    APP_LOGGER.fatal("Fatal Exception Detected", exc_info=1)
    print(exc_error)
    traceback.print_exc()
    exit(1)
