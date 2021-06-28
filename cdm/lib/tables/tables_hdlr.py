#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Thu Apr 11 13:45:38 2019

Module to handle C3S Climate Data Store Common Data Model (CMD) tables within
the cdm tool.

@author: iregon
"""

# we remove python2 portability regarding OrderedDictionaries:
#from collections import OrderedDict # This is because python2 dictionaries do not keep key insertion order: this should only matter creating final tables
#tables[key] = json.load(json_file, object_pairs_hook=OrderedDict)

import os
import glob
import json
import requests
import csv
from cdm.common import logging_hdlr
from cdm import properties


module_path = os.path.dirname(os.path.abspath(__file__))
table_path = module_path

def load_tables(log_level = 'DEBUG'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)
    table_paths = glob.glob(os.path.join(table_path,'*.json'))
    table_paths = { os.path.basename(x).split(".")[0]:x for x in list(table_paths) }

    observation_tables = [ x for x in properties.cdm_tables if x.startswith('observations-')]
    # Make a copy from the generic observations table for each to the observations
    # table defined in properties
    observation_path = table_paths.get('observations')
    shuss = table_paths.pop('observations',None)
    table_paths.update({ observation_table:observation_path for observation_table in observation_tables})

    tables = dict()
    try:
        for key in table_paths.keys():
            with open(table_paths.get(key)) as json_file:
                tables[key] = json.load(json_file)
    except Exception as e:
        logger.error('Could not load table {}'.format(key), exc_info=True)
        return
    return tables


 ### cdm elements dtypes
# Mail sent may 7th to Dave. Are the types there real SQL types, or just approximations?
# Numeric type in table definition not useful here to define floats with a specific precision
# We should be able to use those definitions. Keep in mind that arrays are object type in pandas!
# Remember any int and float (int, numeric) need to be tied for the parser!!!!
# Also datetimes!
# Until CDM table definition gets clarified:
# We map from cdm table definition types to those in properties.pandas_dtypes.get('from_sql'), else: 'object'
# We update to df column dtype if is of float type


def from_glamod(table_filename, gitlinkroot = None, element_col = 1, type_col = 2, field_separator = '\t',skip_lines = 3):
    # Get tables from GLAMOD Git repo and format to nested dictionary with:
    # { cdm_name: {'data_type':value}}
    #
    # table_filename: table filename in repo directory
    # gitlinkroot: url to directory where tables are stored
    # element_col: column with element names (first is 1)
    # type_col: column with element data typs (first is 1)
    #
    # About data type definitions in this source (table_definitions in GitHub):
    # it is not controlled vocab. and might change in the future!!!!


    # Get data types and clean primary key, optional and whitespaces: '(pk)', '*'
    logger = logging_hdlr.init_logger(__name__,level = 'INFO')
    if not gitlinkroot:
        gitlinkroot = 'https://github.com/glamod/common_data_model/blob/master/table_definitions/'
        logger.info('Setting gitlink root to default: {}'.format(gitlinkroot))

    gitlinkroot = gitlinkroot.replace('blob/','')
    gitlinkroot = gitlinkroot.replace('https://','https://raw.')
    response = requests.get(os.path.join(gitlinkroot,table_filename))
    field_separator = '\t'
    lines = list(csv.reader(response.content.decode('utf-8').splitlines(), delimiter = field_separator))
    for i in range(0,skip_lines):
        lines.pop(0)
    return { x[element_col-1]:{'data_type':x[type_col-1].strip('(pk)').strip('*').strip()} for x in lines}
