#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Thu Apr 11 13:45:38 2019

Module to handle data models mappings to C3S Climate Data Store 
Common Data Model (CMD) tables within the cdm tool.

@author: iregon
"""
# we remove python2 portability regarding OrderedDictionaries:
#from collections import OrderedDict # This is because python2 dictionaries do not keep key insertion order: this should only matter creating final tables
#maps[key] = json.load(json_file), object_pairs_hook=OrderedDict)

import os
import glob
import json
import datetime
from copy import deepcopy
import cdm.common.logging_hdlr as logging_hdlr

tool_name = 'cdm'
module_path = os.path.dirname(os.path.abspath(__file__))
module_tree = module_path[module_path.find('/' + tool_name + '/')+1:].split('/')

def dict_depth():
    return max(dict_depth(v) if isinstance(v,dict) else 0 for v in d.values()) + 1

class smart_dict(dict):
    # Gets items from nested dictionaries:
    # For simple dictionaries:
    #   smart_dict(dict)[key]
    # For nested dictionaries up to n levels
    #   Get first level: can declare key as single element or in list:
    #     smart_dict(dict)[[key]], smart_dict(dict)[key]
    #   Sucessive levels: keys in list from outer to inner, up to desired level:
    #     smart_dict(dict)[[key1, key2,key3]]
    #     smart_dict(dict)[[key1, key2,key3,..keyn]]
    # Returns None if key or combination not found
    def __init__(self,*args,**kwargs):
        dict.__init__(self,*args,**kwargs)
        self.__dict__ = self
        self.__depth__ = dict_depth(self.__dict__)
        self.__getstr__ = ['dict.get(self,key[0],None)']
        if self.__depth__ > 1:
            for d in range(2,self.__depth__ + 1):
                self.__getstr__.append(self.__getstr__[d-2].replace('None','{}') + '.get(key[' + str(d-1) + '],None)')
    def __getitem__(self, key):
        key = key if isinstance(key,list) else [key]
        val = eval(self.__getstr__[len(key) - 1])
        return val

def expand_integer_range_key(d):
    # Looping based on print_nested above
    if isinstance(d, dict):
        for k,v in list(d.items()):
            if 'range_key' in k[0:9]:
                range_params = k[10:-1].split(",")
                try:
                    lower = int(range_params[0])
                except Exception as e:
                    print("Lower bound parsing error in range key: ",k)
                    print("Error is:")
                    print(e)
                    return
                try:
                    upper = int(range_params[1])
                except Exception as e:
                    if range_params[1] == 'yyyy':
                        upper = datetime.date.today().year
                    else:
                        print("Upper bound parsing error in range key: ",k)
                        print("Error is:")
                        print(e)
                        return
                if len(range_params) > 2:
                    try:
                        step = int(range_params[2])
                    except Exception as e:
                        print("Range step parsing error in range key: ",k)
                        print("Error is:")
                        print(e)
                        return
                else:
                    step = 1
                for i_range in range(lower,upper + 1,step):
                    deep_copy_value = deepcopy(d[k]) # Otherwiserepetitions are linked and act as one!
                    d.update({str(i_range):deep_copy_value})
                d.pop(k, None)
            else:
                for k, v in d.items():
                    expand_integer_range_key(v)

def get_functions_module_path(imodel, log_level = 'INFO'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)
    imodel_module = os.path.join(module_path,imodel,imodel + ".py")
    if not os.path.isfile(imodel_module):
        logger.error('No mapping functions module for model {}'.format(imodel))
        return
    else:
        imodel_module_tree = module_tree.copy()
        imodel_module_tree.extend([imodel,imodel])
        return '.'.join(imodel_module_tree) #'cdm.mapper.lib.' + '.'.join([imodel,imodel])

def load_code_tables_maps(imodel, codes_subset = None, log_level = 'INFO'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)
    imodel_lib = os.path.join(module_path,imodel)
    if not os.path.isdir(imodel_lib):
        logger.error('No model mapping library for model {}'.format(imodel))
        return
    logger = logging_hdlr.init_logger(__name__)
    imodel_codes_lib = os.path.join(imodel_lib,'code_tables')
    if not os.path.isdir(imodel_codes_lib):
        logger.error('imodel code tables library path not found: {}'.format(imodel_codes_lib))
        return

    codes_paths = glob.glob(os.path.join(imodel_codes_lib,'*.json'))
    codes_paths = { os.path.basename(x).split(".")[0]:x for x in list(codes_paths) }
    if codes_subset:
        not_in_cdm = [ x for x in codes_subset if x not in codes_paths.keys() ]
        if any(not_in_cdm):
            logger.error('A wrong code table was requested for in model {0}: {1}'.format(imodel_codes_lib,",".join(not_in_cdm)))
            logger.info('code tables registered for model are: {}'.format(",".join(list(codes_paths.keys()))))
            return
        remove_codes = [ x for x in codes_paths.keys() if x not in codes_subset ]
        for x in remove_codes:
            shuss = codes_paths.pop(x, None)

    codes = dict()
    for key in codes_paths.keys():
        with open(codes_paths.get(key)) as fileObj:
            codes[key] = json.load(fileObj)
        expand_integer_range_key(codes[key])
    return codes


def load_tables_maps(imodel, cdm_subset = None, log_level = 'INFO'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)
    imodel_lib = os.path.join(module_path,imodel)
    if not os.path.isdir(imodel_lib):
        logger.error('No model mapping library for model {}'.format(imodel))
        return
    map_paths = glob.glob(os.path.join(imodel_lib,'*.json'))
    map_paths = { os.path.basename(x).split(".")[0]:x for x in list(map_paths) }
    if cdm_subset:
        not_in_cdm = [ x for x in cdm_subset if x not in map_paths.keys() ]
        if any(not_in_cdm):
            logger.error('A wrong cdm table was requested for in model {0}: {1}'.format(imodel_lib,",".join(not_in_cdm)))
            logger.info('cdm tables registered for model are: {}'.format(",".join(list(map_paths.keys()))))
            return
        remove_tables = [ x for x in map_paths.keys() if x not in cdm_subset ]
        for x in remove_tables:
            shuss = map_paths.pop(x, None)
    maps = dict()
    try:
        for key in map_paths.keys():
            with open(map_paths.get(key)) as json_file:
                maps[key] = json.load(json_file)
            for k,v in maps[key].items():
                elements = v.get('elements')
                if elements and not isinstance(elements,list):
                    v['elements'] = [elements]
                section = v.get('sections')
                if section:
                    if not isinstance(section,list):
                        section = [section]*len(v.get('elements'))
                    v['elements'] = [(s,e) for s,e in zip(section,v['elements'])]
                    shuss = v.pop('sections', None)
    except Exception as e:
        logger.error('Could not load mapping file {0}: {1}'.format(map_paths.get(key),e))
        return
    return maps
