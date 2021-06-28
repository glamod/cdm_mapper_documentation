#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 13:45:38 2019

Exports C3S Climate Data Store Common Data Model (CDM) tables 
contained in a dictionary of pandas DataFrames (or pd.io.parsers.TextFileReader)
to ascii files.

This module uses a set of printer functions to "print" element values to a
string object before export to the final ascii files. Each of the CDM table element's 
has a data type (pseudo-sql as defined in the CDM documentation) which defines 
the printer function used. 

Numeric data types are printed with a number of decimal places as defined in
the data element attributes input to this module (this is defined on a
CDMelement-imodelMapping specific basis in the data model mapping to the CDM) 
If it is not defined in the input attributes, the number of decimal places used
is a tool default defined in properties.py

@author: iregon
"""

import os
import pandas as pd
import numpy as np
from io import StringIO
from cdm import properties
from cdm.common import pandas_TextParser_hdlr
from cdm.common import logging_hdlr


module_path = os.path.dirname(os.path.abspath(__file__))

def print_integer(data,null_label):
    data.iloc[np.where(data.notna())] = data.iloc[np.where(data.notna())].astype(int).astype(str)
    data.iloc[np.where(data.isna())] = null_label
    return data

def print_float(data,null_label, decimal_places = None):
    decimal_places = properties.default_decimal_places if decimal_places is None else decimal_places
    format_float='{:.' + str(decimal_places) + 'f}'
    data.iloc[np.where(data.notna())] = data.iloc[np.where(data.notna())].apply(format_float.format)
    data.iloc[np.where(data.isna())] = null_label
    return data

def print_datetime(data,null_label):
    data.iloc[np.where(data.notna())] = data.iloc[np.where(data.notna())].dt.strftime("%Y-%m-%d %H:%M:%S")
    data.iloc[np.where(data.isna())] = null_label
    return data

def print_varchar(data,null_label):
    data.iloc[np.where(data.notna())] = data.iloc[np.where(data.notna())].astype(str)
    data.iloc[np.where(data.isna())] = null_label
    return data

def print_integer_array(data,null_label):
    return data.apply(print_integer_array_i,null_label=null_label)

def print_float_array(data,null_label, decimal_places = None):
     return 'float array not defined in printers'

def print_datetime_array(data,null_label):
    return 'datetime tz array not defined in printers'

def print_varchar_array(data,null_label):
    return data.apply(print_varchar_array_i,null_label=null_label)

printers = {'int': print_integer, 'numeric': print_float, 'varchar':print_varchar,
            'timestamp with timezone': print_datetime,
            'int[]': print_integer_array, 'numeric[]': print_float_array,
            'varchar[]':print_varchar_array,
            'timestamp with timezone[]': print_datetime_array}

iprinters_kwargs = {'numeric':['decimal_places'],
                   'numeric[]':['decimal_places']}


def print_integer_array_i(row,null_label = None):
    if row==row:
        row = eval(row)
        row = row if isinstance(row,list) else [row]
        string = ','.join(filter(bool,[ str(int(x)) for x in row if np.isfinite(x)]))
        if len(string) > 0:
            return '{' + string + '}'
        else:
            return null_label
    else:
        return null_label

def print_varchar_array_i(row,null_label = None):
    if row==row:
        row = eval(row)
        row = row if isinstance(row,list) else [row]
        string = ','.join(filter(bool,row))
        if len(string) > 0:
            return '{' + string + '}'
        else:
            return null_label
    else:
        return null_label

def table_to_ascii(table, table_atts, delimiter = '|', null_label = 'null', cdm_complete = True, filename = None, full_table = True, log_level = 'INFO'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)

    empty_table = False
    if 'observation_value' in table:
        table.dropna(subset=['observation_value'],inplace=True)
        empty_table = True if len(table) == 0 else False
    elif 'observation_value' in table_atts.keys():
        empty_table = True
    else:
        empty_table = True if len(table) == 0 else False  
    if empty_table:
        logger.warning('No observation values in table')
        ascii_table = pd.DataFrame(columns = table_atts.keys(), dtype = 'object')
        ascii_table.to_csv(filename, index = False, sep = delimiter, header = True, mode = 'w')
        return
    
    ascii_table = pd.DataFrame(index = table.index, columns = table_atts.keys(), dtype = 'object')
    for iele in table_atts.keys():
        if iele in table:
            itype = table_atts.get(iele).get('data_type')
            if printers.get(itype):
                iprinter_kwargs = iprinters_kwargs.get(itype)
                if iprinter_kwargs:
                    kwargs = { x:table_atts.get(iele).get(x) for x in iprinter_kwargs}
                else:
                    kwargs = {}
                ascii_table[iele] = printers.get(itype)(table[iele], null_label, **kwargs)
            else:
                logger.error('No printer defined for element {}'.format(iele))
        else:
            ascii_table[iele] = null_label
            
    header = True 
    wmode = 'w'
    columns_to_ascii = [ x for x in table_atts.keys() if x in table.columns ] if not cdm_complete else table_atts.keys()
    ascii_table.to_csv(filename, index = False, sep = delimiter, columns = columns_to_ascii, header = header, mode = wmode)

#    # Convert to iterable if plain dataframe
#    # This is no longer needed as the mapper now only produces real dataframes,
#    # never TextParser...
#    if isinstance(table,pd.DataFrame):
#        table = [table]
#    ichunk = 0
#    for itable in table:
#        # drop records with no 'observation_value'
#        empty_table = False
#        if 'observation_value' in itable:
#            itable.dropna(subset=['observation_value'],inplace=True)
#            empty_table = True if len(itable) == 0 else False
#        elif 'observation_value' in table_atts.keys():
#            empty_table = True  
#        if empty_table:
#            logger.warning('No observation values in table')
#            ascii_table = pd.DataFrame(columns = table_atts.keys(), dtype = 'object')
#            ascii_table.to_csv(filename, index = False, sep = delimiter, header = True, mode = 'w')
#            break    
#        ascii_table = pd.DataFrame(index = itable.index, columns = table_atts.keys(), dtype = 'object')
#        for iele in table_atts.keys():
#            if iele in itable:
#                itype = table_atts.get(iele).get('data_type')
#                if printers.get(itype):
#                    iprinter_kwargs = iprinters_kwargs.get(itype)
#                    if iprinter_kwargs:
#                        kwargs = { x:table_atts.get(iele).get(x) for x in iprinter_kwargs}
#                    else:
#                        kwargs = {}
#                    ascii_table[iele] = printers.get(itype)(itable[iele], null_label, **kwargs)
#                else:
#                    logger.error('No printer defined for element {}'.format(iele))
#            else:
#                ascii_table[iele] = null_label
#                
#        header = False if ichunk > 0 else True 
#        wmode = 'a' if ichunk > 0 else 'w'
#        columns_to_ascii = [ x for x in table_atts.keys() if x in itable.columns ] if not cdm_complete else table_atts.keys()
#        ascii_table.to_csv(filename, index = False, sep = delimiter, columns = columns_to_ascii, header = header, mode = wmode)
#        ichunk += 1

    return

def cdm_to_ascii( cdm, delimiter = '|', null_label = 'null', cdm_complete = True, extension = 'psv',out_dir = None, suffix = None, prefix = None, log_level = 'INFO'):
    logger = logging_hdlr.init_logger(__name__,level = log_level)
    # Because how the printers are written, they modify the original data frame!,
    # also removing rows with empty observation_value in observation_tables
    extension = '.' + extension
    for table in cdm.keys():
        logger.info('Printing table {}'.format(table))
        filename = '-'.join(filter(bool,[prefix,table,suffix])) + extension
        filepath = filename if not out_dir else os.path.join(out_dir, filename )
        table_to_ascii(cdm[table]['data'], cdm[table]['atts'], delimiter = delimiter, null_label= null_label, cdm_complete = cdm_complete, filename = filepath, log_level = log_level)
    return
