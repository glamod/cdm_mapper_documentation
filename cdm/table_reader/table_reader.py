#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 13:45:38 2019

Reads files with the CDM table format from a file system to a pandas.Dataframe.

All CDM fields are read as objects. Null values are read with the specified null value
in the table files, or as NaN if the na_values argument is set to the a specific null
value in the file.

Reads the full set of files (default), a subset or a single table, as controlled
by cdm_subset:

    - When reading multiple tables, the resulting dataframe is multi-indexed in
        the columns, with (table-name, field) as column names. Merging of tables
        occurs on the report_id field.
    - When reading a single table, the resulting dataframe has simple indexing
        in the columns.

Reads the full set of fields (default) or a subset of it, as controlled by
param col_subset:
    - When reading multiple tables (default or subset), the col_subset is a 
        dictionary like: col_subset = {table0:[columns],...tablen:[columns]}
        If a table is not specified in col_subset, all its fields are read.
    - When reading a single table, the col_subset is a list like: 
        col_subset = [columns]
    - It is assumed that the column names are all conform to the cdm field names
        in lib.tables/*.json

The full table set (header, observations-*) is assumed to be in the same directory.
 
Filenames for tables are assumed to be:
    tableName-<tb_id>.<extension>
with:
    valid tableName: as declared in properties.cdm_tables
    tb_id: any identifier including wildcards if required
    extension: defaulting to 'psv'

When specifying a subset of tables, valid names are those in properties.cdm_tables

@author: iregon
"""

import os
import pandas as pd
from cdm import properties
from cdm.common import logging_hdlr
import glob

module_path = os.path.dirname(os.path.abspath(__file__))


def read_tables(tb_path, tb_id, cdm_subset=None, delimiter='|',
                extension='psv', col_subset=None, log_level='INFO', na_values=[]):
    """
    Reads CDM table like files from file system to a pandas data frame.

    Parameters
    ----------
    tb_path:
        path to the file
    tb_id:
        any identifier including wildcards if required extension, defaulting to 'psv'
    cdm_subset: specifies a subset of tables or a single table.
            - For multiple subsets of tables: This option will return a pandas.Dataframe that is multi-index at
            the columns, with (table-name, field) as column names. Tables are merged via the report_id field.
            - For a single table: the function returns a pandas.Dataframe with a simple indexing for the columns.

    delimiter:
        default is '|'
    extension:
        default is psv
    col_subset: a python dictionary specifying the section or sections of the file to read
            - For multiple sections of the tables:
                e.g ``col_subset = {table0:[columns],...tablen:[columns]}``
            - For a single section:
                e.g. ``list type object col_subset = [columns]``
                This variable assumes that the column names are all conform to the cdm field names in lib.tables/*.json
    log_level: Level of logging messages to save
    na_values: specifies the format of NaN values

    Returns
    -------
    pandas.Dataframe: either the entire file or a subset of it.
    logger.error: logs specific messages if there is any error.
    """
    logger = logging_hdlr.init_logger(__name__, level=log_level)
    # Because how the printers are written, they modify the original data frame!,
    # also removing rows with empty observation_value in observation_tables
    if not os.path.isdir(tb_path):
        logger.error('Data path not found {}: '.format(tb_path))
        return

    # See if theres anything at all:
    files = glob.glob(os.path.join(tb_path, '*' + tb_id + '*.' + extension))
    if len(files) == 0:
        logger.error('No files found matching pattern {}'.format(tb_id))
        return

        # See if subset, if any of the tables is not as specs
    if cdm_subset:
        for tb in cdm_subset:
            if tb not in properties.cdm_tables:
                logger.error('Requested table {} not defined in CDM'.format(tb))
                return

    tables = properties.cdm_tables if not cdm_subset else cdm_subset
    file_patterns = {tb: os.path.join(tb_path, '-'.join([tb, tb_id]) + '.' + extension) for tb in tables}
    file_paths = {}
    for k, v in file_patterns.items():
        logger.info('Getting file path for pattern {}'.format(v))
        file_path = glob.glob(v)
        if len(file_path) == 1:
            file_paths[k] = file_path[0]
        elif len(file_path) > 1:
            logger.error(
                'Pattern {0} resulted in multiple files for table {1}. '
                'Cannot seccurely retrieve cdm table(s)'.format(tb_id, k))
            return

    if len(file_paths) == 0:
        logger.error(
            'No cdm table files found for search patterns {0}: '.format(','.join(list(file_patterns.values()))))
        return

    usecols = None if len(tables) == 1 else {table: None for table in tables}
    if col_subset:
        if len(tables) == 1:
            if not isinstance(col_subset, list):
                logger.error('Column subset (col_subset) has to be declared as a list')
                return
            else:
                usecols = col_subset
        else:
            if not isinstance(col_subset, dict):
                logger.error(
                    'Column subset (col_subset) has to be declared as a dictionary '
                    'with a table:[columns] pair per table to subset')
                return
            else:
                usecols = {table: col_subset.get(table, None) for table in tables}

    logger.info('Reading into dataframe data files {}: '.format(','.join(list(file_paths.values()))))
    if len(tables) == 1:
        file_path = list(file_paths.values())[0]
        return pd.read_csv(file_path, delimiter=delimiter, usecols=usecols,
                           dtype='object', na_values=na_values, keep_default_na=False)
    else:
        df_list = []
        for tb, tb_file in file_paths.items():
            dfi = pd.read_csv(tb_file, delimiter=delimiter,
                              usecols=usecols.get(tb), dtype='object',
                              na_values=na_values, keep_default_na=False)
            if len(dfi) > 0:
                dfi.set_index('report_id', inplace=True, drop=False)
                dfi.columns = pd.MultiIndex.from_product([[tb], dfi.columns])
                df_list.append(dfi)
            else:
                logger.warning('Table {} empty in file system, not added to the final DF'.format(tb))

        if len(df_list) > 0:
            merged = pd.concat(df_list, axis=1, join='outer')
            merged.reset_index(drop=True, inplace=True)
            return merged
        else:
            logger.error('All tables empty in file system')
            return
