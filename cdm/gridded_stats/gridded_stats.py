#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 21 16:06:16 2019

Important note:
    This script is not well documented and it seems to have incomplete work.

Sets data descriptors on a monthly lat-lon box grid: counts, max, min, mean. And saves data to a netcdf file

@author: iregon

DEVS notes:
    These functions using dask need to work with env1 in C3S r092019, since there are some issues with pyarrow and
    msgpack that had to be downgraded from 0.6.0 (default) to 0.5.6 after.

    File "msgpack/_unpacker.pyx", line 187, in msgpack._cmsgpack.unpackb
    ValueError: 1281167 exceeds max_array_len(131072)
    See https://github.com/tensorpack/tensorpack/issues/1003
"""

import os
from dask import dataframe as dd
import dask.diagnostics as diag
import datashader as ds
import xarray as xr
import pandas as pd
from cdm import properties
import datetime
import logging
import glob
import time
import shutil

# SOME COMMON PARAMS ----------------------------------------------------------
# For canvas
REGIONS = dict()
REGIONS['Global'] = ((-180.00, 180.00), (-90.00, 90.00))
DEGREE_FACTOR_RESOLUTION = dict()
DEGREE_FACTOR_RESOLUTION['lo_res'] = 1
DEGREE_FACTOR_RESOLUTION['me_res'] = 2
DEGREE_FACTOR_RESOLUTION['hi_res'] = 5
# To define aggregationa
DS_AGGREGATIONS = {'counts': ds.count, 'max': ds.max, 'min': ds.min, 'mean': ds.mean}
AGGREGATIONS = ['counts', 'max', 'min', 'mean']
DS_AGGREGATIONS_HDR = {'counts': ds.count}
AGGREGATIONS_HDR = ['counts']
# TO output nc
ENCODINGS = {'latitude': {'dtype': 'int16', 'scale_factor': 0.01, '_FillValue': -99999},
             'longitude': {'dtype': 'int16', 'scale_factor': 0.01, '_FillValue': -99999},
             'counts': {'dtype': 'int64', '_FillValue': -99999},
             'max': {'dtype': 'int32', 'scale_factor': 0.01, '_FillValue': -99999},
             'min': {'dtype': 'int32', 'scale_factor': 0.01, '_FillValue': -99999},
             'mean': {'dtype': 'int32', 'scale_factor': 0.01, '_FillValue': -99999}}
ENCODINGS_HDR = {'latitude': {'dtype': 'int16', 'scale_factor': 0.01, '_FillValue': -99999},
                 'longitude': {'dtype': 'int16', 'scale_factor': 0.01, '_FillValue': -99999},
                 'counts': {'dtype': 'int64', '_FillValue': -99999}}
# To read tables
CDM_DTYPES = {'latitude': 'float32', 'longitude': 'float32',
              'observation_value': 'float32', 'date_time': 'object',
              'quality_flag': 'int8', 'crs': 'int', 'report_quality': 'int8',
              'report_id': 'object'}
READ_COLS = ['report_id', 'latitude', 'longitude', 'observation_value', 'date_time',
             'quality_flag']
DTYPES = {x: CDM_DTYPES.get(x) for x in READ_COLS}

READ_COLS_HDR = ['report_id', 'latitude', 'longitude', 'crs', 'report_timestamp', 'report_quality']
DTYPES_HDR = {x: CDM_DTYPES.get(x) for x in READ_COLS}

DELIMITER = '|'


# SOME FUNCTIONS THAT HELP ----------------------------------------------------
def bounds(x_range, y_range):
    """

    Parameters
    ----------
    x_range:
    y_range:

    Returns
    -------
    dict:
    """
    return dict(x_range=x_range, y_range=y_range)


def create_canvas(bbox, degree_factor):
    """

    Parameters
    ----------
    bbox:
    degree_factor:

    Returns
    -------
    plot:
    """
    plot_width = int(abs(bbox[0][0] - bbox[0][1]) * degree_factor)
    plot_height = int(abs(bbox[1][0] - bbox[1][1]) * degree_factor)
    return ds.Canvas(plot_width=plot_width, plot_height=plot_height, **bounds(*bbox))


# FUNCTIONS TO DO WHAT WE WANT ------------------------------------------------
def from_cdm_monthly(dir_data, cdm_id=None, region='Global',
                     resolution='lo_res', nc_dir=None, qc=None, qc_report=None):
    """

    Parameters
    ----------
    dir_data
    cdm_id
    region
    resolution
    nc_dir
    qc
    qc_report

    Returns
    -------
    logging.error:
    netcdf_file:
    """
    qc_extensions = '_'.join(['qc' + str(x) for x in qc]) if qc else None
    qc_report_extensions = '_'.join(['qcr' + str(x) for x in qc_report]) if qc_report else None
    # qc is the list of flags we want to keep, as integers, ideally, ok, include conversion just in case
    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                        level=logging.INFO, datefmt='%Y%m%d %H:%M:%S', filename=None)

    canvas = create_canvas(REGIONS.get(region), DEGREE_FACTOR_RESOLUTION.get(resolution))

    table = 'header'
    logging.info('Processing table {}'.format(table))
    table_file = os.path.join(dir_data, '-'.join([table, cdm_id]) + '.psv')
    if not os.path.isfile(table_file):
        logging.error('Table file not found {}'.format(table_file))
        return
    df_header = pd.read_csv(table_file, delimiter=DELIMITER, usecols=READ_COLS_HDR,
                            parse_dates=['report_timestamp'], dtype=DTYPES_HDR)
    df_header.set_index('report_id', inplace=True, drop=True)

    if qc_report:
        # locs_ok = df_header.loc[df_header['report_quality'].isin([ int(x) for x in qc_report ])].index
        df_header = df_header.loc[df_header['report_quality'].isin([int(x) for x in qc_report])]

    try:
        date_time = datetime.datetime(df_header['report_timestamp'][0].year, df_header['report_timestamp'][0].month, 1)
    except Exception:
        fields = cdm_id.split('-')
        date_time = datetime.datetime(int(fields[0]), int(fields[1]), 1)
    # Aggregate on to and aggregate to a dict
    xarr_dict = {x: '' for x in AGGREGATIONS_HDR}
    for agg in AGGREGATIONS_HDR:
        xarr_dict[agg] = canvas.points(df_header, 'longitude', 'latitude',
                                       DS_AGGREGATIONS_HDR.get(agg)('crs'))
    # Merge aggs in a single xarr
    xarr = xr.merge([v.rename(k) for k, v in xarr_dict.items()])
    xarr.expand_dims(**{'time': [date_time]})
    xarr.encoding = ENCODINGS_HDR
    # Save to nc

    nc_dir = dir_data if not nc_dir else nc_dir
    nc_name = '-'.join(filter(bool, [table, cdm_id, qc_report_extensions])) + '.nc'
    xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS_HDR, mode='w')

    obs_tables = [x for x in properties.cdm_tables if x != 'header']
    for table in obs_tables:
        logging.info('Processing table {}'.format(table))
        table_file = os.path.join(dir_data, '-'.join([table, cdm_id]) + '.psv')
        if not os.path.isfile(table_file):
            logging.warning('Table file not found {}'.format(table_file))
            continue
        # Read the data
        df = pd.read_csv(table_file, delimiter=DELIMITER, usecols=READ_COLS,
                         parse_dates=['date_time'], dtype=DTYPES)

        df.set_index('report_id', inplace=True, drop=True)

        if qc_report:
            # was giving werror ...df.loc[df_header['report_quality'].isin([ int(x) for x in qc_report ]))]
            df = df.loc[[x for x in df_header.index if x in df.index]]

        if qc:
            df = df.loc[df['quality_flag'].isin([int(x) for x in qc])]
        try:
            date_time = datetime.datetime(df['date_time'][0].year, df['date_time'][0].month, 1)
        except Exception:
            fields = cdm_id.split('-')
            date_time = datetime.datetime(int(fields[0]), int(fields[1]), 1)
        # Aggregate on to and aggregate to a dict
        xarr_dict = {x: '' for x in AGGREGATIONS}
        for agg in AGGREGATIONS:
            xarr_dict[agg] = canvas.points(df, 'longitude', 'latitude',
                                           DS_AGGREGATIONS.get(agg)('observation_value'))
        # Merge aggs in a single xarr
        xarr = xr.merge([v.rename(k) for k, v in xarr_dict.items()])
        xarr.expand_dims(**{'time': [date_time]})
        xarr.encoding = ENCODINGS
        # Save to nc
        nc_dir = dir_data if not nc_dir else nc_dir
        nc_name = '-'.join(filter(bool, [table, cdm_id, qc_report_extensions, qc_extensions])) + '.nc'
        try:
            xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS, mode='w')
        except Exception as e:
            logging.info('Error saving nc:')
            logging.info(e)
            logging.info('Retrying in 6 seconds...')
            time.sleep(6)
            xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS, mode='w')

    return


def merge_from_monthly_nc(dir_data, cdm_id=None, nc_dir=None, force_header=True):
    """

    Parameters
    ----------
    dir_data:
    cdm_id:
    nc_dir:
    force_header:

    Returns
    -------
    logging.error:
    netcdf_file:
    """
    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                        level=logging.INFO, datefmt='%Y%m%d %H:%M:%S', filename=None)
    table = 'header'
    logging.info('Processing table {}'.format(table))
    pattern = '-'.join([table, '*', cdm_id]) + '.nc'
    all_files = glob.glob(os.path.join(dir_data, pattern))
    process_header = True
    if len(all_files) == 0:
        if force_header:
            logging.error('No nc files found {}'.format(pattern))
            return
        else:
            logging.warning('No header nc files found {}'.format(pattern))
            process_header = False
    if process_header:
        all_files.sort()
        # Read all files to a single dataset
        dataset = xr.open_mfdataset(all_files, concat_dim='time')
        # Aggregate each aggregation correspondingly....
        merged = {}
        merged['counts'] = dataset['counts'].sum(dim='time', skipna=True)
        # Merge aggregations to a single xarr
        xarr = xr.merge([v.rename(k) for k, v in merged.items()])
        xarr.encoding = ENCODINGS_HDR
        # Save to nc
        nc_dir = dir_data if not nc_dir else nc_dir
        nc_name = '-'.join([table, cdm_id]) + '.nc'
        xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS_HDR)

    obs_tables = [x for x in properties.cdm_tables if x != 'header']
    for table in obs_tables:
        logging.info('Processing table {}'.format(table))
        pattern = '-'.join([table, '*', cdm_id]) + '.nc'
        all_files = glob.glob(os.path.join(dir_data, pattern))
        if len(all_files) == 0:
            logging.warning('No nc files found {}'.format(pattern))
            continue
        all_files.sort()
        # Read all files to a single dataset
        dataset = xr.open_mfdataset(all_files, concat_dim='time')
        # Aggregate each aggregation correspondingly....
        merged = {}
        merged['max'] = dataset['max'].max(dim='time', skipna=True)
        merged['min'] = dataset['min'].min(dim='time', skipna=True)
        merged['mean'] = dataset['mean'].mean(dim='time', skipna=True)
        merged['counts'] = dataset['counts'].sum(dim='time', skipna=True)
        # Merge aggregations to a single xarr
        xarr = xr.merge([v.rename(k) for k, v in merged.items()])
        xarr.encoding = ENCODINGS
        # Save to nc
        nc_dir = dir_data if not nc_dir else nc_dir
        nc_name = '-'.join([table, cdm_id]) + '.nc'
        xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS, mode='w')
    return


def global_from_monthly_cdm(dir_data, cdm_id=None, region='Global',
                            resolution='lo_res', nc_dir=None, qc=None, qc_report=None, scratch_dir=None,
                            tables=None):
    """

    Parameters
    ----------
    dir_data:
    cdm_id:
    region:
    resolution:
    nc_dir:
    qc:
    qc_report:
    scratch_dir:
    tables:

    Returns
    -------
    logging.error:
    netcdf_file:
    """
    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                        level=logging.INFO, datefmt='%Y%m%d %H:%M:%S', filename=None)

    qc_extensions = '_'.join(['qc' + str(x) for x in qc]) if qc else None
    qc_report_extensions = '_'.join(['qcr' + str(x) for x in qc_report]) if qc_report else None
    if qc_report:
        logging.info('qc report extension'.format(qc_report))
    # Create canvas for aggregations  
    canvas = create_canvas(REGIONS.get(region), DEGREE_FACTOR_RESOLUTION.get(resolution))

    table = 'header'
    logging.info('Processing table {}'.format(table))
    pattern = '-'.join([table, '*', cdm_id]) + '.psv'
    header_path = os.path.join(dir_data, pattern)
    all_files = glob.glob(header_path)
    if len(all_files) == 0:
        logging.error('No files found {}'.format(header_path))
        return

    header_df = dd.read_csv(header_path, delimiter=DELIMITER, usecols=READ_COLS_HDR, parse_dates=['report_timestamp'],
                            dtype=DTYPES_HDR)

    scratch_dir = scratch_dir if scratch_dir else dir_data
    logging.info('Saving hdr file to parquet in {}'.format(scratch_dir))
    try:
        header_parq_path = os.path.join(scratch_dir, 'header.parq.tmp')
        with diag.ProgressBar(), diag.Profiler() as prof, diag.ResourceProfiler(0.5) as rprof:
            header_df.to_parquet(header_parq_path)
        del header_df

        logging.info('Reading header from parquet')
        with diag.ProgressBar(), diag.Profiler() as prof, diag.ResourceProfiler(0.5) as rprof:
            header_df = dd.read_parquet(header_parq_path)
        if qc_report:
            logging.info('Subsetting header report_quality')
            header_df = header_df[header_df['report_quality'].isin(qc_report)]
        header_df = header_df.set_index(header_df['report_id'])
        qcr_index = header_df.index.compute()

        # Aggregate header abd save if general case or requested
        if not tables or 'header' in tables:
            logging.info('Aggregating header data (counts)')
            xarr_dict = {x: '' for x in AGGREGATIONS_HDR}

            for agg in AGGREGATIONS_HDR:
                logging.info('Applying {} aggregation'.format(agg))
                xarr_dict[agg] = canvas.points(header_df, 'longitude', 'latitude',
                                               DS_AGGREGATIONS_HDR.get(agg)('crs'))
            logging.info('Removing parquet files')
            shutil.rmtree(header_parq_path)
            # Merge aggs in a single xarr
            logging.info('Merging and saving nc file')
            xarr = xr.merge([v.rename(k) for k, v in xarr_dict.items()])
            xarr.encoding = ENCODINGS_HDR
            # Save to nc
            nc_dir = dir_data if not nc_dir else nc_dir
            nc_name = '-'.join(filter(bool, [table, cdm_id, qc_report_extensions])) + '.nc'
            xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS_HDR, mode='w')
        else:
            shutil.rmtree(header_parq_path)
    except Exception:
        logging.error('Error processing header tables', exc_info=True)
        logging.info('Removing parquet files')
        shutil.rmtree(header_parq_path)
        return

    obs_tables = [x for x in properties.cdm_tables if x != 'header']
    if tables:
        obs_tables = [x for x in obs_tables if x in tables]
    for table in obs_tables:
        logging.info('Processing table {}'.format(table))
        pattern = '-'.join([table, '*', cdm_id]) + '.psv'
        table_path = os.path.join(dir_data, pattern)
        all_files = glob.glob(table_path)
        if len(all_files) == 0:
            logging.warning('Table {0}. No files found {1}'.format(table, table_path))
            continue

        obs_df = dd.read_csv(table_path, delimiter=DELIMITER, usecols=READ_COLS, parse_dates=['date_time'],
                             dtype=DTYPES)
        logging.info('Saving obs file to parquet in {}'.format(scratch_dir))
        try:
            obs_parq_path = os.path.join(scratch_dir, table + '.parq.tmp')
            with diag.ProgressBar(), diag.Profiler() as prof, diag.ResourceProfiler(0.5) as rprof:
                obs_df.to_parquet(obs_parq_path)
            del obs_df
            # Read the data
            with diag.ProgressBar(), diag.Profiler() as prof, diag.ResourceProfiler(0.5) as rprof:
                obs_df = dd.read_parquet(obs_parq_path)
            obs_df = obs_df.set_index(obs_df['report_id'])
            qcr_index = qcr_index[qcr_index.isin(obs_df.index.compute())]
            obs_df = obs_df.loc[list(qcr_index)]
            if qc:
                logging.info('Subsetting by obs parameter quality flag')
                obs_df = obs_df.loc[obs_df['quality_flag'].isin(qc)]

            logging.info('Aggregating data')
            xarr_dict = {x: '' for x in AGGREGATIONS}
            for agg in AGGREGATIONS:
                logging.info('Applying {} aggregation'.format(agg))
                xarr_dict[agg] = canvas.points(obs_df, 'longitude', 'latitude',
                                               DS_AGGREGATIONS.get(agg)('observation_value'))
            logging.info('Removing parquet files')
            shutil.rmtree(obs_parq_path)
            # Merge aggs in a single xarr
            logging.info('Merging and saving nc file')
            xarr = xr.merge([v.rename(k) for k, v in xarr_dict.items()])
            xarr.encoding = ENCODINGS
            # Save to nc
            nc_name = '-'.join(filter(bool, [table, cdm_id, qc_report_extensions, qc_extensions])) + '.nc'
            xarr.to_netcdf(os.path.join(nc_dir, nc_name), encoding=ENCODINGS_HDR, mode='w')

        except Exception:
            logging.error('Error processing table {}'.format(table), exc_info=True)
            logging.info('Removing parquet files')
            shutil.rmtree(obs_parq_path)
            return

# TODO: this function is incomplete
# def global_from_monthly_nc():
#     """
#
#     Returns
#     -------
#
#     """
#     print('hi, not there yet')
#     return

# if __name__=='__main__':
#     kwargs = dict(arg.split('=') for arg in sys.argv[2:])
#     if 'sections' in kwargs.keys():
#         kwargs.update({ 'sections': [ x.strip() for x in kwargs.get('sections').split(",")] })
#     read(sys.argv[1],
#          **kwargs) # kwargs
