#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 10:31:18 2019

imodel: imma1

Functions to map imodel elements to CDM elements

Main functions are those invoqued in the mappings files (table_name.json)

Main functions need to be part of class mapping_functions()

Main functions get:
    - 1 positional argument (pd.Series or pd.DataFrame with imodel data or
    imodel element name)
    - Optionally, keyword arguments

Main function return: pd.Series, np.array or scalars

Auxiliary functions can be used and defined in or outside class mapping_functions

@author: iregon
"""
import math
import numpy as np
import pandas as pd
import datetime


def longitude_360to180_i(lon):
    if lon > 180:
        return -180 + math.fmod(lon, 180)
    else:
        return lon


def location_accuracy_i(li, lat):
    #    math.sqrt(111**2)=111.0
    #    math.sqrt(2*111**2)=156.97770542341354
    #   Previous implementation:
    #    degrees = {0: .1,1: 1,2: fmiss,3: fmiss,4: 1/60,5: 1/3600,imiss: fmiss}
    degrees = {0: .1, 1: 1, 4: 1 / 60, 5: 1 / 3600}
    deg_km = 111
    accuracy = degrees.get(int(li), np.nan) * math.sqrt((deg_km ** 2) * (1 + math.cos(math.radians(lat)) ** 2))
    return np.nan if np.isnan(accuracy) else max(1, int(round(accuracy)))


def string_add_i(a, b, c, sep):
    if b:
        return sep.join(filter(None, [a, b, c]))
    else:
        return


class mapping_functions():
    def __init__(self, atts):
        self.atts = atts

    def datetime_decimalhour_to_HM(self, ds):
        hours = int(math.floor(ds))
        minutes = int(math.floor(60.0 * math.fmod(ds, 1)))
        return hours, minutes

    def datetime_imma1(self, df):  # TZ awareness?
        date_format = "%Y-%m-%d-%H-%M"
        hours, minutes = np.vectorize(mapping_functions(self.atts).datetime_decimalhour_to_HM)(df.iloc[:, -1].values)
        # the mapping_functions(self.atts) does not look right, but otherwise it won't recognize things,
        # seems it need to be
        # newly instantiated to be vectorized
        df.drop(df.columns[len(df.columns) - 1], axis=1, inplace=True)
        df['H'] = hours
        df['M'] = minutes
        # VALUES!!!!
        data = pd.to_datetime(df.astype(str).apply("-".join, axis=1).values, format=date_format, errors='coerce')
        return data

    def datetime_utcnow(self):
        return datetime.datetime.utcnow()

    def decimal_places(self, element):
        return self.atts.get(element[0]).get('decimal_places')

    def decimal_places_temperature_kelvin(self, element):
        origin_decimals = self.atts.get(element[0]).get('decimal_places')
        if origin_decimals <= 2:
            return 2
        else:
            return origin_decimals

    def decimal_places_pressure_pascal(self, element):
        origin_decimals = self.atts.get(element[0]).get('decimal_places')
        if origin_decimals > 2:
            return origin_decimals - 2
        else:
            return 0

    def df_col_join(self, df, sep):
        joint = df.iloc[:, 0].astype(str)
        for i in range(1, len(df.columns)):
            joint = joint + sep + df.iloc[:, i].astype(str)
        return joint

    def float_scale(self, ds, factor=1):
        return ds * factor

    def integer_to_float(self, ds, float_type='float32'):
        return ds.astype(float_type)

    def lineage(self, ds):
        return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') + ". Initial conversion from ICOADS R3.0.0T"

    def location_accuracy(self, df): # (li_core,lat_core) math.radians(lat_core)
        la = np.vectorize(location_accuracy_i, otypes='f')(df.iloc[:, 0], df.iloc[:, 1])  # Last minute tweak so that
        # is does no fail on nans!
        return la

    def longitude_360to180(self, ds):
        lon = np.vectorize(longitude_360to180_i)(ds)
        return lon

    def observing_programme(self, ds):
        op = {str(i): [5, 7, 56] for i in range(0, 6)}
        op.update({'7': [5, 7, 9]})
        return ds.map(op, na_action='ignore')
        # Previous version:
        # observing_programmes = { range(1, 5): '{7, 56}',7: '{5,7,9}'}
        # if no PT, assume ship
        # Set only for drifting buoys. Rest assumed ships!
        # return df[df.columns[0]].swifter.apply( lambda x: '{5,7,9}' if x == 7 else '{7,56}')

    def string_add(self, ds, prepend=None, append=None, separator=None, zfill_col=None, zfill=None):
        prepend = '' if not prepend else prepend
        append = '' if not append else append
        separator = '' if not separator else separator
        if zfill_col and zfill:
            for col, width in zip(zfill_col, zfill):
                df.iloc[:, col] = df.iloc[:, col].astype(str).str.zfill(width)
        ds['string_add'] = np.vectorize(string_add_i)(prepend, ds, append, separator)
        return ds['string_add']

    def string_join_add(self, df, prepend=None, append=None, separator=None, zfill_col=None, zfill=None):
        separator = '' if not separator else separator
        # This duplication is to prevent error in Int to object casting of types
        # when nrows ==1, shown after introduction of nullable integers in objects.
        duplicated = False
        if len(df) == 1:
            df = pd.concat([df, df])
            duplicated = True
        if zfill_col and zfill:
            for col, width in zip(zfill_col, zfill):
                df.iloc[:, col] = df.iloc[:, col].astype(str).str.zfill(width)
        joint = mapping_functions(self.atts).df_col_join(df, separator)
        df['string_add'] = np.vectorize(string_add_i)(prepend, joint, append, separator)
        if duplicated:
            df = df[:-1]
        return df['string_add']

    def temperature_celsius_to_kelvin(self, ds):
        return ds + 273.15

    def time_accuracy(self, ds):  # ti_core
        # Shouldn't we use the code_table mapping for this? see CDM!
        secs = {'0': 3600, '1': int(round(3600 / 10)), '2': int(round(3600 / 60)), '3': int(round(3600 / 100))}
        return ds.map(secs, na_action='ignore')

    def feet_to_m(self, ds, float_type='float32'):
        ds.astype(float_type)
        return np.round(ds/3.2808, 2)
