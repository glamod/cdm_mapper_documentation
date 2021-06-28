#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 10:31:18 2019

Functions to map NOC processed WMO-PUB47 to the CDM.

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
import numpy as np
import pandas as pd


class mapping_functions():
    def __init__(self, atts):
        self.atts = atts

    def string_opposite(self,ds):
        return '-' + ds
    
    def select_column(self,df):
        c = df.columns.to_list()
        c.reverse()
        s = df[c[0]].copy()
        if len(c)> 1:
            for ci in c[1:]:
                s.update(df[ci])
        return s
