#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 10:34:56 2019

Module with functions to handle pd.io.parsers.TextFileReader objects.

@author: iregon
"""

import pandas as pd
import cdm.common.logging_hdlr as logging_hdlr

logger = logging_hdlr.init_logger(__name__, level='ERROR')

def restore(TextParser_ref,TextParser_options):
    try:
        TextParser_ref.seek(0)
        TextParser = pd.read_csv( TextParser_ref, names = TextParser_options['names'],chunksize = TextParser_options['chunksize'], dtype = TextParser_options['dtype']) #, skiprows = options['skiprows'])
        return TextParser
    except Exception as e:
        logger.error('Failed to restore TextParser', exc_info=True)
        return TextParser

def is_not_empty(TextParser):
    try:
        TextParser_ref = TextParser.f
        TextParser_options = TextParser.orig_options
    except Exception as e:
        logger.error('Failed to process input. Input type is {}'.format(type(TextParser)), exc_info=True)
        return
    try:
        first_chunk = TextParser.get_chunk()
        TextParser = restore(TextParser_ref,TextParser_options)
        if len(first_chunk) > 0:
            logger.debug('Is not empty')
            return True, TextParser
        else:
            return False, TextParser
    except Exception:
         return False, TextParser
