#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 08:45:03 2019

@author: iregon
"""

import logging


def init_logger(module, level='DEBUG', fn=None):
    """
    Initialize logging parameters and levels of logging

    Parameters
    ----------
    module: name of the module to log
    level: level of logging. e.g. ``DEBUG``, ``INFO`` etc
    fn: file name

    Returns
    -------
    logging.object: Return a logger with the specified name, or if name is None, return a logger which is the root
        logger of the hierarchy.

    """
    # !!! here overriide potential previous config of logging
    from imp import reload  # python 2.x don't need to import reload, use it directly
    reload(logging)
    level = logging.getLevelName(level)
    logging_params = {
        'level': level,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    }
    if fn is not None:
        logging_params['filename'] = fn
    logging.basicConfig(**logging_params)
    # logging.info('init basic configure of logging success')
    return logging.getLogger(module)
