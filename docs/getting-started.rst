.. _getting-started:

Getting started
===============

1. Test the tool
~~~~~~~~~~~~~~~~
You can test the tool very easy by just calling both modules installed, the mdf_reader and the cdm. For this you need to run the following code::

   import os
   import sys
   sys.path.append('/path_to_folder_directory_containing_the_cdm_and_mdf_reader_folder/')
   import cdm
   import json
   import mdf_reader
   import warnings
   warnings.filterwarnings('ignore')

If there is no error everything went well in the installation!

2. Read a sample ``.imma``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Read a file from the folder ``~/mdf_reader/test/data/`` via the following code::

    schema = 'imma1_d704'
    data_file_path = '~/mdf_reader/test/data/125-704_1879-01_subset.imma'
    data_raw = mdf_reader.read(data_file_path, data_model = schema)
    attributes = data_raw.atts.copy()

This ``data_raw`` and its ``attributes`` will be the input to the ``cdm.map_model()`` function.

3. Map this data to a CDM build for the same deck
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In this case deck 704: US Marine Meteorological Journal collection of data code::

    name_of_model = 'icoads_r3000_d704'

    cdm_dict = cdm.map_model(name_of_model, data_raw.data, attributes,
                             cdm_subset = None, log_level = 'DEBUG')

4. Write the output
~~~~~~~~~~~~~~~~~~~
This writes the output to an ascii file with a pipe delimited format using the following function::

    cdm.cdm_to_ascii(cdm_dict, delimiter = '|', extension = 'psv', null_label = 'null', out_dir = None, suffix = None, prefix = None, log_level = 'INFO')

For more details and an overview of the tool check out the following python notebook:

- `CDM mapper example <https://git.noc.ac.uk/brecinosrivas/cdm-mapper/-/blob/master/docs/notebooks/CDM_mapper_example_deck704.ipynb>`_
