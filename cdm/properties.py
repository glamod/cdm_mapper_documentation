import os
import glob

mappings_lib = os.path.join(os.path.dirname(__file__),'lib','mappings')
supported_models = [ os.path.basename(x).split(".")[0] for x in glob.glob(mappings_lib + '/*/*.py') if os.path.basename(x).split(".")[0] == os.path.dirname(x).split("/")[-1]]

cdm_tables = ['header','observations-at','observations-sst',
              'observations-dpt','observations-wbt',
              'observations-wd','observations-ws',
              'observations-slp']

# DATA TYPES ------------------------------------------------------------------
numpy_integers = ['int8','int16','int32','int64','uint8','uint16','uint32','uint64']
numpy_floats = ['float16','float32','float64']

pandas_nan_integers = {'int8':'Int8','int16':'Int16','int32':'Int32',
                       'int64':'Int64','uint8':'UInt8','uint16':'UInt16',
                       'uint32':'UInt32','uint64':'UInt64'}

numeric_types = numpy_integers.copy()
numeric_types.extend(numpy_floats)
numeric_types.extend(pandas_nan_integers.values())
object_types = ['str','object','key','datetime']

data_types = object_types.copy()
data_types.extend(numpy_integers)
data_types.extend(numpy_floats)

# In pandas read --------------------------------------------------------------
pandas_dtypes = {}
#....from input data attributes expected types
pandas_dtypes['from_atts'] = {}
for dtype in object_types:
    pandas_dtypes['from_atts'][dtype] = 'object'
pandas_dtypes['from_atts'].update({ x:x for x in numeric_types })
#...from CDM table definitions psuedo-sql(...) --------------------------------
pandas_dtypes['from_sql'] = {}
pandas_dtypes['from_sql']['timestamp with timezone'] = 'object'
pandas_dtypes['from_sql']['numeric'] = 'float64'
pandas_dtypes['from_sql']['int'] = 'int'

# Some defaults ---------------------------------------------------------------
default_decimal_places = 5
