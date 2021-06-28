 # Following to access the subpackages main modules (or/and functions) directly wihout loops through the full subpackage path
from .mapper.mapper import map_model as map_model
from .table_writer.table_writer import cdm_to_ascii as cdm_to_ascii
from .table_writer.table_writer import table_to_ascii as table_to_ascii
from .table_reader.table_reader import read_tables as read_tables
from .gridded_stats import gridded_stats
