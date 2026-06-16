import __future__
from __future__ import annotations
from conffwk.ConfigObject import ConfigObject
from conffwk.Configuration import Configuration
from . import _daq_conffwk_py
from . import dal
from . import dalproperty
from . import proxy
from . import schema
__all__: list[str] = ['ConfigObject', 'Configuration', 'absolute_import', 'dal', 'dalproperty', 'proxy', 'reset_updated_dals', 'schema', 'updated_dals']
def reset_updated_dals():
    """
    Reset the set keeping track of modified DAL objects
            
    """
def updated_dals():
    """
    Returns a set of DAL objects that were modified in this DB session
            
    """
absolute_import: __future__._Feature  # value = _Feature((2, 5, 0, 'alpha', 1), (3, 0, 0, 'alpha', 0), 262144)
