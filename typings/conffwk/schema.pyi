"""
A set of utilities to simplify OKS instrospection.
"""

from __future__ import annotations

import logging as logging
import re as re
import sys as sys

from conffwk import ConfigObject

__all__: list[str] = [
    "Cache",
    "ConfigObject",
    "check_cardinality",
    "check_range",
    "check_relation",
    "coerce",
    "decode_range",
    "logging",
    "map_coercion",
    "oks_types",
    "range_regexp",
    "re",
    "str2integer",
    "sys",
    "to_int",
    "to_long",
]

class Cache:
    """
    Defines a cache for all known schemas at a certain time.

    """
    def __getitem__(self, key):
        """
        Gets the description of a certain class.
        """
    def __init__(self, conffwk, all=True):
        """
        Initializes the cache with information from the Configuration
                object.

                This method will browse for all declared classes in the Configuration
                object given as input and will setup the schema for all known classes.
                After this you can still update the cache using the update() method.

                Keyword parameters:

                conffwk -- The conffwk.Configuration object to use as base for the
                current cache.

                all -- A boolean indicating if I should store all the attributes and
                relations from a certain class or just the ones directly associated
                with a class.

        """
    def __str__(self):
        """
        Prints a nice display of myself
        """
    def update(self, conffwk):
        """
        Updates this cache with information from the Configuration object.

                This method will add new classes not yet know to this cache. Classes
                with existing names will not be added. No warning is generated (this
                should be done by the OKS layer in any case.

        """
    def update_dal(self, conffwk):
        """
        Updates this cache with information for DAL.

                This method will add new DAL classes not yet know to this cache.
                Classes with existing DAL representations will not be touched.

        """

def check_cardinality(v, prop):
    """
    Checks the cardinality of a certain attribute or relationship.
    """

def check_range(v, range, range_re, pytype):
    """
    Checks the range of the value 'v' to make sure it is inside.
    """

def check_relation(v, rel):
    """
    Checks the value v against the relationship parameters in 'rel'.
    """

def coerce(v, attr):
    """
    Coerces the input value 'v' in the way the attribute expects.
    """

def decode_range(s):
    """
    Decodes a range string representation, returns a tuple with 2 values.

        This is the supported format in regexp representation:
        '([-0x]*\\d+)\\D+-?\\d+'

    """

def map_coercion(class_name, schema):
    """
    Given a schema of a class, maps coercion functions from libpyconffwk.
    """

def str2integer(v, t, max):
    """
    Converts a value v to integer, irrespectively of its formatting.

        If the number starts with a '0', we convert it using an octal
        representation. Else, we try a decimal conversion. If any of these fail,
        we try an hexa conversion before throwing a ValueError.

        Keyword arguments:

        v -- the value to be converted
        t -- the python type (int or float) to use in the conversion

    """

def to_int(v): ...
def to_long(v): ...

oks_types: dict = {
    "bool": ["bool"],
    "integer": ["s8", "u8", "s16", "u16", "s32"],
    "long": ["u32", "s64", "u64"],
    "float": ["float", "double"],
    "int-number": ["s8", "u8", "s16", "u16", "s32", "u32", "s64", "u64"],
    "number": ["u32", "s64", "u64", "s8", "u8", "s16", "u16", "s32", "float", "double"],
    "time": ["date", "time"],
    "string": ["date", "time", "string", "uid", "enum", "class"],
}
range_regexp: re.Pattern  # value = re.compile('(?P<s1>-?0?x?[\\da-fA-F]+(\\.\\d+)?)-(?P<s2>-?0?x?[\\da-fA-F]+(\\.\\d+)?)')
