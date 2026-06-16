"""
Python interface to the conffwk package
"""

from __future__ import annotations

import typing

__all__: list[str] = list()

class _ConfigObject:
    def UID(self) -> str:
        """
        Return object identity
        """
    @typing.overload
    def __init__(self) -> None: ...
    @typing.overload
    def __init__(self, arg0: _ConfigObject) -> None: ...
    def class_name(self) -> str:
        """
        Return object's class name
        """
    def contained_in(self) -> str:
        """
        Return the name of the database file this object belongs to.
        """
    def full_name(self) -> str:
        """
        Return full object name
        """
    def get_bool(self, attr: str) -> bool:
        """
        Simple getter function
        """
    def get_bool_vec(self, attr: str) -> list[bool]:
        """
        Getter function for a list
        """
    def get_double(self, attr: str) -> float:
        """
        Simple getter function
        """
    def get_double_vec(self, attr: str) -> list[float]:
        """
        Getter function for a list
        """
    def get_float(self, attr: str) -> float:
        """
        Simple getter function
        """
    def get_float_vec(self, attr: str) -> list[float]:
        """
        Getter function for a list
        """
    def get_obj(self, attrname: str) -> _ConfigObject:
        """
        Get a copy of an object
        """
    def get_objs(self, attr: str) -> list[_ConfigObject]:
        """
        Getter function for a list
        """
    def get_s16(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_s16_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_s32(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_s32_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_s64(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_s64_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_s8(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_s8_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_string(self, attr: str) -> str:
        """
        Simple getter function
        """
    def get_string_vec(self, attr: str) -> list[str]:
        """
        Getter function for a list
        """
    def get_u16(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_u16_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_u32(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_u32_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_u64(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_u64_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def get_u8(self, attr: str) -> int:
        """
        Simple getter function
        """
    def get_u8_vec(self, attr: str) -> list[int]:
        """
        Getter function for a list
        """
    def rename(self, new_id: str) -> None:
        """
        Rename object
        """
    def set_bool(self, name: str, value: bool) -> None:
        """
        Simple setter function
        """
    def set_bool_vec(self, attrname: str, l: list[bool]) -> None:
        """
        Setter function for list
        """
    def set_class(self, name: str, value: str) -> None:
        """
        Set the class name
        """
    def set_class_vec(self, attrname: str, l: list[str]) -> None:
        """
        Set list of classes
        """
    def set_date(self, name: str, value: str) -> None:
        """
        Set the date
        """
    def set_date_vec(self, attrname: str, l: list[str]) -> None:
        """
        Set list of dates
        """
    def set_double(self, name: str, value: float) -> None:
        """
        Simple setter function
        """
    def set_double_vec(self, attrname: str, l: list[float]) -> None:
        """
        Setter function for list
        """
    def set_enum(self, name: str, value: str) -> None:
        """
        Set the enum
        """
    def set_enum_vec(self, attrname: str, l: list[str]) -> None:
        """
        Set list of enums
        """
    def set_float(self, name: str, value: float) -> None:
        """
        Simple setter function
        """
    def set_float_vec(self, attrname: str, l: list[float]) -> None:
        """
        Setter function for list
        """
    def set_obj(
        self, name: str, o: _ConfigObject, skip_non_null_check: bool = False
    ) -> None:
        """
        Set relationship single-value
        """
    def set_objs(
        self, name: str, o: list[_ConfigObject], skip_non_null_check: bool = False
    ) -> None:
        """
        Set relationship multi-value.
        """
    def set_s16(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_s16_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_s32(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_s32_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_s64(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_s64_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_s8(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_s8_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_string(self, name: str, value: str) -> None:
        """
        Simple setter function
        """
    def set_string_vec(self, attrname: str, l: list[str]) -> None:
        """
        Set list of strings
        """
    def set_time(self, name: str, value: str) -> None:
        """
        Set the time
        """
    def set_time_vec(self, attrname: str, l: list[str]) -> None:
        """
        Set list of times
        """
    def set_u16(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_u16_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_u32(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_u32_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_u64(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_u64_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """
    def set_u8(self, name: str, value: int) -> None:
        """
        Simple setter function
        """
    def set_u8_vec(self, attrname: str, l: list[int]) -> None:
        """
        Setter function for list
        """

class _Configuration:
    @typing.overload
    def __init__(self) -> None: ...
    @typing.overload
    def __init__(self, arg0: str) -> None: ...
    def add_include(self, db_name: str, include: str) -> None:
        """
        Add include file to existing database.
        """
    def attributes(self, class_name: str, all: bool) -> dict[str, dict[str, str]]:
        """
        Get the properties of each attribute in a given class
        """
    def classes(self) -> list[str]:
        """
        Get the names of the superclasses for each class
        """
    def commit(self, log_message: str = "") -> None:
        """
        Commit database changes.
        """
    def create_db(self, db_name: str, includes: list[str]) -> None:
        """
        Create a database from a list of files
        """
    @typing.overload
    def create_obj(self, at: str, class_name: str, id: str) -> object:
        """
        Create new object by class name and object id.
        """
    @typing.overload
    def create_obj(self, at: object, class_name: str, id: str) -> object:
        """
        Create new object by class name and object id.
        """
    def destroy_obj(self, object: object) -> None:
        """
        The method tries to destroy given object.
        """
    def get_impl_param(self) -> str:
        """
        Get implementation plug-in parameter used to build conffwk object
        """
    def get_impl_spec(self) -> str:
        """
        Get implementation plug-in and its parameter used to build conffwk object
        """
    def get_includes(self, db_name: str) -> list[str]:
        """
        Returns list of files included by given database.
        """
    def get_obj(self, class_name: str, id: str) -> object:
        """
        Create a configuration object containing the desired entity from the database
        """
    def get_objs(self, class_name: str, query: str = "") -> list[...]:
        """
        Create a list of configuration objects of a given class from the database
        """
    def get_schema_path(self, class_name: str) -> str:
        """
        Get path to schema file with definition of the given class
        """
    def load(self, db_name: str) -> None:
        """
        Load database according to the name.
        """
    def loaded(self) -> bool:
        """
        Check if database is correctly loaded.
        """
    def relations(self, class_name: str, all: bool) -> dict[str, dict[str, str]]:
        """
        Get the properties of each relation in a given class
        """
    def remove_include(self, db_name: str, include: str) -> None:
        """
        Remove include file.
        """
    def subclasses(self, class_name: str, all: bool) -> list[str]:
        """
        Get the subclasses of a single class
        """
    def superclasses(self, class_name: str, all: bool) -> list[str]:
        """
        Get the superclasses of a single class
        """
    def test_object(
        self, class_name: str, id: str, rlevel: int, rclasses: list[str]
    ) -> bool:
        """
        Test the existence of the object
        """
    def unload(self) -> None:
        """
        Unload previously-loaded database
        """
