from __future__ import annotations

from typing import List, Optional

from druncschema.controller_pb2 import Argument


class Transition:
    def __init__(self, 
        name: str, 
        source: str, 
        destination: str, 
        arguments: Optional[List[Argument]] = None,
        help: str = ""
        ) -> None:
        self.source = source
        self.destination = destination
        self.name = name
        self.arguments = arguments
        self.help = help

    def __eq__(self, another: object) -> bool:  
        same_name = hasattr(another, "name") and self.name == another.name
        same_destination = (
            hasattr(another, "destination") and self.destination == another.destination
        )
        same_source = hasattr(another, "source") and self.source == another.source
        return same_name and same_destination and same_source

    def __hash__(self) -> int:
        return hash(self.__str__())

    def __str__(self) -> str:
        return f'"{self.name}": "{self.source}" → "{self.destination}"'
