from typing import Any


class Configuration:
    def __init__(self, connection: str = "oksconflibs:") -> None: ...
    
    def get_dal(
        self,
        class_name: str,
        uid: str,
    ) -> Any: ...
    