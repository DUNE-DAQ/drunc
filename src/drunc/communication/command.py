from datetime import datetime

class Command:
    def __init__(self, name:str, data:dict):
        self.name = name
        self.data = data

class CommandResponse:
    def __init__(self, response_code: str, name:str, data:dict, time:datetime, sender:str):
        self.response_code = response_code
        self.name = name
        self.data = data
        self.time = time
        self.sender = sender
