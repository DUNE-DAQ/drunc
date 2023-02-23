from drunc.utils.utils import setup_fancy_logging
import logging

class PyLogger:
    def __init__(self, name):
        self.name = name
        self.log = setup_fancy_logging(name)
        self.log = logging.getLogger(self.__class__.__name__)

    def critical(self,message:str) -> None:
        self.log.critical(message)
    def error(self,message:str) -> None:
        self.log.error(message)
    def warning(self,message:str) -> None:
        self.log.warning(message)
    def info(self,message:str) -> None:
        self.log.info(message)
    def debug(self, message:str) -> None:
        self.log.debug(message)
        
    def log(self, level:int, message:str, emitter:str='') -> None:
        message = f'{emitter}: {message}' if emitter else message
        if   level <= 0           : self.log.critical(message)
        elif level == 1           : self.log.error   (message)
        elif level == range( 1,10): self.log.warning (message)
        elif level == range(10,20): self.log.info    (message)
        else                      : self.log.debug   (message)



def test():
    class LogClass(PyLogger):
        def __init__(self, name):
            super().__init__(name)

    lc = LogClass('tester')
    lc.debug('debug message')
    lc.info('info message')
    lc.warning('info message')
    lc.error('error')
    lc.critical('critical')

if __name__ == '__main__':
    test()
