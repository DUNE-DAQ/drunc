"""
Proxing/Delegation tools

Provide several tools to implement proxying/delegation of objects. The proxying
instances expose the same public interface of the proxied object, but avoiding
inheritance. This allows to control the reference counts of the proxied object.

"""
from __future__ import annotations
__all__: list[str] = ['Proxy', 'make_proxy_class']
class Proxy:
    """
    A very basic holder class
    
        Just holds the reference to a provided object.
    
        
    """
    def __init__(self, obj):
        ...
def _DelegateMetaFunction(clsName, bases, atts):
    """
     Implements a delegation pattern using a metaclass approach
    
        A class using this meta mechanism should have 'memberclass' class attribute
        initialized at the class of the instance to proxied. The metaclass will
        make sure the delegate class will expose all the public methods of the
        proxied one.
        Moreover, the metaclass will provide the delegate class with a '__init__'
        function instantiating a 'memberclass' object, storing it in 'self._obj'.
        The delegate class constructor method will therefore accept all the
        arguments accepted by the proxied class constructor.
        The delegate class uses slots
    
        
    """
def make_proxy_class(theclass):
    """
    Builds a delegation class out of a given type.
    
        Uses the Proxy class to generate a new proxy class exposing the same
        interface of the provided class and delegating the method calls to
        the hosted object instance.
    
        
    """
