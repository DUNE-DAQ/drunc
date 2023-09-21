from drunc.controller.children_interface.child_node import ChildNode

class ResponseListener:
    _instance = None

    def __init__(self):
        raise RuntimeError('Call get() instead')

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            from drunc.utils.utils import get_new_port
            cls.port = get_new_port()



        return cls._instance

    @classmethod
    def get_port(cls):
        return cls.port

class RESTAPIChildNode(ChildNode):
    def __init__(self, child_conf, **kwargs):
        super(RESTAPIChildNode, self).__init__(
            **kwargs
        )
        self.address = child_conf['address']
        self.response_listener = ResponseListener.get()
        self.proxy = child_conf.get('proxy')

    def close(self):
        pass


    def propagate_command(self, command, data, token):
        headers = {
            "content-type": "application/json",
            "X-Answer-Port": str(ResponseListener.get().get_port()),
        }


        import requests, json

        ack = requests.post(
            self.address,
            data=json.dumps(command),
            headers=headers,
            timeout=1.,
            proxies={
                'http': f'socks5h://{self.proxy}',
                'https': f'socks5h://{self.proxy}'
            } if self.proxy else None
        )


