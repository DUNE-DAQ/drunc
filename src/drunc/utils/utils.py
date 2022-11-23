

def get_new_port():
    import socket
    sock = socket.socket()
    sock.bind(('', 0))
    return sock.getsockname()[1]


def now_str():
    from datetime import datetime
    return datetime.now().strftime("%m/%d/%Y,%H:%M:%S.%f")
