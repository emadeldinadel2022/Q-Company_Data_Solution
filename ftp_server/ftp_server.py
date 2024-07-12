from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

def main():
    authorizer = DummyAuthorizer()
    authorizer.add_anonymous("/var/ftp", perm="elradfmw")

    handler = FTPHandler
    handler.authorizer = authorizer
    handler.passive_ports = range(60000, 60010) 

    handler.masquerade_address = "172.17.0.2"

    server = FTPServer(("0.0.0.0", 21), handler)
    server.serve_forever()

if __name__ == "__main__":
    main()