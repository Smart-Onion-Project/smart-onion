import socket
import threading


class MetricsRealtimeAnalyzer:

    def __init__(self):
        pass

    def client_handler(self, client_socket):
        print("Hi ")

    def run(self, ip='', port=3000, connections_backlog=10):
        while True:
            # create an INET, STREAMing socket
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # bind the socket to a public host, and a well-known port
            serversocket.bind((ip, port))
            # become a server socket
            serversocket.listen(connections_backlog)

            # accept connections from outside
            (clientsocket, address) = serversocket.accept()
            # now do something with the clientsocket
            # in this case, we'll pretend this is a threaded server
            ct = threading.Thread(target=self.client_handler, args=clientsocket)
            ct.run()


MetricsRealtimeAnalyzer().run()