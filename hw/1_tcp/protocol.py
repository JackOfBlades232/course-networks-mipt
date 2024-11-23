import tcp
#import socket


class MyTCPProtocol():
    def __init__(self, *, local_addr, remote_addr):
        #self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        #self.remote_addr = remote_addr
        #self.udp_socket.bind(local_addr)
        self.state = tcp.State(local_addr, remote_addr)

    def send(self, data):
        #return self.udp_socket.sendto(data, self.remote_addr)
        return self.state.sendto(data);

    def recv(self, n):
        #msg, _ = self.udp_socket.recvfrom(n)
        #return msg
        return self.state.recvfrom(n)

    def close(self):
        #self.udp_socket.close()
        self.state.close()

