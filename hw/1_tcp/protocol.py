# An as of yet failed attempt to make a c-python impl
#import tcp

#class MyTCPProtocol():
    #def __init__(self, *, local_addr, remote_addr):
        #self.state = tcp.State(local_addr, remote_addr)
    #def send(self, data):
        #return self.state.sendto(data);
    #def recv(self, n):
        #return self.state.recvfrom(n)
    #def close(self):
        #self.state.close()

import socket
import struct
import numpy as np
import threading
import atomics
from testable_thread import TestableThread


# 2 bytes for fill amount & flags
MAX_SEG_SIZE = 1 << 10 - 1
MAX_SEG_SIZE_MASK = MAX_SEG_SIZE
REQUEST_FLAG = 1 << 15
RECEIVED_FLAG = 1 << 15
BUF_LEN = 1 << 16
BUF_BYTE_LEN = BUF_LEN * MAX_SEG_SIZE
HEADER_SIZE = 6 # 4 bytes for seq number, 2 for size & flags
PACKET_SIZE = MAX_SEG_SIZE + HEADER_SIZE

def seg_count(nbytes: int) -> int:
    return (nbytes - 1) // MAX_SEG_SIZE + 1

class MyTCPProtocol():
    def __init__(self, *, local_addr, remote_addr, name='<none>'):
        self.udp_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)
        self.udp_socket.settimeout(0.001)
        self.out_buffer = np.zeros(BUF_BYTE_LEN, dtype='byte')
        self.out_buffer_pos = 0
        self.in_buffer = np.zeros(BUF_BYTE_LEN, dtype='byte')
        self.in_buffer_pos = 0
        self.out_buffer_metadatas = [(int(0), bool(False))] * BUF_LEN
        self.in_buffer_metadatas = [(int(0), bool(False))] * BUF_LEN
        self.request_counts = [int(0)] * BUF_LEN
        self.closing = atomics.atomic(width=1, atype=atomics.INT)
        self.closing.store(0)
        self.segment_arrived_evt = threading.Event()
        self.segment_arrived_evt.set()
        self.awaited_segment_at = 0
        self.listener_thread = None
        self.name = name

    def send_data_segment(self, at: int, size: int):
        assert size <= self.seg_size
        header = struct.pack('@IH', at, 0)
        data = self.out_buffer[(at * self.seg_size) % BUF_BYTE_LEN:((at * self.seg_size) % BUF_BYTE_LEN) + size].tobytes()
        self.udp_socket.sendto(header + data, self.remote_addr) 

    def send_request_segment(self, at: int):
        header = struct.pack('@IH', at, REQUEST_FLAG)
        self.udp_socket.sendto(header, self.remote_addr) 

    def get_metadata(self, at: int, is_in: bool):
        meta = None
        if is_in:
            meta = self.in_buffer_metadatas[at // self.seg_size]
        else:
            meta = self.out_buffer_metadatas[at // self.seg_size]
        return meta

    def send(self, data):
        if self.listener_thread is None:
            self.seg_size = min(len(data), MAX_SEG_SIZE)
            self.listener_thread = TestableThread(target=self.listener_thread_main)
            self.listener_thread.start()

        segcnt = seg_count(len(data))
        data_arr = np.frombuffer(data, dtype='byte')
        for seg_start in range(0, segcnt * self.seg_size, self.seg_size):
            seg_end = min(len(data), seg_start + self.seg_size)
            out_from = self.out_buffer_pos % BUF_BYTE_LEN
            out_to = out_from + (seg_end - seg_start)
            self.out_buffer[out_from:out_to] = data_arr[seg_start:seg_end]
            self.out_buffer_metadatas[out_from // self.seg_size] = (out_to - out_from, False)
            self.send_data_segment(self.out_buffer_pos // self.seg_size, out_to - out_from)
            self.out_buffer_pos += self.seg_size
        return data_arr.size


    def listener_thread_main(self):
        while self.closing.load() == 0:
            try:
                seg, _ = self.udp_socket.recvfrom(PACKET_SIZE)
            except:
                if not self.segment_arrived_evt.is_set(): # syncs w/ clear in await => no race on awaited_segment_at
                    self.send_request_segment(self.awaited_segment_at)
            else:
                at, flags = struct.unpack('@IH', seg[:HEADER_SIZE])
                size = len(seg) - HEADER_SIZE
                is_req = flags & REQUEST_FLAG
                at_bytes = at * self.seg_size
                at_in_buf = at_bytes % BUF_BYTE_LEN
                if is_req and self.out_buffer_pos >= at_bytes + self.seg_size and self.request_counts[at % BUF_LEN] <= 2:
                    size = self.get_metadata(at_in_buf, False)[0]
                    self.send_data_segment(at, size)
                    self.request_counts[at % BUF_LEN] += 1
                elif not is_req and not self.get_metadata(at_in_buf, True)[1]:
                    self.in_buffer_metadatas[at_in_buf // self.seg_size] = (size, True)
                    self.in_buffer[at_in_buf:at_in_buf + size] = np.frombuffer(seg[HEADER_SIZE:], dtype='byte')
                    if not self.segment_arrived_evt.is_set() and self.awaited_segment_at == at:
                        self.segment_arrived_evt.set()

    def await_segment(self, at: int):
        if not self.segment_arrived_evt.is_set(): # Don't overlap waits, i. e. we are not thread safe
            return
        if not self.get_metadata((at % BUF_LEN) * self.seg_size, True)[1]:
            self.awaited_segment_at = at
            self.segment_arrived_evt.clear()
            self.segment_arrived_evt.wait()
            
    def recv(self, n):
        if self.listener_thread is None:
            self.seg_size = min(n, MAX_SEG_SIZE)
            self.listener_thread = TestableThread(target=self.listener_thread_main)
            self.listener_thread.start()

        data_arr = np.empty(n, dtype='byte')
        out_from = 0
        while out_from < data_arr.size:
            in_from = self.in_buffer_pos % BUF_BYTE_LEN
            in_offset = in_from % self.seg_size
            self.await_segment(self.in_buffer_pos // self.seg_size) 
            seg_size = self.get_metadata(in_from - in_offset, True)[0]
            left_in_seg = seg_size - in_offset
            out_to = min(out_from + left_in_seg, data_arr.size)
            read_size = out_to - out_from
            data_arr[out_from:out_to] = self.in_buffer[in_from:in_from + read_size]
            if in_offset + (out_to - out_from) >= seg_size:
                self.in_buffer_pos += self.seg_size - in_offset
            else:
                self.in_buffer_pos += out_to - out_from
            out_from = out_to
        return data_arr.tobytes()

    def close(self):
        self.closing.store(1)
        if self.listener_thread is not None:
            self.listener_thread.join()
        self.udp_socket.close()

