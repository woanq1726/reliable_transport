import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    def make_ack(seq_num):
        hdr = PacketHeader(type=3, seq_num=seq_num, length=0)
        hdr.checksum = 0
        hdr.checksum = compute_checksum(hdr)
        return bytes(hdr)

    def parse_packet(raw):
        if len(raw) < 16:
            return None, None
        hdr = PacketHeader(raw[:16])
        if len(raw) != 16 + hdr.length:
            return None, None
        payload = raw[16:16 + hdr.length]
        saved = hdr.checksum
        hdr.checksum = 0
        if compute_checksum(hdr / payload) != saved:
            return None, None
        hdr.checksum = saved
        return hdr, payload

    connected = False
    sender_addr = None
    next_seq = 1
    buf = {}

    while True:
        raw, addr = s.recvfrom(2048)
        hdr, payload = parse_packet(raw)
        if hdr is None:
            continue

        if hdr.type == 0:
            if not connected:
                connected = True
                sender_addr = addr
                next_seq = 1
                buf = {}
                s.sendto(make_ack(1), addr)
            elif addr == sender_addr and next_seq == 1 and not buf:
                s.sendto(make_ack(1), addr)
            continue

        if not connected or addr != sender_addr:
            continue

        if hdr.type == 1:
            s.sendto(make_ack(hdr.seq_num), addr)
            break

        if hdr.type == 2:
            seq = hdr.seq_num
            if seq < next_seq:
                s.sendto(make_ack(seq), addr)
                continue
            if seq >= next_seq + window_size:
                continue
            if seq not in buf:
                buf[seq] = payload
            while next_seq in buf:
                sys.stdout.buffer.write(buf.pop(next_seq))
                sys.stdout.buffer.flush()
                next_seq += 1
            s.sendto(make_ack(seq), addr)

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
