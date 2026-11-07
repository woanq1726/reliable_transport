import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

CHUNK_SIZE = 1456
TIMEOUT = 0.5


def sender(receiver_ip, receiver_port, window_size):
    data = sys.stdin.buffer.read()
    chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    total = len(chunks)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = (receiver_ip, receiver_port)

    def make_pkt(pkt_type, seq_num, payload=b''):
        hdr = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload))
        hdr.checksum = 0
        hdr.checksum = compute_checksum(hdr / payload)
        return bytes(hdr / payload)

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

    s.settimeout(TIMEOUT)
    while True:
        s.sendto(make_pkt(0, 0), dest)
        try:
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_packet(raw)
            if hdr is not None and hdr.type == 3 and hdr.seq_num == 1:
                break
        except socket.timeout:
            pass

    base = 1
    nxt = 1
    pkts = [make_pkt(2, i + 1, chunks[i]) for i in range(total)]
    acked = set()
    timer = time.time()

    while base <= total:
        old_base = base
        while base <= total and base in acked:
            acked.discard(base)
            base += 1

        if base > old_base:
            timer = time.time()

        if base > total:
            break

        while nxt <= min(base + window_size - 1, total):
            s.sendto(pkts[nxt - 1], dest)
            nxt += 1

        remaining = TIMEOUT - (time.time() - timer)
        if remaining <= 0:
            for i in range(base, nxt):
                if i not in acked:
                    s.sendto(pkts[i - 1], dest)
            timer = time.time()
            continue

        try:
            s.settimeout(remaining)
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_packet(raw)
            if hdr is not None and hdr.type == 3:
                seq = hdr.seq_num
                if base <= seq <= min(base + window_size - 1, total):
                    acked.add(seq)
        except socket.timeout:
            pass

    end_seq = total + 1
    end_pkt = make_pkt(1, end_seq)
    t = time.time()
    last_sent = 0
    while time.time() - t < TIMEOUT:
        now = time.time()
        if now - last_sent >= 0.1:
            s.sendto(end_pkt, dest)
            last_sent = now
        remaining = min(0.1, TIMEOUT - (now - t))
        if remaining <= 0:
            break
        try:
            s.settimeout(remaining)
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_packet(raw)
            if hdr is not None and hdr.type == 3 and hdr.seq_num == end_seq:
                break
        except socket.timeout:
            pass

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
