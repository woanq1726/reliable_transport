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

    # Handshake: gửi START, chờ ACK(seq=1)
    s.settimeout(TIMEOUT)
    while True:
        s.sendto(make_pkt(0, 0), dest)
        try:
            raw, _ = s.recvfrom(2048)
            hdr = PacketHeader(raw[:16])
            if hdr.type == 3 and hdr.seq_num == 1:
                break
        except socket.timeout:
            pass

    # Sliding window, DATA seq_num bắt đầu từ 1
    base = 1
    nxt = 1
    pkts = [make_pkt(2, i + 1, chunks[i]) for i in range(total)]
    timer = time.time()

    while base <= total:
        # Gửi các gói mới trong cửa sổ
        while nxt <= min(base + window_size - 1, total):
            s.sendto(pkts[nxt - 1], dest)
            nxt += 1

        # Tính thời gian còn lại trong timer
        remaining = TIMEOUT - (time.time() - timer)
        if remaining <= 0:
            # Timeout: gửi lại toàn bộ cửa sổ
            for i in range(base, nxt):
                s.sendto(pkts[i - 1], dest)
            timer = time.time()
            continue

        # Chờ ACK
        try:
            s.settimeout(remaining)
            raw, _ = s.recvfrom(2048)
            hdr = PacketHeader(raw[:16])
            if hdr.type == 3 and hdr.seq_num > base:
                base = hdr.seq_num  # cumulative ACK
                timer = time.time()  # reset timer khi window tiến
        except socket.timeout:
            pass

    # Gửi END
    end_seq = total + 1
    end_pkt = make_pkt(1, end_seq)
    t = time.time()
    while True:
        s.sendto(end_pkt, dest)
        try:
            s.settimeout(0.1)
            raw, _ = s.recvfrom(2048)
            hdr = PacketHeader(raw[:16])
            if hdr.type == 3 and hdr.seq_num == end_seq:
                break
        except socket.timeout:
            pass
        if time.time() - t >= TIMEOUT:
            break

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
