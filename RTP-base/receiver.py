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

    connected = False
    sender_addr = None
    next_seq = 1
    buf = {}
    result = []

    while True:
        raw, addr = s.recvfrom(2048)
        hdr = PacketHeader(raw[:16])
        payload = raw[16:16 + hdr.length]

        # Kiểm tra checksum, bỏ gói nếu sai
        saved = hdr.checksum
        hdr.checksum = 0
        if compute_checksum(hdr / payload) != saved:
            continue
        hdr.checksum = saved

        # START
        if hdr.type == 0:
            if not connected:
                connected = True
                sender_addr = addr
                next_seq = 1
                buf = {}
                result = []
                s.sendto(make_ack(1), addr)
            continue

        if not connected or addr != sender_addr:
            continue

        # END
        if hdr.type == 1:
            sys.stdout.buffer.write(b''.join(result))
            sys.stdout.buffer.flush()
            s.sendto(make_ack(hdr.seq_num), addr)
            break

        # DATA
        if hdr.type == 2:
            seq = hdr.seq_num

            # Bỏ gói nằm ngoài cửa sổ
            if seq < next_seq or seq >= next_seq + window_size:
                s.sendto(make_ack(next_seq), addr)
                continue

            if seq not in buf:
                buf[seq] = payload

            # Trượt cửa sổ
            while next_seq in buf:
                result.append(buf.pop(next_seq))
                next_seq += 1

            # Cumulative ACK: seq_num = số tiếp theo đang chờ
            s.sendto(make_ack(next_seq), addr)

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
