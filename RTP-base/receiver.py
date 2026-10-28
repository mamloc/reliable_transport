
import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3

HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472


def make_packet(packet_type, seq_num, payload=b""):
    header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0,
    )

    packet = header / payload
    header.checksum = compute_checksum(packet)

    return bytes(header / payload)


def parse_packet(packet):
    if len(packet) < HEADER_SIZE or len(packet) > MAX_PACKET_SIZE:
        return None

    header = PacketHeader(packet[:HEADER_SIZE])

    if len(packet) < HEADER_SIZE + header.length:
        return None

    payload = packet[HEADER_SIZE:HEADER_SIZE + header.length]

    received_checksum = header.checksum
    header.checksum = 0

    if compute_checksum(header / payload) != received_checksum:
        return None

    return header, payload


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected_seq = 0
    buffer = {}
    started = False

    try:
        while True:
            packet, address = s.recvfrom(2048)
            parsed = parse_packet(packet)

            if parsed is None:
                continue

            header, payload = parsed

            if header.type == START:
                if header.seq_num != 0:
                    continue

                started = True
                expected_seq = 1
                buffer.clear()

                s.sendto(make_packet(ACK, expected_seq), address)

            elif not started:
                continue

            elif header.type == DATA:
                if header.seq_num >= expected_seq:
                    buffer.setdefault(header.seq_num, payload)

                while expected_seq in buffer:
                    data = buffer.pop(expected_seq)
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                    expected_seq += 1

                s.sendto(make_packet(ACK, expected_seq), address)

            elif header.type == END:
                if header.seq_num == expected_seq:
                    s.sendto(make_packet(ACK, expected_seq + 1), address)
                    break

    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)

    args = parser.parse_args()

    receiver(
        args.receiver_ip,
        args.receiver_port,
        args.window_size,
    )


if __name__ == "__main__":
    main()
