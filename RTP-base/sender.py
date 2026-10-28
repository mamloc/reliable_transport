
import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3

HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE
TIMEOUT = 0.5


def make_packet(packet_type, seq_num, payload=b""):
    header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0,
    )

    packet = header / payload
    checksum = compute_checksum(packet)
    packet[PacketHeader].checksum = checksum

    return bytes(packet)


def parse_ack(packet):
    if len(packet) < HEADER_SIZE:
        return None

    header = PacketHeader(packet[:HEADER_SIZE])
    payload = packet[HEADER_SIZE:HEADER_SIZE + header.length]

    if header.type != ACK or header.length != 0:
        return None

    received_checksum = header.checksum
    header.checksum = 0

    if compute_checksum(header / payload) != received_checksum:
        return None

    return header.seq_num


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")
    message = sys.stdin.buffer.read()

    chunks = [
        message[i:i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(message), MAX_PAYLOAD_SIZE)
    ]

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(TIMEOUT)

    address = (receiver_ip, receiver_port)

    try:
        start_packet = make_packet(START, 0)
        s.sendto(start_packet, address)

        while True:
            try:
                packet, _ = s.recvfrom(2048)
            except socket.timeout:
                raise TimeoutError("Timed out waiting for START ACK")

            ack_num = parse_ack(packet)
            if ack_num == 1:
                break

        packets = {
            seq: make_packet(DATA, seq, chunk)
            for seq, chunk in enumerate(chunks, start=1)
        }

        total_packets = len(packets)
        base = 1
        next_seq = 1
        timer_start = None

        while base <= total_packets:
            while (
                next_seq <= total_packets
                and next_seq < base + window_size
            ):
                s.sendto(packets[next_seq], address)

                if base == next_seq:
                    timer_start = time.monotonic()

                next_seq += 1

            if timer_start is None:
                timer_start = time.monotonic()

            remaining = TIMEOUT - (
                time.monotonic() - timer_start
            )

            if remaining <= 0:
                for seq in range(base, next_seq):
                    s.sendto(packets[seq], address)

                timer_start = time.monotonic()
                continue

            s.settimeout(remaining)

            try:
                packet, _ = s.recvfrom(2048)
            except socket.timeout:
                for seq in range(base, next_seq):
                    s.sendto(packets[seq], address)

                timer_start = time.monotonic()
                continue

            ack_num = parse_ack(packet)

            if ack_num is not None and base < ack_num <= next_seq:
                base = ack_num
                timer_start = (
                    time.monotonic() if base < next_seq else None
                )

        end_seq = total_packets + 1
        end_packet = make_packet(END, end_seq)
        s.sendto(end_packet, address)

        deadline = time.monotonic() + TIMEOUT

        while True:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            s.settimeout(remaining)

            try:
                packet, _ = s.recvfrom(2048)
            except socket.timeout:
                break

            ack_num = parse_ack(packet)

            if ack_num == end_seq + 1:
                break

    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "receiver_ip",
        help="The IP address of the host that receiver is running on",
    )
    parser.add_argument(
        "receiver_port",
        type=int,
        help="The port number on which receiver is listening",
    )
    parser.add_argument(
        "window_size",
        type=int,
        help="Maximum number of outstanding packets",
    )

    args = parser.parse_args()

    sender(
        args.receiver_ip,
        args.receiver_port,
        args.window_size,
    )


if __name__ == "__main__":
    main()
