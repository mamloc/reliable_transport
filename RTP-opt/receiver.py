
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
    packet_header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0,
    )

    packet = packet_header / payload
    packet_header.checksum = compute_checksum(packet)

    return bytes(packet_header / payload)


def parse_packet(raw_packet):
    if len(raw_packet) < HEADER_SIZE or len(raw_packet) > MAX_PACKET_SIZE:
        return None

    packet_header = PacketHeader(raw_packet[:HEADER_SIZE])

    if len(raw_packet) < HEADER_SIZE + packet_header.length:
        return None

    packet_payload = raw_packet[
        HEADER_SIZE:HEADER_SIZE + packet_header.length
    ]

    received_checksum = packet_header.checksum
    packet_header.checksum = 0

    if compute_checksum(packet_header / packet_payload) != received_checksum:
        return None

    return packet_header, packet_payload


def receiver(receiver_ip, receiver_port, window_size):
    recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_socket.bind((receiver_ip, receiver_port))

    next_expected_seq = 0
    received_buffer = {}
    connection_started = False

    try:
        while True:
            raw_packet, sender_address = recv_socket.recvfrom(2048)
            parsed_packet = parse_packet(raw_packet)

            if parsed_packet is None:
                continue

            packet_header, packet_payload = parsed_packet

            if packet_header.type == START:
                if packet_header.seq_num != 0:
                    continue

                connection_started = True
                next_expected_seq = 1
                received_buffer.clear()

                recv_socket.sendto(
                    make_packet(ACK, next_expected_seq),
                    sender_address,
                )

            elif not connection_started:
                continue

            elif packet_header.type == DATA:
                current_seq = packet_header.seq_num

                # Bo qua goi nam ngoai cua so nhan
                if current_seq >= next_expected_seq + window_size:
                    continue

                # Luu goi tin chua duoc ghi theo dung thu tu
                if current_seq >= next_expected_seq:
                    received_buffer.setdefault(current_seq, packet_payload)

                    while next_expected_seq in received_buffer:
                        ordered_data = received_buffer.pop(next_expected_seq)
                        sys.stdout.buffer.write(ordered_data)
                        sys.stdout.buffer.flush()
                        next_expected_seq += 1

                # Gui ACK rieng cho goi DATA vua nhan
                recv_socket.sendto(
                    make_packet(ACK, current_seq),
                    sender_address,
                )

            elif packet_header.type == END:
                if packet_header.seq_num == next_expected_seq:
                    recv_socket.sendto(
                        make_packet(ACK, next_expected_seq + 1),
                        sender_address,
                    )
                    break

    finally:
        recv_socket.close()


def main():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("receiver_ip")
    arg_parser.add_argument("receiver_port", type=int)
    arg_parser.add_argument("window_size", type=int)

    args = arg_parser.parse_args()

    receiver(
        args.receiver_ip,
        args.receiver_port,
        args.window_size,
    )


if __name__ == "__main__":
    main()
