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
    packet_header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0,
    )

    packet = packet_header / payload
    checksum_value = compute_checksum(packet)
    packet[PacketHeader].checksum = checksum_value

    return bytes(packet)


def parse_ack(received_packet):
    if len(received_packet) < HEADER_SIZE:
        return None

    packet_header = PacketHeader(received_packet[:HEADER_SIZE])
    packet_payload = received_packet[
        HEADER_SIZE:HEADER_SIZE + packet_header.length
    ]

    if packet_header.type != ACK or packet_header.length != 0:
        return None

    received_checksum = packet_header.checksum
    packet_header.checksum = 0

    if compute_checksum(packet_header / packet_payload) != received_checksum:
        return None

    return packet_header.seq_num


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    input_message = sys.stdin.buffer.read()
    data_chunks = [
        input_message[i:i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(input_message), MAX_PAYLOAD_SIZE)
    ]

    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket.settimeout(TIMEOUT)
    receiver_address = (receiver_ip, receiver_port)

    try:
        # Gui START va cho ACK
        send_socket.sendto(make_packet(START, 0), receiver_address)

        while True:
            try:
                received_packet, _ = send_socket.recvfrom(2048)
            except socket.timeout:
                raise TimeoutError("Timed out waiting for START ACK")

            if parse_ack(received_packet) == 1:
                break

        data_packets = {
            seq: make_packet(DATA, seq, chunk)
            for seq, chunk in enumerate(data_chunks, start=1)
        }

        packet_count = len(data_packets)
        window_base = 1
        next_seq_to_send = 1
        acked_packets = set()
        timer_started_at = None

        while window_base <= packet_count:

            # Gui cac goi moi trong cua so
            while (
                next_seq_to_send <= packet_count
                and next_seq_to_send < window_base + window_size
            ):
                send_socket.sendto(
                    data_packets[next_seq_to_send],
                    receiver_address,
                )

                if timer_started_at is None:
                    timer_started_at = time.monotonic()

                next_seq_to_send += 1

            if timer_started_at is None:
                timer_started_at = time.monotonic()

            time_left = TIMEOUT - (
                time.monotonic() - timer_started_at
            )

            if time_left <= 0:
                # Chi gui lai cac goi chua duoc ACK
                for seq in range(window_base, next_seq_to_send):
                    if seq not in acked_packets:
                        send_socket.sendto(
                            data_packets[seq],
                            receiver_address,
                        )

                timer_started_at = time.monotonic()
                continue

            send_socket.settimeout(time_left)

            try:
                received_packet, _ = send_socket.recvfrom(2048)
            except socket.timeout:
                for seq in range(window_base, next_seq_to_send):
                    if seq not in acked_packets:
                        send_socket.sendto(
                            data_packets[seq],
                            receiver_address,
                        )

                timer_started_at = time.monotonic()
                continue

            ack_seq_num = parse_ack(received_packet)

            # ACK rieng le: danh dau goi duoc xac nhan
            if (
                ack_seq_num is not None
                and window_base <= ack_seq_num < next_seq_to_send
            ):
                acked_packets.add(ack_seq_num)

                previous_base = window_base

                # Chi dich cua so qua cac goi da duoc ACK
                while window_base in acked_packets:
                    window_base += 1

                # Cua so tien len: dat lai timer
                if window_base > previous_base:
                    timer_started_at = (
                        time.monotonic()
                        if window_base < next_seq_to_send
                        else None
                    )

        # Gui END
        end_seq = packet_count + 1
        end_packet = make_packet(END, end_seq)
        send_socket.sendto(end_packet, receiver_address)

        ack_deadline = time.monotonic() + TIMEOUT

        while True:
            time_left = ack_deadline - time.monotonic()

            if time_left <= 0:
                break

            send_socket.settimeout(time_left)

            try:
                received_packet, _ = send_socket.recvfrom(2048)
            except socket.timeout:
                break

            if parse_ack(received_packet) == end_seq + 1:
                break

    finally:
        send_socket.close()


def main():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "receiver_ip",
        help="The IP address of the host that receiver is running on",
    )
    arg_parser.add_argument(
        "receiver_port",
        type=int,
        help="The port number on which receiver is listening",
    )
    arg_parser.add_argument(
        "window_size",
        type=int,
        help="Maximum number of outstanding packets",
    )

    args = arg_parser.parse_args()

    sender(
        args.receiver_ip,
        args.receiver_port,
        args.window_size,
    )


if __name__ == "__main__":
    main()
