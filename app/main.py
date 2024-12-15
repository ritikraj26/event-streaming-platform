import socket
import threading
from dataclasses import dataclass
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key = int.from_bytes(data[4:6], byteorder="big"),
            api_version = int.from_bytes(data[6:8], byteorder="big"),
            correlation_id = int.from_bytes(data[8:12], byteorder="big")
        )

def create_message(request: KafkaRequest):
    response_header = request.correlation_id.to_bytes(4, byteorder="big")
    valid_api_version = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_version
        else ErrorCode.UNSUPPORTED_VERSION
    )
    response_header += error_code.value.to_bytes(2, byteorder="big")

    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    response_body = (
        int(2).to_bytes(1)
        + request.api_key.to_bytes(2)
        + min_version.to_bytes(2)
        + max_version.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4)
        + tag_buffer
    )

    response_length = len(response_header) + len(response_body)
    response = response_length.to_bytes(4, byteorder="big") + response_header + response_body
    return response


def handle_client(client):
    try:
        while True:
            request = KafkaRequest.from_client(client)
            if not request:
                break
            client.sendall(create_message(request))
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client.close()


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    # server.accept()
    while True:
        client, _ = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client,))
        client_thread.start()


if __name__ == "__main__":
    main()
