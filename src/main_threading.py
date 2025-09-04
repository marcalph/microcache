import socket
from concurrent.futures import ThreadPoolExecutor
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class MessageParser:
    def __init__(self, client_socket):
        self.client_socket = client_socket
        self.buffer = ""

    def __call__(self, data):
        self.buffer += data

        # Parse RESP protocol
        if self.buffer.startswith("*"):
            try:
                command_parts = self._parse_resp()
                if command_parts:
                    self._handle_command(command_parts)
                    self.buffer = ""  # Clear buffer after successful parse
            except (ValueError, IndexError):
                # Incomplete message, wait for more
                pass
        else:
            # fallback to simple text protocol
            if "\n" in self.buffer:
                line = self.buffer.split("\n")[0].strip()
                self.buffer = ""
                parts = line.split()
                if parts:
                    self._handle_command(parts)

    def _parse_resp(self):
        lines = self.buffer.split("\r\n")
        if not lines[0].startswith("*"):
            return None

        num_args = int(lines[0][1:])
        args = []
        line_idx = 1

        for _ in range(num_args):
            if line_idx >= len(lines):
                return None  # Incomplete message

            if lines[line_idx].startswith("$"):
                # Skip the length line, just get the string content
                line_idx += 1
                if line_idx >= len(lines):
                    return None
                args.append(lines[line_idx])
                line_idx += 1
            else:
                args.append(lines[line_idx])
                line_idx += 1

        return args

    def _handle_command(self, parts):
        if not parts:
            return

        command = parts[0].lower()
        match command:
            case "echo":
                self._echo(parts[1:])
            case "ping":
                self._ping()
            case _:
                logger.info(f"Unknown command: {command}")

    def _echo(self, data):
        if data:
            # For multiple arguments, join them with space
            response = " ".join(data)
            # Return as RESP bulk string
            resp_response = f"${len(response)}\r\n{response}\r\n"
            self.client_socket.sendall(resp_response.encode())
            logger.info(f"Sent back: {data!r} as {resp_response}")

        else:
            # Empty bulk string
            self.client_socket.sendall(b"$-1\r\n")

    def _ping(self):
        # Simple string response for PING
        self.client_socket.sendall(b"+PONG\r\n")


def handle_client(client_socket, client_address):
    parser = MessageParser(client_socket)
    try:
        while True:
            request = client_socket.recv(512)
            if not request:
                break

            data = request.decode()
            logger.info(f"Received: {data!r} from {client_address}")
            parser(data)

    except Exception as e:
        logger.error(f"Error handling client {client_address}: {e}")
    finally:
        client_socket.close()
        logger.info(f"Client disconnected: {client_address}")


def main():
    server_socket = socket.create_server(("127.0.0.1", 6379), reuse_port=True)
    logger.info("Server listening on localhost:6379")

    with ThreadPoolExecutor(max_workers=10) as executor:
        try:
            while True:
                client_socket, client_address = server_socket.accept()
                logger.info(f"Client connected: {client_address}")
                executor.submit(handle_client, client_socket, client_address)

        except KeyboardInterrupt:
            logger.info("Shutting down server...")
        finally:
            server_socket.close()


if __name__ == "__main__":
    main()
