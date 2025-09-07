"""RESP parser and command handler."""

from typing import Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)


class RESPParser:
    """
    Redis Serialization Protocol (RESP) parser.
    Handles both RESP protocol and simple text protocol.
    """

    def __init__(self):
        self.buffer = bytearray()

    def feed(self, data: bytes) -> List[List[str]]:
        """
        Feed raw bytes into the parser and return a list of parsed commands.
        Returns empty list if no complete commands are available yet.
        """
        self.buffer.extend(data)
        commands = []

        while True:
            result = self._try_parse()
            if result is None:
                break
            commands.append(result)

        return commands

    def _try_parse(self) -> Optional[List[str]]:
        """Try to parse a single command from the buffer."""
        if not self.buffer:
            return None

        # Try RESP protocol first
        if self.buffer[0] == ord("*"):
            return self._parse_resp_array()

        # Fallback to simple text protocol (newline-delimited)
        return self._parse_text_protocol()

    def _parse_resp_array(self) -> Optional[List[str]]:
        """Parse a RESP array (command with arguments)."""
        # Find first CRLF
        try:
            first_crlf = self.buffer.index(b"\r\n")
        except ValueError:
            return None  # Incomplete message

        # Parse array length
        try:
            array_length = int(self.buffer[1:first_crlf])
        except (ValueError, IndexError):
            # Malformed array declaration
            self._consume_bytes(first_crlf + 2)
            return None

        position = first_crlf + 2
        elements = []

        for _ in range(array_length):
            element, new_position = self._parse_bulk_string(position)
            if element is None:
                return None  # Incomplete message
            elements.append(element)
            position = new_position

        # Successfully parsed complete array
        self._consume_bytes(position)
        return elements

    def _parse_bulk_string(self, start_pos: int) -> Tuple[Optional[str], int]:
        """Parse a RESP bulk string."""
        if start_pos >= len(self.buffer):
            return None, start_pos

        # Must start with $
        if self.buffer[start_pos] != ord("$"):
            return None, start_pos

        # Find the CRLF after the length declaration
        try:
            length_end = self.buffer.index(b"\r\n", start_pos)
        except ValueError:
            return None, start_pos

        # Parse the length
        try:
            length = int(self.buffer[start_pos + 1 : length_end])
        except ValueError:
            return None, start_pos

        if length == -1:
            # Null bulk string
            return "", length_end + 2

        # Calculate where the string content should be
        content_start = length_end + 2
        content_end = content_start + length

        if content_end + 2 > len(self.buffer):
            return None, start_pos  # Incomplete message

        # Extract the string content
        content = self.buffer[content_start:content_end].decode(
            "utf-8", errors="replace"
        )

        # Verify CRLF after content
        if self.buffer[content_end : content_end + 2] != b"\r\n":
            return None, start_pos  # Malformed message

        return content, content_end + 2

    def _parse_text_protocol(self) -> Optional[List[str]]:
        """Parse simple text protocol (space-separated, newline-terminated)."""
        # Look for newline
        for delimiter in (b"\r\n", b"\n"):
            try:
                end = self.buffer.index(delimiter)
                line = self.buffer[:end].decode("utf-8", errors="replace").strip()
                self._consume_bytes(end + len(delimiter))

                if line:
                    return line.split()
                return None
            except ValueError:
                continue

        return None  # No complete line yet

    def _consume_bytes(self, count: int):
        """Remove the first `count` bytes from the buffer."""
        self.buffer = self.buffer[count:]

    def clear(self):
        """Clear the internal buffer."""
        self.buffer.clear()


class RESPEncoder:
    """Encoder for RESP protocol responses."""

    @staticmethod
    def encode_simple_string(s: str) -> bytes:
        """Encode a simple string response."""
        return f"+{s}\r\n".encode("utf-8")

    @staticmethod
    def encode_error(msg: str) -> bytes:
        """Encode an error response."""
        return f"-ERR {msg}\r\n".encode("utf-8")

    @staticmethod
    def encode_bulk_string(s: Optional[str]) -> bytes:
        """Encode a bulk string response."""
        if s is None:
            return b"$-1\r\n"
        encoded = s.encode("utf-8")
        return f"${len(encoded)}\r\n".encode("utf-8") + encoded + b"\r\n"


class CommandHandler:
    """Handles parsed Redis commands (ECHO and PING only for now)."""

    async def handle_command(self, command: List[str]) -> bytes:
        """
        Process a command and return the RESP-encoded response.
        """
        if not command:
            return RESPEncoder.encode_error("empty command")

        cmd_name = command[0].upper()
        args = command[1:] if len(command) > 1 else []

        if cmd_name == "PING":
            return await self._handle_ping(args)
        elif cmd_name == "ECHO":
            return await self._handle_echo(args)
        else:
            logger.info(f"Unknown command: {cmd_name}")
            return RESPEncoder.encode_error(f"unknown command '{cmd_name}'")

    async def _handle_ping(self, args: List[str]) -> bytes:
        """Handle PING command."""
        if args:
            # PING with message returns the message
            return RESPEncoder.encode_bulk_string(args[0])
        # PING without args returns PONG
        return RESPEncoder.encode_simple_string("PONG")

    async def _handle_echo(self, args: List[str]) -> bytes:
        """Handle ECHO command."""
        if not args:
            # Empty bulk string for no arguments
            return RESPEncoder.encode_bulk_string(None)
        # Join all arguments with space
        return RESPEncoder.encode_bulk_string(" ".join(args))
