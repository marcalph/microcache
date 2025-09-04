"""Minimal Redis Protocol parser."""


class MessageParser:
    def __init__(self):
        self.buffer = ""

    def _parse(self):
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
