import asyncio
import logging
import sys
from message_parser import RESPParser, CommandHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def handle_client(reader, writer):
    client_addr = writer.get_extra_info("peername")
    logger.info(f"Client connected: {client_addr}")

    parser = RESPParser()
    handler = CommandHandler()

    try:
        while True:
            data = await reader.read(512)
            if not data:
                break

            logger.info(f"Received: {data!r} from {client_addr}")

            # Parse commands from the received data
            commands = parser.feed(data)

            # Process each command
            for command in commands:
                logger.info(f"Processing command: {command}")
                response = await handler.handle_command(command)
                writer.write(response)
                await writer.drain()
                logger.info(f"Sent response: {response!r}")

    except Exception as e:
        logger.error(f"Error handling client {client_addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info(f"Client disconnected: {client_addr}")


async def start_server(host="localhost", port=6379):
    server = await asyncio.start_server(handle_client, host, port)

    addr = server.sockets[0].getsockname()
    logger.info(f"Async Redis server listening on {addr[0]}:{addr[1]}")

    async with server:
        await server.serve_forever()


def main():
    asyncio.run(start_server())


if __name__ == "__main__":
    main()
