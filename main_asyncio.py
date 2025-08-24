import asyncio


async def handle_client(reader, writer):
    client_addr = writer.get_extra_info('peername')
    print(f"Client connected: {client_addr}")
    
    try:
        while True:
            data = await reader.read(512)
            if not data:
                break
            
            message = data.decode()
            
            if 'ping' in message.lower():
                writer.write(b"+PONG\r\n")
                await writer.drain()
                
    except Exception as e:
        print(f"Error handling client {client_addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {client_addr}")


async def start_server(host='localhost', port=6379):
    server = await asyncio.start_server(
        handle_client,
        host,
        port
    )
    
    addr = server.sockets[0].getsockname()
    print(f"Async Redis server listening on {addr[0]}:{addr[1]}")
    
    async with server:
        await server.serve_forever()


def main():
    asyncio.run(start_server())


if __name__ == "__main__":
    main()