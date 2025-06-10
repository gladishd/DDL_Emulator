"""
server.py

UDP server that listens for external connections on 0.0.0.0:6363 and handles up to 5 concurrent clients.
Implements a JSON messaging protocol for client requests and server state updates.
"""

import asyncio
from dataclasses import asdict, is_dataclass
import json
import threading
from websockets.asyncio.server import serve
import json
import logging
import queue
import websockets

class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if is_dataclass(obj):
            return asdict(obj)
        return super().default(obj)

class WebSocketServer:
    def __init__(
        self,
        host: str="0.0.0.0",
        port: int=6363,
        max_connections: int=5,
        command_queue: queue.Queue=None,
    ):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.active_connections = set()
        self.logger = logging.getLogger('WebSocketServer')
        self.command_queue = command_queue or queue.Queue()
        self.loop = None # Event loop for the server
    
    async def handle_client(self, websocket):
        """Handle incoming client connections"""
        client_id = None
        try:
            client_id = f"client_{len(self.active_connections)}"
            self.active_connections.add((client_id, websocket))
            self.logger.info(f"New client connected: {websocket.remote_address}")

            async for message in websocket:
                try:
                    data = json.loads(message)
                    self.logger.debug(f"Received message from {client_id}: {data}")

                    await self.command_queue.put({
                        "port": data["port"],
                        "action": data["action"]
                    })

                    await websocket.send(json.dumps({"status": "message received"}))
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid JSON message from {client_id}: {message}")
                except websockets.exceptions.ConnectionClosedError:
                    self.logger.info(f"Connection closed by client {client_id}")
                    break
                except Exception as e:
                    self.logger.error(f"Error processing message from {client_id}: {e}")

        except websockets.exceptions.ConnectionClosedError:
            self.logger.info(f"Connection closed by client during handshake")
        except Exception as e:
            self.logger.error(f"Unexpected error in handle_client: {e}")
        finally:
            if client_id:
                try:
                    self.active_connections.remove((client_id, websocket))
                    self.logger.info(f"Client {client_id} disconnected")
                except KeyError:
                    pass  # Connection might have already been removed

    async def start_server(self):
        self.logger.info(f"Starting WebSocket server on {self.host}:{self.port}")
        
        try:
            # Don't use a context manager - directly create the server
            self.server = await serve(
                self.handle_client, 
                self.host, 
                self.port,
                ping_interval=None,  # Disable ping to simplify
                ping_timeout=None    # Disable ping timeout
            )
            
            self.logger.info(f"WebSocket server listening on {self.host}:{self.port}")
            
            # Keep server running indefinitely
            await asyncio.Future()
        except Exception as e:
            self.logger.error(f"WebSocket server error: {e}")

    
    async def send_updates(self, snapshots, agent_snapshot):
        """Send updates to all connected clients"""
        message = json.dumps({
            "type": "update",
            "node_id": agent_snapshot['node_id'],
            "snapshots": snapshots,
            "trees_dict": agent_snapshot['trees'],
            'port_paths': agent_snapshot['port_paths']
        }, cls=DataclassJSONEncoder)
        dead_connections = set()

        for client_id, connection in self.active_connections.copy():
            try:
                await connection.send(message)
            except websockets.exceptions.ConnectionClosed:
                self.logger.info(f"Removing dead connection for client {client_id}")
                dead_connections.add((client_id, connection))
            except Exception as e:
                self.logger.error(f"Error sending update to client {client_id}: {e}")
                dead_connections.add((client_id, connection))

        # Clean up dead connections
        for dead_connection in dead_connections:
            try:
                self.active_connections.remove(dead_connection)
            except KeyError:
                pass



if __name__ == "__main__":
    import signal
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s',
        handlers=[logging.StreamHandler()]
    )
    """ I don't know if this 0.0.0.0 is necessary over say 127.0.0.1 because the idea is to launch on the port..the ports en2 and en3 and en4..on the Mac mini that all bind to
    fe80::c99:…%bridge0  port 55555   (local)
    fe80::439:…%bridge0  port 55555   (remote)
    and the Air has one peer port (sahas:en3) that uses exactly the same pair. Because every Hermes socket is set up with reuse_port = True, macOS lets all three share the same UDP 55555. The first few frames get fanned out to an essentially random receiver, so only one side of the pair completes the hand-shake..the others wait, time out, and drop. When they reopen, the race starts again -> flicker. But that's where we step in beause we have all these topical articles but as you know, topical stuff doesn't work; you have got, to be able to run the Ethernet ports locally such that if you leave "en2" and "en3" running locally, they steal enough traffic from "en4" that the link never stays stable for more than ~1 s.)...
"""
    server = WebSocketServer(
        "0.0.0.0", 6363,
        logger=logging.getLogger("WebSocketServer"),
        command_queue=queue.Queue(),
        message_event=threading.Event()
    )
    asyncio.run(server.start_server())
    signal.pause()
