import asyncio
from typing import Optional

from transitions import Machine

from hermes.faults.FaultInjector import ThreadSafeFaultInjector
from hermes.model.ports import PortIO
from hermes.model.types import IPV6_ADDR
from hermes.port.protocol import LinkProtocol

class SliceStateMachine(Machine):
    """
    State machine for the SliceProtocol that manages the alternating read/write cycle.
    
    States:
    - RD (Read): Ready to read data from write_q and send it
    - RA (Read Acknowledge): Waiting for ACK after sending data
    - WT (Write): Ready to receive data and write it to read_q
    
    Transitions:
    - RD -> RA: After reading and sending data
    - RA -> WT: After receiving ACK
    - WT -> RD: After writing data and sending ACK
    
    The client starts in RD state, server starts in WT state to begin the cycle.
    """
    def __init__(self, is_client: bool):
        states = ["RD", "RA", "WT", "L"]  # Read, Read Acknowledge, Write
        transitions = [
            # When data is read and sent, move to waiting for ACK
            {'trigger': 'read_complete', 'source': 'RD', 'dest': 'RA'},
            # When ACK is received, move to waiting for data
            {'trigger': 'ack_received', 'source': 'RA', 'dest': 'WT'},
            # When data is written and ACK sent, move back to reading
            {'trigger': 'write_complete', 'source': 'WT', 'dest': 'RD'}
        ]
        super().__init__(
            model=self,
            states=states,
            transitions=transitions,
            initial="RD" if is_client else "WT"  # Client starts reading, server starts writing
        )


class ChunkProtocol(LinkProtocol):
    """
    A protocol that implements an alternating read/write cycle between two endpoints.
    
    Protocol Flow:
    1. Client (RD) reads from write_q and sends data
    2. Server (WT) receives data, writes to read_q, sends ACK
    3. Client (RA) receives ACK, transitions to WT
    4. Server (RD) reads from write_q and sends data
    5. Client (WT) receives data, writes to read_q, sends ACK
    6. Server (RA) receives ACK, transitions to WT
    7. Cycle repeats...
    
    Packet Types:
    - DATA:payload: Contains the actual data to be transmitted
    - ACK: Acknowledgment that data was received and processed
    """
    def __init__(self,
                 io: PortIO,
                 name: str,
                 sending_addr: IPV6_ADDR,
                 is_client: bool,
                 faultInjector: Optional[ThreadSafeFaultInjector]=None):
        super().__init__(io, name, sending_addr, is_client, faultInjector)
        self._send_task: Optional[asyncio.Task] = None
        self.state_machine = SliceStateMachine(is_client)
        self.current_data = None  # Store the current data being transmitted

    def on_connected(self):
        """Start the send task when connection is established"""
        self.logger.info("Executing onConnected function")
        self._send_task = asyncio.get_event_loop().create_task(self._process_send())

    def on_disconnected(self):
        """Clean up tasks when connection is lost"""
        if self._send_task:
            self._send_task.cancel()

    def _handle_normal_packet(self, data, addr):
        """
        Handle incoming packets based on current state.
        
        Packet handling rules:
        1. In WT state: Can receive DATA packets, write to read_q, send ACK
        2. In RA state: Can receive ACK packets, transition to WT, and send ACK
        3. In RD state: Can receive LIVENESS packets, respond with LIVENESS
        """
        if data.startswith(b"DATA:"):
            # Received data packet
            if self.state_machine.state == "WT":
                # Extract the actual data after "DATA:" prefix
                received_data = data[5:]
                self.io.read_q.put(received_data)
                # Send ACK and transition to RD state
                self.transport.sendto(b"ACK", addr)
                self.state_machine.write_complete()
                self.logger.info(f"Received data, sent ACK, transitioning to RD state")
        elif data == b"ACK":
            # Received ACK
            if self.state_machine.state == "RA":
                self.state_machine.ack_received()
                self.logger.info("Received ACK, transitioning to WT state")
                # Clear the current data as it was successfully transmitted
                self.current_data = None
                # Send another ACK to trigger the other side's read state
                if self.is_client:
                    self.transport.sendto(b"ACK")
                else:
                    self.transport.sendto(b"ACK", self.sending_addr)
                self.logger.info("Sent ACK to trigger other side's read state")
        elif data == b"LIVENESS":
            # Received LIVENESS packet
            if self.state_machine.state == "RD":
                # If we're in RD state and receive LIVENESS, check our queue
                if not self.io.write_q.empty():
                    # If we have data, send it
                    self.current_data = self.io.write_q.get()
                    if self.is_client:
                        self.transport.sendto(b"DATA:" + self.current_data)
                    else:
                        self.transport.sendto(b"DATA:" + self.current_data, self.sending_addr)
                    self.state_machine.read_complete()
                    self.logger.info("Received LIVENESS, had data to send, transitioning to RA state")
                else:
                    # If no data, respond with LIVENESS
                    if self.is_client:
                        self.transport.sendto(b"LIVENESS")
                    else:
                        self.transport.sendto(b"LIVENESS", self.sending_addr)
                    self.logger.info("Received LIVENESS, no data to send, responded with LIVENESS")

    async def _process_send(self):
        """
        Now, here's where things start getting..a bit more alternating in the bits of the slice protocol..when we close down the extra ports, and that's what it really comes down to..we could always offer to restart Hermes on both machines--and that is how the link should stay solid green..and we know that if we're running several logcical ports per host, then we can give each port its own (local, remote) UDP pair so packets don't collide.for example the Air would use the complementary mapping (bridge0:55556:55555, ...). If we're in RD state and have data -> then we can send the DATA and move to RA..but it doesn't take much data..in fact if we're in RD state and no data -> then we can send LIVENESS only as a keep-alive
        """
        self.logger.info("Process send started")
        try:
            while True:
                if self.state_machine.state == "RD":
                    if not self.io.write_q.empty():                # and so we know that the data has to be real
                        self.current_data = self.io.write_q.get()
                        if self.is_client:
                            self.transport.sendto(b"DATA:" + self.current_data)
                        else:
                            self.transport.sendto(
                                b"DATA:" + self.current_data, self.sending_addr
                            )
                        self.state_machine.read_complete()
                        self.logger.info("Sent DATA, ⇢ RA")
                    # furthermore..we should keep the libeness ..liveness attribute alive.
                    else:
                        if self.is_client:
                            self.transport.sendto(b"LIVENESS")
                        else:
                            self.transport.sendto(
                                b"LIVENESS", self.sending_addr)
                        self.logger.debug("Sent LIVENESS")
                        # And then don't call the read_complete()..the goal is to stay in RD
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.logger.info("Process send cancelled")
        except Exception as e:
            self.logger.error(f"Error in _process_send: {e}", exc_info=True)


    def get_link_status(self):
        """Return current protocol status including state machine state"""
        return {
            "protocol": "SliceProtocol",
            "status": self.link_state.value,
            "state": self.state_machine.state
        }