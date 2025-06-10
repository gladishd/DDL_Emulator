"""
I think one thing to recognize is how the heartbeat watchdog is going to be far too aggressive for a real network + two busy event-loops. What we need instead is to design a port where we have one port and one thread and one, logical link-endpoint. If that's not enough of a transliteration I think that the mode we have really is going to be how we get the neighbor to be discovered with the IPv6 ND and how we get, the virtual mode to exist in the sense of the interface string being the host:local_port:remote_port and how we get, the UDP socket to be only connected when we are the client..whereas on the server-side we're only going to bind so that the very first packet that we discover is going to be accepted.
"""
from __future__ import annotations
import asyncio
import logging
import socket
import threading
from typing import Optional, Tuple, Type
from hermes.faults.FaultInjector import ThreadSafeFaultInjector
from hermes.model.ports import PortConfig, PortIO
from hermes.model.types import IPV6_ADDR
from hermes.port.protocol import LinkProtocol, EthernetProtocolExtended
from hermes.util import get_ipv6_neighbors

""" And then we have the scaffolding for the event-loop as well as the base of the thread; the thing is that when you have two _busy_ event-loops you can see, that the watchdog you get this thing where between, the two Macs you routinely get gaps >= 400-800 ms which indicates something like scheduler jitter, Thunderbolt/USB framing, Python's GIL, log i/O, etc. and so on and so forth such that whenever a gap exceeds the 375 ms mark then the watchdog fires, the socket is closed, the port instantly re-creates a new endpoint, the Agent sees DISCONNECTED -> CONNECTED, and we generate new tree -ids and the loop is going to start all over again. """


class BasePort(threading.Thread):
    def __init__(
        self,
        config: PortConfig,
        io: PortIO,
        faultInjector: Optional[ThreadSafeFaultInjector] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._loop = asyncio.new_event_loop()
        self.stop_event = asyncio.Event()
        self.logger = logging.getLogger(f"Port.{config.port_id}")
        self.config = config
        self.io = io
        self.faultInjector = faultInjector
        self.protocol_instance = None
        # Once the link is up we're ready to pouplate, we've got that after a few dozen
        # reconnects the old FDs and queues are going to be closed while the thread Agent, is
        # still going to be polling them which causes the final population.
        self.local_addr:  Optional[IPV6_ADDR] = None
        self.remote_addr: Optional[IPV6_ADDR] = None
        self.is_client:   bool = False

        self.daemon = True          # With the main primary process, the thread exits

    def run(self) -> None:
        self.logger.info("Starting UDP port thread for %s",
                         self.config.port_id)
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self.run_link())
        finally:
            self.logger.info(
                "Shutting down UDP port thread for %s", self.config.port_id)
            self._loop.close()

    async def run_link(self):
        raise NotImplementedError

    """ With 1 second for the beacon and 3 seconds for the grace, that makes us still quick enough to notice a real fault has occurred but still reminds us that we as people are immune, to ordinary jitter because, we have this thing that helps us it's the REST / Dashboard implementation of the snapshot. So we get a slightly cleaner snapshot of the configuration attached to this self. """

    def get_snapshot(self) -> dict:
        if self.protocol_instance is None:
            return {
                "name":  self.config.port_id,
                "link":  {"protocol": "EthernetProtocol",
                          "status":   "disconnected"},
            }

        return {
            "name":  self.config.port_id,
            "connection": {
                "local_address":    str(self.local_addr),
                "neighbor_address": str(self.remote_addr),
                "neighbor_portid":  getattr(self.protocol_instance,
                                            "neighbor_portid", None),
                "is_client":        self.is_client,
            },
            "link": {
                "protocol": "EthernetProtocol",
                "status":   self.protocol_instance.link_state.value,
            },
        }


""" The implementation of UDP encourages us to harden the shutdown path. Because even with sane timing you can still want to guard the PipeQueue.empty() against a closed FD, right? And be sure that Agent.run() is going to break after the main sim asks it to stop so that we can clean up the queue, something which we do once. """


class UDPPort(BasePort):
    def __init__(
        self,
        config: PortConfig,
        io: PortIO,
        faultInjector: Optional[ThreadSafeFaultInjector] = None,
        protocolClass: Optional[Type[LinkProtocol]] = None,
        **kwargs,
    ):
        super().__init__(config, io, faultInjector, **kwargs)

        self.port_id = config.port_id
        self.name = config.name
        self.protocolClass = protocolClass or EthernetProtocolExtended
        """ And so that's the situation, we have this thing where we have the 127.0.0.1:55555:55556 and that's our shorthand, the "virtual" edition which is what we got at Qooley and what made it so great, to discover what we can expect after the change. There's not going to any more need for connect/disconnect spam, not in the "Tree" View. """
        self.is_virtual = ":" in self.config.interface
        if self.is_virtual:
            host, lport, rport = self.config.interface.split(":")
            self.host = host
            self.local_port = int(lport)
            self.remote_port = int(rport)

    """ And that's what we get, we get that the TREE_BUILD / TREE_BUILD_ACK traffic is something that's going to settle down to the initial exchange only. And that's why the final TypeError disappears, because the Agent thread isn't hammering a defunct fd anymore. """

    async def wait_for_connection(self) -> Tuple[bool, IPV6_ADDR, IPV6_ADDR]:
        """ And then we're going to have to do that, give the tweak of the logs a block, until both of the peer & the local addresses are known at least to, we can if we want to see still what an occasional timeout looks like; of course we could always bump the grace period. """
        while True:
            if self.is_virtual:
                local = (self.host, self.local_port)
                peer = (self.host, self.remote_port)
                is_cli = self.local_port > self.remote_port
                return is_cli, peer, local

            neigh = await get_ipv6_neighbors(self.config.interface)
            if neigh:
                return self._extract_running_details(neigh)

            await asyncio.sleep(1)

    # -----------------------------------------------------------
    """ The grace period bump meaning, bring it to about 4-5x the beacon interval, and make sure of what we can see, which is what we see; in the torrent of log messages we see that the HEARTBEAT_INTERVAL is something that is dependent on having a datagram endpoint; you see long before the new 100 s heartbeat could ever matter, we have that the code path is something that's not the heartbeat watchdog but it is the hand-shaking / link-establishment timeout. And when we have that timeout we can look in the previous, LinkProtocol._connection_timeout() for ..the file which has in fact raised the log line for the Connection timed ou that occurs at line, probably about 128 in the LinkPtorotocl but here, the whole thing about if time.monotonic() - self._since_last_packet > self.INIT_TIMEOUT: then we can, know that the INIT_TIMEOUT (or the CONNECTION_TIMEOUT) is still going to be at the original tiny value it was once which is the 500 ms / 1 s...thing. """

    async def run_link(self) -> None:
        self.logger.info("%s -- Running link", self.port_id)

        while True:
            try:
                self.is_client, self.remote_addr, self.local_addr = \
                    await self.wait_for_connection()

                self.logger.info("is_client=%s, local=%s, peer=%s",
                                 self.is_client, self.local_addr, self.remote_addr)
                self.logger.info(
                    "%s -- Creating datagram endpoint", self.port_id)

                transport, self.protocol_instance = await self._loop.create_datagram_endpoint(
                    lambda: self.protocolClass(
                        io=self.io,
                        name=self.port_id,
                        sending_addr=self.remote_addr,
                        is_client=self.is_client,
                        faultInjector=self.faultInjector,
                    ),
                    local_addr=self.local_addr,
                    # And so forever it repeats, and connects only when the client is there.
                    remote_addr=self.remote_addr if self.is_client else None,
                    family=socket.AF_INET6,
                    reuse_port=True,
                )
                self.logger.info("%s -- Transport opened", self.port_id)
                """ And thus we block it until the protocol, is going to set the disconnected_future """
                await self.protocol_instance.disconnected_future
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.error("run_link(): %s", exc, exc_info=True)
                # The back-off is small but, now we know
                await asyncio.sleep(3)
            finally:
                if self.protocol_instance:
                    self.protocol_instance = None
                if "transport" in locals() and transport:
                    transport.close()

    """ Now we know that because the port thread immediately has restarted, the peer's tree-IDs no longer match ("...but with different instance id. Ignoring.")..rather, *each side thinks the other died, so they both tear the link down and try again--an infinite loop*. """

    def _extract_running_details(
        self, neigh_info
    ) -> Tuple[bool, IPV6_ADDR, IPV6_ADDR]:
        """
        And so what we chance is the thing where we change the initial link-establish timeout, where withal we have the LinkProtocol and the ChunkProtocol (for which we search for the _connection_timeout, and the INIT_TIMEOUT, or the CONNECTION_TIMEOUT or even the literal 0.5 thing which is how, we raise it). And when we do, we shall convert the output of the util.get_ipv6_neighbors() and that's our putout which lends itself to the (is_client, peer, local) trichotomy.
        """
        is_cli = False
        local = peer = None
        for addr, kind, role in neigh_info:
            full = f"{addr}%{self.config.interface}"
            tup = (full, 55555)
            if role == "client" and kind == "local":
                is_cli, local = True, tup
            elif role == "client" and kind == "neighbor":
                peer = tup
            elif role == "server" and kind == "local":
                local = tup
            elif role == "server" and kind == "neighbor":
                peer = tup
        return is_cli, peer, local
