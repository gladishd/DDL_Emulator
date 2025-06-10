"""
And so we start out in this port with one logical link-endpoint..then we can progress to having one port and then one thread.
The goal of course is to look at what are the discoveries that we can make via the UDP socket and these are :
* "real" mode -> neighbor discovered with IPv6 ND
* "virtual" mode -> intefface string    host:local_port:remote_port
And we know, that a UDP socket is ONLY CONNECTED WHEN WE ARE THE CLIENT; on the server side we bind-only so the very first discovery packet is accepted.
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

""" And so that's the discovery loop that we've got. Typically we do that cycle and the cycle repeats and then we flicker. That's how I experienced it anyway, but that's just a testing characteristic of the test that we achieve when we know why it was timing out in the sense that..we launched three ports (en2, en3, en4) on the mac Mini and all of those ports bind to the following:
fe80::c99:…%bridge0  port 55555   (local)
fe80::439:…%bridge0  port 55555   (remote)
And ultimately what we get is that we have that the Air, has one peer port that is located at sahas:en3 and that is the port that uses exactly the same identical type of pair..because we know that every Hermes socket is set up with `reuse_port=True`, whereas alternatively we're going to have that macOS lets all three, share the same UDP 55555..the 'ultimate' thing that we have is that there's always the "other side" of the receiver; the first few frames get "fanned out" to an essentially random receiver, so only one side of the pair completes the hand-shake; the others wait, time out, and drop. So what that means is that when they reopen, the race is going to start once more and then we get a flicker, or what I call a flicker..supposedly if you leave en2 and en3 running locally, they're going to have the capacity, to steal enough traffic from en4 that the link never stays stable for more then ~1 s.
So of course if we're doing a quick demo / debugging then we know that we can disable the extra ports such that only the matching pair is going to be active..we do that via the YAML scripts..and when we do the ports..rename the ports or set them to be disconnected. Restart Hermes on both machines - the links then stay solid green. And, we can run several logical ports as well per host--we're going to want to give each port its own (local, remote) UDP pair so packets don't collide..for example we're going to do something quite similar except we'd specify the ports in the YAML..the Air would use the complementary mapping (bridge0:55556:55555, ...) while the original, the interface would be for bridge0:55555:55556. And that is how we achieve our base thread as well as the scaffolding for the event-loop.
 """
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

        """ Once the link is up, we're going to populate it and that is why we need the self.daemon ..specifically, so that we can see to it that the thread exits with the main process. """
        self.local_addr:  Optional[IPV6_ADDR] = None
        self.remote_addr: Optional[IPV6_ADDR] = None
        self.is_client:   bool = False

        self.daemon = True

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

    """ Alternately, we can set up this helper in REST / Dashboard..we could of course relax the timeout in the sense of..having multiple simultaneous links on the same multicast-style addressing, which would necessitate increasing the window Hermes waits before giving up..but then again, sorting the socket-fanout first is usually the cleaner path. Ultimately whether or not we decide, to look into the ChunkProtocol or the EthernetProtocolExtended and then set the CONNECT_TIMEOUT_MS = 3000 to be true, whereas prior to now it was 1000..we can do that, but that's just delaying the inevitable..we could go through the checklist and then make sure that we had restarted Hermes on both machines and that we had watched, the logs in that we know, that we should no longer see Connection timed out. The dashboard demonstrates a green connection when the active ports are continuously, not dropping after isolating each socket--the protocol timers are one thing but nine times out of ten the duplicate-socket race is the reason that, we need not to revise our dashboard but to revise the behavior of the "real" backend. """

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


""" And thus we have the implementation of UDP..it turns out that for these active ports..if the link still drops after isolating each socket--then we know that the protocol timers are the key of course..the thing to remember for this whole "tutorial" thing is that `bridge0` is an interface name, not a host-name or IP address, so the DNS resolver chokes on it in the sense of, nodename nor servname provided ..and then, for a virtual & same-machine link which we will get to on the production server we have got to give a real host / IP address followed, by two UDP ports..and that's what we need, for the working development configuration it typically looks like this: <host-or-ip>:<local-port>:<remote_port> and that, will yield what we need which is the working development configuration that keeps every link on the loopback interface that is called with the appellation of 127.0.0.1. On the peer Mac of course were it so easy we would simply flip the two ports' numbers in each tuple. """
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
        """ And that is the birthplace of our configuration..because for the field host as well as the field local_port as well as the field remote_port, we know that the meaning on this specific host is that the host indicates where to send packets, while the local port indicates the port that we bind to, and the remote port indicates the port that we send to..these in particular order must be the resolvable IP or hostname (e.g. the 127.0.0.1 and or the other Mac's LAN IP)..and in that sense we know that the port we bind to must be unique per logical link on this host while the port we send to must be the partner's local port. That leads us to the following, virtual shorthand of the value that we get when we get 127.0.0.1:55555:55556."""
        self.is_virtual = ":" in self.config.interface
        if self.is_virtual:
            host, lport, rport = self.config.interface.split(":")
            self.host = host
            self.local_port = int(lport)
            self.remote_port = int(rport)
    """ And that's what we have, before we wait for the connection. It's essential to recognize the parallels between the Port as well as the configuration, in YAML, that we utilize and we utilize it, when we have the port to which we send data..that's our remote port..and that's the partner's local port that we send it to. """

    async def wait_for_connection(self) -> Tuple[bool, IPV6_ADDR, IPV6_ADDR]:
        """ And so we keep every (local, remote) pair unique across the two machines and we can spin up, as many logical links as we'd like..that is recognizing that it's not just the number of links but it's also the "speed" at which we travel amongst them in the sense of, keeping incrementing the port numbers..and, then we can restart Hermes on both Macs knowing that yes, each port thread should now bind cleanly and the dashboard itself should have connections that are green..and to which we block, until both the peer & local addresses are being known.
        """
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
    """ And that's what we're doing, we want to disable what are the extra ports so that only the matching pair is active. In the process of doing that, we have a lot of debugging and we know that we can temporarily dis-able these things, disable the ports with the appellation of disconnected type-wise, and then when we look at that we know that we can understand, what is the real interface which we would like to test.. """

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
                    # client is the only one the precondition to connect(), otherwise
                    remote_addr=self.remote_addr if self.is_client else None,
                    family=socket.AF_INET6,
                    reuse_port=True,
                )
                self.logger.info("%s -- Transport opened", self.port_id)
                # Otherwise, we're going to have the server side of the link opening its UDP socket..so we block, until the protocol sets the disconnected_future
                await self.protocol_instance.disconnected_future
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.error("run_link(): %s", exc, exc_info=True)
                await asyncio.sleep(3)          # back off in a small way
            finally:
                if self.protocol_instance:
                    self.protocol_instance = None
                if "transport" in locals() and transport:
                    transport.close()

    """ Of course it's important to recognize that the lack of a peer address means that when the protocol later calls the transport.sendto(data) without having a destination explicitly, then that's when on macOS / Python >= 3.10 that raises the following exception--this is the TypeError: sendto(): AF_INET6 adress must be tuple, not NoneType. This simply indicates that the packet never leaves the box, the 1-econd liveness timer expires and..finally, the port drops back to DISCONNECTED. The fix for this is obviously to simply just always give the remote_addr to the loop.create_datagram_endpoint() even when, the local port is in the role of "server". Of course it's important to remember the connection-less nature of UDP; the kernel simply has got to remember, the default peer such that the sendto(data) invocation works. """

    def _extract_running_details(
        self, neigh_info
    ) -> Tuple[bool, IPV6_ADDR, IPV6_ADDR]:
        """
        And so inside the Port, we have this link that we run and replace..in it we have the UDPPort.run_link command which tells us what to do which is to open, the IPv6 datagram endpoint and then do some handling for the reconnects. Thus we have that, and we do that run_link but here, it's a little difference. We're not just discovering the neighbors, we're converting the util.get_ipv6_neighbors() output into the following tuple -- (is_client, peer, local)
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
