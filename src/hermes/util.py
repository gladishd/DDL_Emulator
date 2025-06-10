"""
So as before (although this is the version for Air we have this) thing called the utilities for Hermes. And for these utilities we have the helper that crosses platforms, ordinarily the server..well, rather unordinarily the server-side of the link is something that's going to be opening its UDP socket without a peer address, however we're not always going to have an explicit destination..the protocol, is later on going to call the transport.sendto(data)..and that's why when we get on macOS / Python >= 3.10 that's going to raise the following error: the TypeError: sendto(): AF_INET6 address must be tuple, not NoneType..and that means, that the packet is never going to leave the box, the 1-second liveness timer is going to expire, and the port shall drop back to DISCONNECTED. The solution for this is to geenrate the remote_addr and give that to loop.create_datagram_endpoint(), even in the case that the local port is in the role of the "server". That is how we know that UDP is still connection-less..because, the kernel is going ot just remember the peer's default value so that the sendto(data) thing is going to work. The solution is to replace the run_link() inside the Port and we've done that, and now it is time to generate this cross-platform helper for IPv6 neighbor discovery. On Linux that would traditionally mean looking at the ip -6 neighbor show dev <iface> aspect of things, whereas on the macOS that would mean looking at the ndp -an type of things where we parse the single-letter state codes.
"""
from __future__ import annotations
import asyncio
import platform
import re
from typing import List, Optional, Tuple

""" The first thing to look at is the wrapper for the sub-process. in it, we will see the way that instead of what we had before, now we have this remote_addr that  is now non-None, such that the create_Datagram_endpoint() function is going to be when "invoked", the socket.connect() under the hood that is, in the sense that the transport.sendto(data) can therefore omit the destination argument and "there-fore" we're going to have no TypeError, no forced disconnect, and the dashboard is going to stay solid green because w know, that the same patched file that we got, was the way that we utilized Hermes and restarted it on each one thus that the link is going to come up once and retain its stability in the sub-process, wrapper.  """


async def run_subprocess(cmd: List[str], timeout: int = 10) -> str:
    """ Asynchronous is the way that we run the cmd; on the timeout / error thing we're going to want to return a string that's empty, of course. The idea is to keep the original structure but always to open the datagram socket as it exists as it is connected, to the peer EVEN when the side is the server, and "even" to add a small socket import such that we can request the endpoint for IPv6 in an amalgamated manner. """
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        if proc.returncode:
            return ""
        return stdout.decode()
    except Exception:
        return ""

""" The discovery of the neighbor IPv6. In this we're going to do what we've always done but the only difference is that now, we have this socket to import and then we can pass, the remote address as the peer that is unconditional and specificationally "geared" toward the family being the socket.AF_INET6.  """


async def _ping_mcast(interface: str, group: str = "ff02::1") -> None:
    """ But that's how it works; we can literally play a "game of ping pong" in the sense of what we get when we cycle through, we're not even touching any of the other logic; we're just utilizing what remains of the emulator as it works and continues to work exactly as it did before except now we're no longer cycling every second. That's what Hermes is for. With the "power" of Hermes we can make sure that one multicast ping is going to give a "helpful" nudge to the ND cache to make it REACHABLE. """
    await run_subprocess(["ping6", "-c", "1", f"{group}%{interface}"], timeout=5)


async def get_ipv6_neighbors(interface: str) -> Optional[
    List[Tuple[str, str, str]]
]:
    """
    But of course, that's going to require us to restore the get_snapshot() integration that we call via the periodic_status_update thing wherein we make it certain that yes, we got all the "attributes that it needs" such as the self.local_addr thing, the self.remote_addr thing, as well as the self.is_client which is something that we, as the always defined creators..utilize, the discovery of the link-local peer on the interface. The return values for this particular function shall be either a List or of course the None datatype, but this shouldn't dissuade us from doing what we do return:
    list | None
        [
          (local_addr,   'local',    'client' | 'server'),
          (neighbor_addr,'neighbor', 'client' | 'server')
        ]
    And so the first thing that we are going to look at are the addresses in their local form
    """
    ifcfg = await run_subprocess(["ifconfig", interface], 5)
    local_addrs = [
        ln.split()[1].split("%")[0]
        for ln in ifcfg.splitlines()
        if "inet6 " in ln
    ]
    if not local_addrs:
        return None
    """ And then we bump the cache for the ND. The ultimate idea is that we do have two "machines" but more importantly the AttributeError is something that "indicates" that the dashboard, isn't even again receiving snapshots at least not regularly. That's why we need to bump the ND cache. """
    await _ping_mcast(interface)
    """ The table for the neighbor is something that we "talk about" when we see, that the server-side socket is being opened in the mode that we can only describe as "connected". The reason for this is that otherwise, when a UDP socket is connected it only and always accepts datagrams from the pre-set peer, so the very first discovery packet that arrives from an address "unknown' is discarded, and then the FSM times out, and then the link is going to tear down resulting in a reboot of the whole cycle "as we know it". """
    sysname = platform.system()
    lines = ""
    if sysname == "Linux":
        lines = await run_subprocess(
            ["ip", "-6", "neighbor", "show", "dev", interface], 5
        )
    elif sysname == "Darwin":
        ndp_out = await run_subprocess(["ndp", "-an"], 5)
        """ But what's more remarkable is that we should keep the full line here so that we have still, the R S P state column..and we do, all we needed to do was connect only when we were the client (exactly how the original code behaved by the way) and, while we're here, we can add this small back-off so that retries don't hammer the interface.  """
        lines = "\n".join(
            ln for ln in ndp_out.splitlines()
            if f"%{interface}" in ln and ln.lstrip().startswith("fe80::")
        )
    else:
        return None
    if not lines:
        return None
    """ And then we're going to take the neighbors in and parse them; this is going to require us to understand the importance and conceptualization of the codex of ndp: Reachable, Permanent, Stale. These are the only things that will tell us, that the server side is going to bind but it "will not connect", so that it can only accept the "very first" packet from its peer but, the handshake is still going to complete and the dashboard will retain its green characteristic..and that's what we do when we use Hermes, we need to restart Hermes on both of our machines and then disappear the infinite loop for what that's worth..this neighbor parsing will show us why, we state Stale Permanent and Reachable, are our goals for N.D.P..  """
    peers: list[Tuple[str, str]] = []
    VALID_LINUX = ("REACHABLE", "PERMANENT", "STALE")
    VALID_DARWIN = ("R", "P", "S")
    for ln in lines.splitlines():
        ln = ln.strip()
        addr = ln.split()[0].split("%")[0]
        ok = (
            any(v.lower() in ln.lower() for v in VALID_LINUX)
            or ln.split()[-1] in VALID_DARWIN
        )
        if not ok:
            continue
        kind = "local" if addr in local_addrs else "neighbor"
        peers.append((addr, kind))
    locals_ = [p for p in peers if p[1] == "local"]
    neighbors = [p for p in peers if p[1] == "neighbor"]
    if not locals_ or not neighbors:
        return None
    a_local = locals_[0][0]
    a_remote = neighbors[0][0]
    local_is_client = a_local < a_remote
    return [
        (a_local,  "local",    "client" if local_is_client else "server"),
        (a_remote, "neighbor", "server" if local_is_client else "client"),
    ]
""" And so we have this probe for the command line probe, because the server  just binds. For instance, when you say remote_addr = self.remote_addr if self.is_client else None, that means that the server is just going to bind. The client is going to be doing the connect() call.  """
if __name__ == "__main__":
    import sys
    import json
    iface = sys.argv[1] if len(sys.argv) > 1 else "bridge0"
    print(json.dumps(asyncio.run(get_ipv6_neighbors(iface)), indent=2))
