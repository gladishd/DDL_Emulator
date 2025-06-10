"""
After a long..hiatus, it is evident that we have encountered some "race condition in the frontend" which is ..because my network link to MacBook Pro while functional does show some minor instability (the 1.8% package/packet loss during the earlier `ping` test)...so what we've got is the frontend's, connection code has a very short, 2-second timeout. The reason being that "probably" the WebSocket connection to MacBook Pro at 169.254.1.1 is taking just slightly longer than 2 seconds to establish, causing the frontend to give up rather than "trying again"..prematurely and then incorrectly label it as `Disconnected`. The solution of course is to increase, the timeout of the frontend connection. The end result of this being that with Hermes, we have this helper that exists cross-platform and which makes it possible not just to do 4 but to do the whole IPv6 thing, the neighbor discovery which lends itself to the ..way that we can parse the codes..Linux being `ip -6 neighbor show dev <iface>` and macOS being `ndp -an` which parses the single-letter state codes..
"""
from __future__ import annotations
import asyncio
import platform
import re
from typing import List, Optional, Tuple
"""
But the timeout will be set to 2000 milliseconds. For now, we have the sub-process wrapper which is going to define an asynchronous..function, the "goal" being to allow the server to listen correctly, the network path to be verified, as well as the connection of the client to be made "more resilient" to ..the protocols of the handshake, what we finally see is that the interface statuses, change to Connected.
"""


async def run_subprocess(cmd: List[str], timeout: int = 10) -> str:
    """What we have here is to run *cmd* asynchronously; on error / timeout we're "going" to return an empty string.
    """
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

""" The easiest thing to do is, all Hermes log level (s) aside, what we have is that the "Found Neighbor" lines are emitted and they're only emitted when, the get_ipv6_neighbors() actually returns a tuple. Because they're present, the helper comes back with ...some thing which we get from running the following preferably in this src/ directory within the context, of the Python shell : import asyncio, hermes.util as u, json \ print(json.dumps(asyncio.run(u.get_ipv6_neighbors("bridge0")), indent = 2))
And thus that is how we get output like this:
[
  ["fe80::c99:b110:347e:19dc", "local", "client"],
  ["fe80::439:13e9:a071:313e", "neighbor", "server"]
]
If we get this output then we know that the helper is "no longer" filtering everything out with what `ndp -a` is returning, for us we get something like the raw ndp -a. We get `ndp -a | grep bridge0`, which is what's going to make it possible for us to match the format exactly. So what we get is these lines;
fe80::439:13e9:a071:313e%bridge0 58:9c:fc:xx:yy:zz  stale
fe80::c99:b110:347e:19dc%bridge0 (incomplete)
Of course, what these lines are going to tell us is that at least one entry, is marked as REACHABLE, STALE, or PERMANENT that is not our own address and that the address part really matches fe80::...%bridge0 . Otherwise, and we know that the regex filters out INCOMPLETE, DELAY, etc. and, our pattern anchors on %bridge0 , "this is how we are allowed to know" that if every neighbor line is INCOMPLETE then the helper will drop them..otherwise the quick way , we can do a multicast ping `ping6 -c1 ff02::1%bridge0` as well as turn on debugging for the ports, wherein we know that we have a list of neighbors. it isn't so much the Hermes logs telling us lines like
Port.dean:en3 -- Found Neighbor. result=[...]
Port.dean:en3 -- Transport opened successfully
as it is the fact that `python` couldn't find the hermes package because we launched it from our home directory, outside the repo that contains DDL_Emulator/src which is why it's so important to make these, python instigations from within the src directory itself, that's ..how we see what the regex filter is doing : `ndp -a | grep bridge0`. The goal of course is for the helper to return the tuple and this is what the helper, isn't dropping ...if `ndp` isn't resolving names by default..this is actually why instead of setting sysname to Darwin we still set sysname to Darwin but then we find the flag, instead of utilizing the, default resolution from `ndp` with regard to the names we can find that the `fe80::` addresses, are something that the regex drops..we can use the numeric flag -n which is designated via ndp -an so that we always get the raw IPv6 address. That is the reason that we changed -a to -an, so that  we can see the log lines, and we know that the Tree view in the dashboard is going to demonstrate a Connected nature for the ports with a peer.
Port.dean:en3 -- Found Neighbor. result=[('fe80::c99:b110:347e:19dc', 'local', 'client'),
                                         ('fe80::439:13e9:a071:313e', 'neighbor', 'server')]
Port.dean:en3 -- Transport opened successfully
So that's how we "got started" out with IPv6, neighbor discovery.
"""


async def _ping_mcast(interface: str, group: str = "ff02::1") -> None:
    """ And so in the output of ndp -an we have that the lines start with a space..we don't necessarily need this space, but we can make it so that the "singular', the one multicast ping nudges the ND cache to REACHABLE. Ordinarily these signals would just go around in circles but now they're going in circles in single-letter state codes (R, S, P) instead of the words "REACHABle", "STALE", "PERMANENT" to name a "few".
    After we do that, which we have, we saw that the `python3 src/main.py -c scripts/prod_config.yml` did all the configuration we "ever" did on _each_ Mac which means that yes, we open the transport :
Port.dean:en3 -- Found Neighbor. result=[('fe80::...', 'local', 'client'), ('fe80::...', 'neighbor', 'server')]
Port.dean:en3 -- Transport opened successfully
    And so we do, we refresh the dashboard--and the port badges flip from Disconnected to Connected.
    """
    await run_subprocess(["ping6", "-c", "1", f"{group}%{interface}"], timeout=5)


async def get_ipv6_neighbors(interface: str) -> Optional[
    List[Tuple[str, str, str]]
]:
    """
    And so what if the ports are still quiet? If the get_ipv6_neighbors() function is still returning None when we "invoke it" inside the port thread, it's pretty clear that when we look for the two-element list, we get this :
        import asyncio, json, hermes.util as u
        print(json.dumps(asyncio.run(u.get_ipv6_neighbors("bridge0")), indent = 2))
    And we get that from the Python REPL we know, that the raw neighbor lines are things that we get, when we see with the new "nomenclature" what the helper is now looking for :
    <spaces>fe80::…%bridge0            …  R|P|S
    With regard specifically to the output of the `ndp -an | grep bridge0` which is "formalized" in our state code, we know that if macOS is going to show..something slightly different like for instance state code lowercase or extra columns, that might "impact the ability" of the regex to be "tweaked" in the..DEBUG level which exists for the Port logger, such that yes we can see every retry.
    And so what we've done is we've looked at the DEBUG level for the Port logger..such that we can see the retries in the change of the INFO to DEBUG and the way that we can configure logging. Then we can restart Hermes..such that each port thread prints on every discovery attempt..that we have successfully opened the transport, that means that the transport opened successfully which means that the dashboard's Tree, view flips immediately to Connected for that port.
    And so with the new util.py we know that `ndp -an` is going to "give" numer-ic addresses (no hostnames) as well as one-letter state codes (R = Reachable, S = Stale, P = Permanent), for which the helper now accepts leading whitespace and those single-letter states..furthermore, it only needs one local + one neighbor entry; extras we ignore, since once the REPL probe returns the 2-element list, any remaining issue is just timing/log visibility--not discovery logic. Therefore the `ndp -an | grep bridge0` lines have it so that the probe still returns `null` and we'll , nail the regex, so that we keep ONLY the part up toe "%bridge0", so that the line that reaches the parser no longer contains the state code R|S|P and it gets "discarded"..thus we have this fix in one line: we keep the whole line instead of just the substring.
    For now, our "task" is to discover the link-local peer on the *interface* which means, that we return the "list" | None paradigm, which "reads" as follows:
        [
          (local_addr,   'local',    'client' | 'server'),
          (neighbor_addr,'neighbor', 'client' | 'server')
        ]
    """
    # And so that's our patch, for the local addresses which means that we have kept, the lines..
    # We have kept the "full" ndp-an lines.
    ifcfg = await run_subprocess(["ifconfig", interface], 5)
    local_addrs = [
        ln.split()[1].split("%")[0]
        for ln in ifcfg.splitlines()
        if "inet6 " in ln
    ]
    if not local_addrs:
        return None
    # And so we find the Darwin block (~ line 70) and we keep the lines for ndp -an. Next, we can invoke the u.get_ipv6_neighbors("bridge0") ..thing from the command line as usual or we can ..we do that and then restart Hermes, via the atypical `python3 src/main.py -c scripts/prod_config.yml`. That s how we get the log lines per port, that is how we get the dashboard refreshed--we know, that the Tree view is going to switch those ports from Disconnected to Connected..when the two-element list means that `bridge0`, is "seeing" both its own link-local address and the peer's, while..roles, were assigned (server / client) as expected so that now, the port threads can connect. And so we take the ND cache, and bump it.
    await _ping_mcast(interface)
    """ And so we have got to add something additionally: the neighbor table. What this means is that ..we got everything as expected..the key thing to remember is that these comments are essentially just , the kinds of things we do when we restart, Hermes on both Macs so each port thread picks up the fixed discovery logic. I for one was a bit skeptical when I saw this but, that's the table of the neighbor, that just shows that we could invoke the entire system attribute of the platform and watch the logs and not see, for any active port..the port badges, that correspond to bridge0 which will flip from Disconnected to Connected.  """
    sysname = platform.system()
    lines = ""
    if sysname == "Linux":
        lines = await run_subprocess(
            ["ip", "-6", "neighbor", "show", "dev", interface], 5
        )
    elif sysname == "Darwin":
        ndp_out = await run_subprocess(["ndp", "-an"], 5)
        """ And what we've got is, the extra ports..ideally I'd have something like the `en2` and `en4` that serve as the foundation of having a live Thunderbolt partner thing..where we keep the *full* line so that we still have the R / S / P state column..but then again, we could also keep the Port logger at DEBUG (in Sim.configure_logging() ), and thus we will see retry cycles and ND cache outputs in real time. """
        lines = "\n".join(
            ln for ln in ndp_out.splitlines()
            if f"%{interface}" in ln and ln.lstrip().startswith("fe80::")
        )
    else:
        return None

    if not lines:
        return None
    """ And that is how we have live links between the two Macs, and all dashboard views (Tree, Raw, DAG, Analysis) will populate with real data..as we parse the neighbors.. """
    peers: list[Tuple[str, str]] = []
    VALID_LINUX = ("REACHABLE", "PERMANENT", "STALE")
    """ And we have the Stale, Permanent, Reachable codes for N.D.P. """
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

""" And this is the probe for the Command Line Interface..the Hermes' keep-alive / connect-timeout watchdog thing that kicks in, in the sense that the UDPPort.run_link() makes the socket - that's when the dashboard flashes green. We also have the EthernetProtocolExtended which starts its 'token ping" state-machine (`onConnected`, then the alternating-bit RA / RD / WT dance). If it doesn't get the expected reply from its peer inside CONNECT_TIMEOUT_MS (default ≈ 1 s), then it raises the following "Connection timed out" -> the port fires a DISCONNECTED signal -> the Agent tears the tree down -> the dashboard flips back to red. Then the port thread immediately restarts the discovery loop, so the cycle repeates => flicker."""
if __name__ == "__main__":
    import sys
    import json
    iface = sys.argv[1] if len(sys.argv) > 1 else "bridge0"
    print(json.dumps(asyncio.run(get_ipv6_neighbors(iface)), indent=2))
