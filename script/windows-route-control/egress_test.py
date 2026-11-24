"""Exercise only the Windows TCP routing contract; both echo peers are runner-local."""

import os
import socket
import struct
import subprocess
import unittest


PEER = ("198.18.247.42", 24443)
IP_UNICAST_IF = 31  # Windows ws2ipdef.h


def powershell(command):
    subprocess.run(
        ["pwsh", "-NoProfile", "-Command", "$ErrorActionPreference='Stop';" + command],
        check=True,
    )


class WindowsRouting(unittest.TestCase):
    def test_established_interface_bound_tcp(self):
        index = int(os.environ["LAB_IFINDEX"])
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
            conn.settimeout(8)
            conn.setsockopt(socket.IPPROTO_IP, IP_UNICAST_IF, struct.pack("!I", index))
            conn.bind(("198.18.247.2", 0))
            conn.connect(PEER)
            original = conn.getsockname()
            with conn.makefile("rb") as reader:

                def verify(sock, stream, phase, nic):
                    sock.sendall(f"{phase}\n".encode())
                    reply = stream.readline(128).decode().strip()
                    print(
                        f"{phase}: {sock.getsockname()} -> {PEER}: {reply}", flush=True
                    )
                    self.assertEqual(reply, f"{nic} {phase}")

                verify(conn, reader, "before", "LabUnder")
                powershell(
                    "New-NetRoute -DestinationPrefix '198.18.247.32/27' "
                    "-InterfaceAlias LabAlt -NextHop '0.0.0.0' -PolicyStore ActiveStore"
                )
                try:
                    # A fresh, unconstrained client positively proves the new route is effective.
                    with (
                        socket.create_connection(PEER, timeout=8) as other,
                        other.makefile("rb") as r,
                    ):
                        verify(other, r, "fresh-default", "LabAlt")
                    self.assertEqual(conn.getsockname(), original)
                    verify(conn, reader, "established-bound", "LabUnder")
                finally:
                    powershell(
                        "Remove-NetRoute -DestinationPrefix '198.18.247.32/27' "
                        "-InterfaceAlias LabAlt -PolicyStore ActiveStore -Confirm:$false"
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
