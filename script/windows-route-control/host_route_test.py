"""Windows-only /32 mitigation test, using isolated interfaces and fresh TCP probes."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import unittest

from egress_test import powershell


@contextmanager
def route(prefix, interface):
    args = f"-DestinationPrefix '{prefix}' -InterfaceAlias {interface} -NextHop '0.0.0.0' -PolicyStore ActiveStore"
    powershell(f"New-NetRoute {args}")
    try:
        yield
    finally:
        powershell(f"Remove-NetRoute {args} -Confirm:$false")


@contextmanager
def instance(name, *args):
    with open(f"{name}.log", "wb") as logfile:
        proc = subprocess.Popen(
            [
                os.environ["LAB_ET"],
                "--instance-name",
                name,
                "--network-name",
                "lab",
                "--no-tun",
                "true",
                "--bind-device",
                "false",
                "--disable-ipv6",
                "true",
                "--disable-p2p",
                "true",
                "--disable-udp-hole-punching",
                "true",
                "--disable-tcp-hole-punching",
                "true",
                *args,
            ],
            stdout=logfile,
            stderr=subprocess.STDOUT,
        )
        try:
            yield proc
        finally:
            proc.terminate()
            proc.wait(timeout=10)


def current_conn():
    cli = Path(os.environ["LAB_ET"]).with_name("easytier-cli.exe")
    raw = subprocess.check_output(
        [cli, "-p", "127.0.0.1:25102", "-v", "peer", "list"],
        stderr=subprocess.DEVNULL,
        timeout=5,
    )

    def walk(node):
        if isinstance(node, dict):
            if (
                node.get("conn_id")
                and (node.get("tunnel") or {}).get("tunnel_type") == "wss"
            ):
                yield node["conn_id"]
            for value in node.values():
                yield from walk(value)
        elif isinstance(node, list):
            for value in node:
                yield from walk(value)

    ids = list(walk(json.loads(raw)))
    if len(ids) != 1:
        raise ValueError(f"expected one WSS connection, got {ids}: {raw[:600]!r}")
    return ids[0]


class HostRoute(unittest.TestCase):
    def check_paths(self, phase, relay, neighbor):
        for addr, expected in [("198.18.247.42", relay), ("198.18.247.43", neighbor)]:
            with (
                socket.create_connection((addr, 24443), timeout=5) as sock,
                sock.makefile("rb") as r,
            ):
                sock.sendall(f"{phase}\n".encode())
                reply = r.readline(128).decode().strip()
                print(phase, addr, reply, flush=True)
                self.assertEqual(reply, f"{expected} {phase}")

    def check_wss(self):
        with (
            instance("labC", "-l", "wss://127.0.0.1:29444", "-r", "25101"),
            instance(
                "labB",
                "--no-listener",
                "-p",
                "wss://198.18.247.42:24444",
                "-r",
                "25102",
            ),
        ):
            deadline = time.monotonic() + 45
            while True:
                try:
                    original = current_conn()
                    break
                except (ValueError, subprocess.SubprocessError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(1)
            start = time.monotonic()
            for _ in range(9):
                time.sleep(5)
                self.assertEqual(current_conn(), original)
            print(
                f"stock WSS conn_id={original}, unchanged at all 9 samples over {time.monotonic() - start:.1f}s",
                flush=True,
            )

    def test_host_exception(self):
        self.check_paths("baseline", "LabUnder", "LabUnder")
        with route("198.18.247.0/24", "LabAlt"):
            self.check_paths("prefix", "LabAlt", "LabAlt")
            with route("198.18.247.42/32", "LabUnder"):
                self.check_paths("exception", "LabUnder", "LabAlt")
                self.check_wss()
            self.check_paths("withdrawn", "LabAlt", "LabAlt")
        self.check_paths("cleanup", "LabUnder", "LabUnder")


if __name__ == "__main__":
    unittest.main(verbosity=2)
