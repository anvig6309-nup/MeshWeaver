"""Week 1 / Task Serialization: ship a Python function to a peer and run it.

Mesh pings stay on UDP (see mesh_node.py). Task payloads can be large, so they
travel over TCP on the SAME port number as the node's UDP socket.

TCP frame (both directions):
    [4-byte big-endian header length][JSON header][payload bytes]
Header: {"type": "TASK"|"RESULT"|"ERROR", "task_id": ..., "sha256": ..., "size": N}

SECURITY NOTE: unpickling executes code. Only run nodes on networks/peers you
trust. The SHA-256 digest detects corruption/tampering in transit; it is NOT
authentication.
"""

import asyncio
import hashlib
import hmac
import inspect
import json
import struct
import traceback
import uuid

import cloudpickle

from .mesh_node import MeshNode

MAX_PAYLOAD = 10 * 1024 * 1024  # 10 MB safety cap
MAX_HEADER = 64 * 1024


class IntegrityError(Exception):
    """Payload hash did not match."""


class RemoteTaskError(Exception):
    """The task raised an exception on the remote node."""


# ---------- serialization helpers ----------
def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def serialize_task(func, *args, **kwargs):
    """Pickle (func, args, kwargs) by value. Returns (payload_bytes, sha256_hex)."""
    payload = cloudpickle.dumps((func, args, kwargs))
    return payload, sha256_hex(payload)


def verify(payload: bytes, expected_hash: str):
    if not hmac.compare_digest(sha256_hex(payload), str(expected_hash)):
        raise IntegrityError("SHA-256 mismatch: payload corrupted or tampered with")


def deserialize_task(payload: bytes, expected_hash: str):
    verify(payload, expected_hash)
    return cloudpickle.loads(payload)  # -> (func, args, kwargs)


# ---------- framing helpers ----------
async def _write_frame(writer, header: dict, payload: bytes = b""):
    header = dict(header, size=len(payload), sha256=sha256_hex(payload))
    raw = json.dumps(header).encode("utf-8")
    writer.write(struct.pack(">I", len(raw)) + raw + payload)
    await writer.drain()


async def _read_frame(reader):
    (hlen,) = struct.unpack(">I", await reader.readexactly(4))
    if hlen > MAX_HEADER:
        raise ValueError("header too large")
    header = json.loads((await reader.readexactly(hlen)).decode("utf-8"))
    size = int(header.get("size", 0))
    if size < 0 or size > MAX_PAYLOAD:
        raise ValueError("payload too large")
    payload = await reader.readexactly(size) if size else b""
    return header, payload


class TaskNode(MeshNode):
    """A MeshNode that can also receive tasks and submit tasks to peers."""

    def __init__(self, host="127.0.0.1", port=0, node_id=None, **kwargs):
        super().__init__(host, port, node_id, **kwargs)
        self._server = None
        self.tasks_executed = 0

    async def start(self):
        if self.port != 0:
            await super().start()
            self._server = await asyncio.start_server(
                self._handle_client, self.host, self.port
            )
            return self
        # port 0: find a port that is free for BOTH UDP and TCP
        last_exc = None
        for _ in range(20):
            await super().start()
            try:
                self._server = await asyncio.start_server(
                    self._handle_client, self.host, self.port
                )
                return self
            except OSError as exc:
                last_exc = exc
                await super().stop()
                self.port = 0
        raise last_exc

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        await super().stop()

    # ---------- receiving side ----------
    async def _handle_client(self, reader, writer):
        try:
            header, payload = await _read_frame(reader)
            if header.get("type") != "TASK":
                raise ValueError("expected TASK frame")
            task_id = header.get("task_id")
            try:
                func, args, kwargs = deserialize_task(payload, header["sha256"])
                loop = asyncio.get_running_loop()
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    # run blocking code off the event loop so the node stays responsive
                    result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                    if inspect.isawaitable(result):
                        result = await result
                self.tasks_executed += 1
                await _write_frame(
                    writer, {"type": "RESULT", "task_id": task_id},
                    cloudpickle.dumps(result),
                )
            except Exception as exc:  # task failed / integrity failed
                err = {"kind": type(exc).__name__, "message": str(exc),
                       "traceback": traceback.format_exc()}
                await _write_frame(
                    writer, {"type": "ERROR", "task_id": task_id},
                    json.dumps(err).encode("utf-8"),
                )
        except (asyncio.IncompleteReadError, ValueError, ConnectionError):
            pass  # malformed/aborted connection - drop it
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # ---------- sending side ----------
    async def submit(self, host, port, func, *args, timeout=30.0, **kwargs):
        """Run func(*args, **kwargs) on the peer at (host, port); return its result."""
        payload, digest = serialize_task(func, *args, **kwargs)
        return await self._submit_raw(host, port, payload, digest, timeout)

    async def _submit_raw(self, host, port, payload, digest, timeout=30.0):
        async def _go():
            reader, writer = await asyncio.open_connection(host, port)
            try:
                header = {"type": "TASK", "task_id": uuid.uuid4().hex,
                          "node_id": self.node_id}
                raw = json.dumps(dict(header, size=len(payload), sha256=digest)).encode()
                writer.write(struct.pack(">I", len(raw)) + raw + payload)
                await writer.drain()
                rheader, rpayload = await _read_frame(reader)
            finally:
                writer.close()
            if rheader.get("type") == "ERROR":
                info = json.loads(rpayload.decode("utf-8"))
                if info["kind"] == "IntegrityError":
                    raise IntegrityError(info["message"])
                raise RemoteTaskError(f'{info["kind"]}: {info["message"]}\n{info["traceback"]}')
            verify(rpayload, rheader["sha256"])
            return cloudpickle.loads(rpayload)

        return await asyncio.wait_for(_go(), timeout)
