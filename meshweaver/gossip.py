"""Asynchronous resource gossip using psutil."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Callable

import psutil


@dataclass
class ResourceRecord:
    node_id: str
    cpu_percent: float
    ram_percent: float
    timestamp: float
    sequence: int
    message_id: str


class GossipEngine:
    """Collect local resources and share them at a fixed interval."""

    def __init__(
        self,
        node_id: str,
        interval: float = 5.0,
        stale_after: float = 30.0,
        sender: Callable[[dict, tuple[str, int]], None] | None = None,
    ):
        self.node_id = node_id
        self.interval = interval
        self.stale_after = stale_after
        self._sender = sender
        self.records: dict[str, ResourceRecord] = {}
        self._seen_messages: set[str] = set()
        self._sequence = 0
        self._task: asyncio.Task | None = None
        self._peers: Callable[[], list[tuple[str, str, int]]] | None = None

    def set_sender(self, sender: Callable[[dict, tuple[str, int]], None]) -> None:
        self._sender = sender

    def set_peer_provider(
        self,
        provider: Callable[[], list[tuple[str, str, int]]],
    ) -> None:
        self._peers = provider

    async def collect_local(self) -> dict:
        loop = asyncio.get_running_loop()
        cpu, ram = await loop.run_in_executor(None, self._collect_psutil)
        self._sequence += 1
        message_id = uuid.uuid4().hex
        message = {
            "type": "GOSSIP_RESOURCE",
            "node_id": self.node_id,
            "cpu_percent": cpu,
            "ram_percent": ram,
            "timestamp": time.time(),
            "sequence": self._sequence,
            "message_id": message_id,
        }
        self.update(message)
        return message

    @staticmethod
    def _collect_psutil() -> tuple[float, float]:
        cpu = float(psutil.cpu_percent(interval=None))
        ram = float(psutil.virtual_memory().percent)
        return cpu, ram

    def update(self, message: dict) -> bool:
        if not isinstance(message, dict) or message.get("type") != "GOSSIP_RESOURCE":
            return False
        node_id = message.get("node_id")
        message_id = message.get("message_id")
        sequence = message.get("sequence")
        cpu = message.get("cpu_percent")
        ram = message.get("ram_percent")
        timestamp = message.get("timestamp")
        if not isinstance(node_id, str) or not node_id:
            return False
        if not isinstance(message_id, str) or not message_id:
            return False
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
            return False
        if not isinstance(cpu, (int, float)) or isinstance(cpu, bool):
            return False
        if not isinstance(ram, (int, float)) or isinstance(ram, bool):
            return False
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            return False
        cpu = float(cpu)
        ram = float(ram)
        if not (0.0 <= cpu <= 100.0 and 0.0 <= ram <= 100.0):
            return False

        if message_id in self._seen_messages:
            return False
        existing = self.records.get(node_id)
        if existing is not None and sequence <= existing.sequence:
            self._seen_messages.add(message_id)
            return False

        self._seen_messages.add(message_id)
        if len(self._seen_messages) > 4096:
            self._seen_messages = set(list(self._seen_messages)[2048:])
        self.records[node_id] = ResourceRecord(
            node_id=node_id,
            cpu_percent=cpu,
            ram_percent=ram,
            timestamp=float(timestamp),
            sequence=sequence,
            message_id=message_id,
        )
        return True

    def get_resource(self, node_id: str) -> ResourceRecord | None:
        record = self.records.get(node_id)
        if record is not None and self.is_stale(record):
            del self.records[node_id]
            return None
        return record

    def get_all_resources(self) -> dict[str, ResourceRecord]:
        now = time.time()
        stale = [
            node_id
            for node_id, record in self.records.items()
            if now - record.timestamp > self.stale_after
        ]
        for node_id in stale:
            del self.records[node_id]
        return dict(self.records)

    def is_stale(self, record: ResourceRecord, now: float | None = None) -> bool:
        current = time.time() if now is None else now
        return current - record.timestamp > self.stale_after

    def forward(self, message: dict) -> None:
        if self._sender is None or self._peers is None:
            return
        for peer_id, host, port in self._peers():
            if peer_id != message.get("node_id"):
                try:
                    self._sender(message, (host, port))
                except (OSError, RuntimeError):
                    pass

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                message = await self.collect_local()
                self.forward(message)
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise
