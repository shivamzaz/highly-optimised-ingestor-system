#!/usr/bin/env python3
"""
Aegis Edge Collector - High-Throughput Edge Ingestion
-------------------------------------------------------
Use Case:
This application is designed to receive high-frequency telemetry data from thousands of edge
devices (e.g., IoT sensors, manufacturing equipment) over long-lived (Keep-Alive) TCP connections.
It uses an asynchronous event loop (asyncio) to manage the massive concurrent connections efficiently,
and memory-mapped (mmap) Write-Ahead Logs (WAL) to persist data at near-RAM latency, bypassing standard
OS disk I/O bottlenecks.

Architecture:
- Shared-Nothing WAL: Each worker process maintains its own dedicated mmap WAL file.
- Zero-Copy Writes: Payloads are written directly to the OS Page Cache.
- Async I/O: Handles persistent connections without thread starvation.

Usage:
  python3 aegis_edge.py [OPTIONS]

Options:
  --dry-run   Run the server without actually mapping files or listening on ports.
              Useful for validating syntax and limits.
"""

import argparse
import asyncio
import logging
import mmap
import os
import resource
import struct

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [PID %(process)d] %(message)s')

class PartitionedMmapWAL:
    # USER JOURNEY: WAL Initialization & State Management
    # This class manages the memory-mapped file for persistent storage.

    # 8 bytes for an unsigned long long (Q) to store the write offset.
    # This header acts as the single source of truth for the file's state.
    # [BRAINSTORM] This allows crash recovery. Is 8 bytes enough if files grow beyond standard limits? (Yes, Q handles massive sizes, but what about checksums?)
    HEADER_FORMAT = "<Q"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, filepath: str, max_size_mb: int = 100, dry_run: bool = False):
        self.filepath = filepath
        # USER JOURNEY: Server configures WAL size.
        self.max_size = max_size_mb * 1024 * 1024
        self.dry_run = dry_run

        if self.dry_run:
            logging.info(f"[DRY-RUN] Would create mmap WAL at {self.filepath} ({max_size_mb}MB)")
            self.current_offset = self.HEADER_SIZE
            return

        is_new = not os.path.exists(filepath)
        # USER JOURNEY: We open the file, creating it if it's the first time this worker runs.
        self.fd = os.open(filepath, os.O_CREAT | os.O_RDWR)

        if is_new:
            # os.posix_fallocate allocates actual disk blocks instantly,
            # preventing fragmentation and OS pauses during traffic spikes.
            # USER JOURNEY: Reserving space immediately. This avoids standard disk I/O wait times later.
            # [BRAINSTORM] Fallback to ftruncate works, but it's sparse on some filesystems and doesn't guarantee contiguous space.
            if hasattr(os, 'posix_fallocate'):
                try:
                    os.posix_fallocate(self.fd, 0, self.max_size)
                except OSError as e:
                    logging.warning(f"posix_fallocate failed: {e}. Falling back to ftruncate.")
                    os.ftruncate(self.fd, self.max_size)
            else:
                os.ftruncate(self.fd, self.max_size)

        # Memory-map the file
        # USER JOURNEY: The OS maps the physical disk space directly into RAM (Page Cache).
        self.mmap_obj = mmap.mmap(self.fd, self.max_size, access=mmap.ACCESS_WRITE)

        if is_new:
            # USER JOURNEY: Fresh file. Start data offset right after the 8-byte header.
            self.current_offset = self.HEADER_SIZE
            self._write_header()
        else:
            # USER JOURNEY: Crash Recovery! We survived a reboot/crash. Read the 8 bytes to see exactly where we stopped writing.
            self.current_offset = struct.unpack(self.HEADER_FORMAT, self.mmap_obj[:self.HEADER_SIZE])[0]

    def _write_header(self):
        # USER JOURNEY: Synchronize the in-memory offset to the mapped file (which OS syncs to disk).
        if not self.dry_run:
            self.mmap_obj[:self.HEADER_SIZE] = struct.pack(self.HEADER_FORMAT, self.current_offset)

    def append(self, payload: bytes) -> bool:
        """
        Appends data to the mmap file.
        Format per record: [4 bytes payload length] + [Payload bytes]
        """
        # USER JOURNEY: Preparing the payload for the WAL.
        payload_len = len(payload)
        # [BRAINSTORM] Should we add a CRC32 checksum to each record to detect corruption on disk?
        record_format = f"<I{payload_len}s"
        record_data = struct.pack(record_format, payload_len, payload)
        record_size = len(record_data)

        # USER JOURNEY: Checking if we have enough space.
        if self.current_offset + record_size > self.max_size:
            # [BRAINSTORM] Rotation logic is missing! Right now it just stops writing. We need to close this file and open `wal_worker_PID_2.dat`.
            logging.warning(f"WAL {self.filepath} is full. Rotation needed.")
            return False

        if self.dry_run:
            self.current_offset += record_size
            return True

        end_offset = self.current_offset + record_size

        # USER JOURNEY: Zero-Copy write. We write directly to the OS Page Cache in memory. Extremely fast.
        self.mmap_obj[self.current_offset:end_offset] = record_data

        # USER JOURNEY: Updating our internal tracker.
        self.current_offset = end_offset

        # USER JOURNEY: Saving the new offset to the header for crash recovery.
        # [BRAINSTORM] Writing the header on EVERY message is safe but slow. Can we update this every 100 messages or on an async timer?
        self._write_header()

        return True

    def force_flush(self):
        if not self.dry_run:
            self.mmap_obj.flush()

    def close(self):
        if not self.dry_run:
            self.force_flush()
            self.mmap_obj.close()
            os.close(self.fd)

def verify_system_limits():
    """Verify system file descriptor limits to ensure it can handle high concurrency."""
    # USER JOURNEY: Application startup. It needs massive open file descriptors to hold Keep-Alive connections.
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < 50000:
        # [BRAINSTORM] Should this be a fatal error (sys.exit) in production instead of just a warning?
        logging.warning(f"FD Limit is low ({soft_limit}). High throughput may cause 'Too many open files' errors.")
    else:
        logging.info(f"System limits verified. Max open files: {soft_limit}")

class EdgeCollectorServer:
    def __init__(self, host='0.0.0.0', port=8888, max_requests_per_conn=1000, wal_dir="/var/log/aegis_edge", dry_run=False):
        self.host = host
        self.port = port
        self.max_requests_per_conn = max_requests_per_conn
        self.wal_dir = wal_dir
        self.dry_run = dry_run

        if not self.dry_run:
            os.makedirs(self.wal_dir, exist_ok=True)

        pid = os.getpid()
        self.wal_file = os.path.join(self.wal_dir, f"wal_worker_{pid}.dat")
        self.wal = PartitionedMmapWAL(self.wal_file, max_size_mb=100, dry_run=self.dry_run)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # USER JOURNEY: A new edge device connects.
        client_addr = writer.get_extra_info('peername')
        logging.info(f"New connection from {client_addr}")
        request_count = 0

        try:
            # USER JOURNEY: The device keeps the connection open (Keep-Alive) and streams multiple payloads over time.
            while request_count < self.max_requests_per_conn:
                # USER JOURNEY: Waiting for the device to tell us how big the next payload is (4 bytes).
                # [BRAINSTORM] This assumes perfectly behaving clients. A timeout (asyncio.wait_for) is needed to drop idle connections.
                length_bytes = await reader.readexactly(4)
                if not length_bytes:
                    break # Device disconnected gracefully.

                payload_len = struct.unpack('<I', length_bytes)[0]
                # [BRAINSTORM] Security risk: What if payload_len is maliciously huge (e.g., 2GB)? We need a MAX_PAYLOAD_SIZE check.

                # USER JOURNEY: Reading the actual payload based on the declared length.
                payload = await reader.readexactly(payload_len)

                # USER JOURNEY: Passing the raw bytes to the WAL for persistent storage.
                success = self.wal.append(payload)
                if not success:
                    # [BRAINSTORM] If WAL fails (e.g., full), we just drop the connection. We might want to send a failure signal back to the client.
                    break

                request_count += 1

        except asyncio.IncompleteReadError:
            # USER JOURNEY: The device lost network connection abruptly while sending data.
            pass
        except Exception as e:
            logging.error(f"Connection error from {client_addr}: {e}")
        finally:
            # USER JOURNEY: The connection is finally closed, freeing up the socket and File Descriptor.
            logging.info(f"Closing connection {client_addr} after {request_count} requests")
            writer.close()
            await writer.wait_closed()

    async def run(self):
        if self.dry_run:
            logging.info(f"[DRY-RUN] Server would listen on {self.host}:{self.port}")
            # Keep alive slightly in dry-run to prove it works
            await asyncio.sleep(2)
            return

        # USER JOURNEY: Server boots up and binds to the network interface, ready for edge devices.
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"Server listening on {self.host}:{self.port} with WAL {self.wal_file}")

        async with server:
            # [BRAINSTORM] We might want a signal handler here (SIGINT/SIGTERM) to do a graceful shutdown of the server loop.
            await server.serve_forever()

if __name__ == "__main__":
    # USER JOURNEY: Operator starts the application.
    parser = argparse.ArgumentParser(description="Aegis Edge Collector")
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode without I/O or binding ports')
    args = parser.parse_args()

    verify_system_limits()
    collector = EdgeCollectorServer(dry_run=args.dry_run)
    try:
        asyncio.run(collector.run())
        if args.dry_run:
            logging.info("[DRY-RUN] Dry run completed successfully.")
    except KeyboardInterrupt:
        # USER JOURNEY: Operator stops the application (Ctrl+C).
        logging.info("Shutting down...")
    finally:
        # USER JOURNEY: Vital step to flush the OS Page Cache to the physical SSD to prevent data loss.
        collector.wal.close()
