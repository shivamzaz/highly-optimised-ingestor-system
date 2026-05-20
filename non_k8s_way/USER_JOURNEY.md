# User Journey: Non-Kubernetes Implementation of Aegis Edge

This document outlines the user journey—the lifecycle of a telemetry payload from an edge device to its persistent storage—by tracing the code in `non_k8s_way/aegis_edge.py` line-by-line. It also includes brainstorm sections to discuss design decisions, edge cases, and future enhancements.

## The Journey

The journey begins when the `EdgeCollectorServer` is started and a device attempts to send data.

### 1. Server Initialization and Startup

The server starts by parsing arguments and verifying system limits.

```python
if __name__ == "__main__":
    # The application starts here. It sets up argument parsing, including a dry-run mode for testing.
    parser = argparse.ArgumentParser(description="Aegis Edge Collector")
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode without I/O or binding ports')
    args = parser.parse_args()

    # Step 1: Verify System Limits
    verify_system_limits()
```

*Inside `verify_system_limits()`:*
```python
def verify_system_limits():
    """Verify system file descriptor limits to ensure it can handle high concurrency."""
    # We check the RLIMIT_NOFILE (Number of open files).
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < 50000:
        # If the limit is too low, we warn the operator. In production, this needs to be high.
        logging.warning(f"FD Limit is low ({soft_limit}). High throughput may cause 'Too many open files' errors.")
    else:
        logging.info(f"System limits verified. Max open files: {soft_limit}")
```
**Brainstorm:** Should we enforce a hard exit if the limits are too low instead of just warning? In a non-k8s environment, perhaps the application shouldn't even start if it cannot guarantee the required scale.

### 2. Initializing the Server and WAL

```python
    # Step 2: Create the EdgeCollectorServer instance
    collector = EdgeCollectorServer(dry_run=args.dry_run)
```

*Inside `EdgeCollectorServer.__init__()`:*
```python
class EdgeCollectorServer:
    def __init__(self, host='0.0.0.0', port=8888, max_requests_per_conn=1000, wal_dir="/var/log/aegis_edge", dry_run=False):
        # Configuration setup...
        if not self.dry_run:
            os.makedirs(self.wal_dir, exist_ok=True) # Ensure the directory exists

        pid = os.getpid()
        # Each worker process gets its own dedicated WAL file based on its PID. This is the "Shared-Nothing" architecture.
        self.wal_file = os.path.join(self.wal_dir, f"wal_worker_{pid}.dat")
        self.wal = PartitionedMmapWAL(self.wal_file, max_size_mb=100, dry_run=self.dry_run)
```

*Inside `PartitionedMmapWAL.__init__()`:*
```python
class PartitionedMmapWAL:
    # ...
    def __init__(self, filepath: str, max_size_mb: int = 100, dry_run: bool = False):
        # ... (skipping dry-run logic for clarity)
        is_new = not os.path.exists(filepath)
        # Open the file for reading and writing, create if it doesn't exist.
        self.fd = os.open(filepath, os.O_CREAT | os.O_RDWR)

        if is_new:
            # Allocate space immediately to prevent fragmentation and latency during spikes.
            if hasattr(os, 'posix_fallocate'):
                try:
                    os.posix_fallocate(self.fd, 0, self.max_size)
                except OSError as e:
                    logging.warning(f"posix_fallocate failed: {e}. Falling back to ftruncate.")
                    os.ftruncate(self.fd, self.max_size)
            else:
                os.ftruncate(self.fd, self.max_size)

        # Map the file into memory. We only request WRITE access to optimize.
        self.mmap_obj = mmap.mmap(self.fd, self.max_size, access=mmap.ACCESS_WRITE)

        if is_new:
            # For a new file, the data offset starts after the header.
            self.current_offset = self.HEADER_SIZE
            self._write_header() # Write the initial offset (8 bytes)
        else:
            # Crash Recovery: If the file exists, read the 8-byte header to know exactly where we left off.
            self.current_offset = struct.unpack(self.HEADER_FORMAT, self.mmap_obj[:self.HEADER_SIZE])[0]
```
**Brainstorm:** The WAL rotation strategy is currently limited. What happens when the 100MB file is full? The code just stops writing (`return False` in `append`). We need a robust rotation mechanism (e.g., closing `wal_worker_PID_1.dat` and opening `wal_worker_PID_2.dat`). We should also consider syncing data periodically (via `msync`) to guarantee data is on disk.

### 3. Running the Async Event Loop

```python
    try:
        # Step 3: Run the asyncio event loop
        asyncio.run(collector.run())
        # ...
```

*Inside `EdgeCollectorServer.run()`:*
```python
    async def run(self):
        # ...
        # Start an asyncio TCP server. For every new connection, it calls `self.handle_client`.
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"Server listening on {self.host}:{self.port} with WAL {self.wal_file}")

        async with server:
            # The server will run forever, listening for connections.
            await server.serve_forever()
```

### 4. Handling an Edge Device Connection

When a device connects, `handle_client` is spawned as a task.

```python
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info('peername')
        logging.info(f"New connection from {client_addr}")
        request_count = 0

        try:
            # This is a Keep-Alive connection. We process up to max_requests_per_conn (1000).
            while request_count < self.max_requests_per_conn:
                # Step 4a: Read the payload length (4 bytes)
                length_bytes = await reader.readexactly(4)
                if not length_bytes:
                    break # Client closed the connection cleanly

                payload_len = struct.unpack('<I', length_bytes)[0]

                # Step 4b: Read the actual payload
                payload = await reader.readexactly(payload_len)

                # Step 4c: Append the payload to the WAL
                success = self.wal.append(payload)
                if not success:
                    # If append fails (e.g., WAL is full), we break the loop and close the connection.
                    break

                request_count += 1

        except asyncio.IncompleteReadError:
            # The connection was closed unexpectedly while reading.
            pass
        except Exception as e:
            logging.error(f"Connection error from {client_addr}: {e}")
        finally:
            # Cleanup: Close the connection
            logging.info(f"Closing connection {client_addr} after {request_count} requests")
            writer.close()
            await writer.wait_closed()
```
**Brainstorm:** Security and validation are missing. We blindly trust the `payload_len`. What if a malicious device sends a 2GB payload length? We need maximum payload size limits. Also, how do we handle authentication for these devices? We might need TLS or a lightweight auth handshake.

### 5. Writing to the Memory-Mapped WAL

The actual data persistence happens here.

```python
    def append(self, payload: bytes) -> bool:
        """
        Appends data to the mmap file.
        Format per record: [4 bytes payload length] + [Payload bytes]
        """
        payload_len = len(payload)
        # Pack the length and the payload into a binary structure
        record_format = f"<I{payload_len}s"
        record_data = struct.pack(record_format, payload_len, payload)
        record_size = len(record_data)

        # Check if we have enough space
        if self.current_offset + record_size > self.max_size:
            logging.warning(f"WAL {self.filepath} is full. Rotation needed.")
            return False

        # ... (dry-run logic skipped)

        # Calculate the end offset
        end_offset = self.current_offset + record_size

        # Step 5a: Zero-Copy Write directly to memory (OS Page Cache)
        self.mmap_obj[self.current_offset:end_offset] = record_data

        # Update the offset
        self.current_offset = end_offset

        # Step 5b: Update the 8-byte header to reflect the new state (Crash Recovery mechanism)
        self._write_header()

        return True
```
**Brainstorm:** Updating the header (`_write_header()`) after every single message is highly robust but might be a bottleneck. Could we update the header periodically (e.g., every 100 messages or every 1 second)? It trades a tiny bit of crash safety for potentially huge throughput gains.

### 6. Shutdown

When the application receives a signal to stop (like `KeyboardInterrupt`).

```python
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        # Crucial step: Ensure all data in memory is flushed to the physical disk before exiting.
        collector.wal.close()
```

*Inside `PartitionedMmapWAL.close()`:*
```python
    def close(self):
        if not self.dry_run:
            self.force_flush() # Calls self.mmap_obj.flush()
            self.mmap_obj.close()
            os.close(self.fd)
```
**Brainstorm:** Graceful shutdown is critical. When shutting down, we should ideally stop accepting new connections and allow existing connections to finish their current payload processing before abruptly closing the WAL file.

## Conclusion

The user journey highlights an incredibly fast ingestion layer optimized by bypassing traditional filesystem I/O and utilizing persistent TCP connections. However, the brainstorm sections reveal that for a fully production-ready system, features like WAL rotation, payload validation, and graceful shutdown logic are necessary additions.
