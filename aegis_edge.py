import asyncio
import mmap
import os
import struct
import logging
import resource

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [PID %(process)d] %(message)s')

class PartitionedMmapWAL:
    # 8 bytes for an unsigned long long (Q) to store the write offset
    HEADER_FORMAT = "<Q"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, filepath: str, max_size_mb: int = 100):
        self.filepath = filepath
        self.max_size = max_size_mb * 1024 * 1024

        # 1. Create and pre-allocate the file if it doesn't exist
        is_new = not os.path.exists(filepath)
        self.fd = os.open(filepath, os.O_CREAT | os.O_RDWR)

        if is_new:
            # os.posix_fallocate is vital on Linux. It allocates actual disk blocks
            # instantly without writing zeros, preventing fragmentation and OS pauses.
            if hasattr(os, 'posix_fallocate'):
                try:
                    os.posix_fallocate(self.fd, 0, self.max_size)
                except OSError as e:
                    logging.warning(f"posix_fallocate failed (maybe unsupported FS): {e}. Falling back to ftruncate.")
                    os.ftruncate(self.fd, self.max_size)
            else:
                os.ftruncate(self.fd, self.max_size)

        # 2. Memory-map the file
        self.mmap_obj = mmap.mmap(self.fd, self.max_size, access=mmap.ACCESS_WRITE)

        # 3. Initialize or read the offset header
        if is_new:
            self.current_offset = self.HEADER_SIZE
            self._write_header()
        else:
            self.current_offset = struct.unpack(self.HEADER_FORMAT, self.mmap_obj[:self.HEADER_SIZE])[0]

    def _write_header(self):
        """Persists the current offset to the first 8 bytes of the file."""
        self.mmap_obj[:self.HEADER_SIZE] = struct.pack(self.HEADER_FORMAT, self.current_offset)

    def append(self, payload: bytes) -> bool:
        """
        Appends data to the mmap file.
        Format per record: [4 bytes payload length] + [Payload bytes]
        """
        payload_len = len(payload)
        record_format = f"<I{payload_len}s"
        record_data = struct.pack(record_format, payload_len, payload)
        record_size = len(record_data)

        # Check if we have enough space left
        if self.current_offset + record_size > self.max_size:
            logging.warning(f"WAL {self.filepath} is full. Needs rotation.")
            return False

        # 4. Direct memory write (bypassing standard file I/O)
        end_offset = self.current_offset + record_size
        self.mmap_obj[self.current_offset:end_offset] = record_data

        # 5. Update offset in memory and header
        self.current_offset = end_offset
        self._write_header()

        return True

    def force_flush(self):
        """Forces the OS to flush dirty pages to disk immediately."""
        self.mmap_obj.flush()

    def close(self):
        self.force_flush()
        self.mmap_obj.close()
        os.close(self.fd)


def verify_system_limits():
    """Verify system file descriptor limits."""
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < 50000:
        logging.warning(f"FD Limit is quite low ({soft_limit}). High throughput may cause 'Too many open files' errors.")
    else:
        logging.info(f"System limits verified. Max open files: {soft_limit}")


class EdgeCollectorServer:
    def __init__(self, host='0.0.0.0', port=8888, max_requests_per_conn=1000, wal_dir="/mnt/wal_storage"):
        self.host = host
        self.port = port
        self.max_requests_per_conn = max_requests_per_conn
        self.wal_dir = wal_dir
        os.makedirs(self.wal_dir, exist_ok=True)

        pid = os.getpid()
        self.wal_file = os.path.join(self.wal_dir, f"wal_worker_{pid}.dat")
        self.wal = PartitionedMmapWAL(self.wal_file, max_size_mb=100)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info('peername')
        logging.info(f"New connection from {client_addr}")
        request_count = 0

        try:
            while request_count < self.max_requests_per_conn:
                # Expecting 4 bytes of payload length
                length_bytes = await reader.readexactly(4)
                if not length_bytes:
                    break

                payload_len = struct.unpack('<I', length_bytes)[0]
                payload = await reader.readexactly(payload_len)

                # Write to WAL directly
                success = self.wal.append(payload)
                if not success:
                    logging.warning("WAL full, dropping payload! Implement rotation here.")
                    break

                request_count += 1

        except asyncio.IncompleteReadError:
            # Connection closed cleanly by client
            pass
        except Exception as e:
            logging.error(f"Error handling connection from {client_addr}: {e}")
        finally:
            logging.info(f"Closing connection from {client_addr} after {request_count} requests")
            writer.close()
            await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"Server listening on {self.host}:{self.port} with WAL {self.wal_file}")

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    verify_system_limits()
    collector = EdgeCollectorServer()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        collector.wal.close()
