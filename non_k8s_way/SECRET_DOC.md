# Secret Document: Bare Metal / VM Way (non_k8s_way)

This document provides a line-by-line conceptual breakdown, use cases, and technical intuition behind the Python application `aegis_edge.py` and its bare-metal deployment, aimed at engineers from Level 0 to Level 1+.

## The Intuition & Narrative: Breaking Down `aegis_edge.py`

When running on bare metal, we control the entire OS. We want to squeeze every ounce of performance out of the hardware to handle thousands of connections without standard web server overhead.

### 1. The `PartitionedMmapWAL` Class (The Disk Optimizer)
**The Problem:** Standard `file.write()` locks the file. If 50,000 devices try to write at once, they wait in a single-file line (Lock Contention). Also, standard writes copy data from the application to the kernel, then to the disk.
**The Fix:**
- `os.posix_fallocate()`: We instantly reserve 100MB of pure, unfragmented physical disk space.
- `mmap.mmap(...)`: We map that physical file directly into the OS's RAM (Page Cache).
- **Zero-Copy writes:** When a payload arrives, `self.mmap_obj[...] = record_data` writes the data directly into RAM. The Linux kernel handles flushing that RAM to disk in the background. The Python process never waits for the disk.

### 2. The 8-Byte Header (Crash Recovery)
```python
HEADER_FORMAT = "<Q"  # unsigned long long (8 bytes)
```
**The Story:** Because we are writing to RAM, what if the Python script crashes? If we used standard files, we'd lose track of where we left off.
**The Fix:** We reserve the first 8 bytes of the WAL file to store an integer: the `current_offset`. Every time we write a payload, we update these 8 bytes. Because `mmap` is backed by the kernel's Page Cache, even if Python is violently killed (SIGKILL), the OS retains the page cache. When the service restarts, it reads those 8 bytes and resumes appending exactly where it left off. No data loss, no corruption.

### 3. Asynchronous I/O (`asyncio`)
**The Problem:** Standard threading (e.g., a standard Flask or Django app) creates one OS thread per connection. 50,000 threads would completely crash the CPU scheduler.
**The Fix:** We use Python's `asyncio`. It uses the Linux `epoll` system call under the hood. A single Python worker thread can track 50,000 open network sockets simultaneously while practically sleeping. It only wakes up the specific connection handler exactly when a byte of data arrives on that specific wire.

## The Intuition & Narrative: Breaking Down System Tuning

### 1. The Systemd Service (`LimitNOFILE=100000`)
By default, Linux protects itself by limiting a single user or process to ~1,024 open files. Because in Linux *everything is a file* (including network sockets), a standard server will start rejecting connections at 1,024 devices with a "Too many open files" error. In `aegis-edge.service`, setting `LimitNOFILE=100000` tells the OS to allow this specific process to hold 100,000 open connections.

### 2. Kernel Sysctl Tuning (`/etc/sysctl.conf`)
```ini
net.ipv4.tcp_rmem = 4096 8192 65536
net.ipv4.tcp_wmem = 4096 8192 65536
```
This forces the Linux kernel to shrink its default TCP memory buffers. Instead of allocating massive buffers for potential 4K video streams, we tell it to allocate ~8KB per connection because we only expect small, frequent telemetry JSON payloads.

---

## Hardware Quantification & Load Scaling

So, what load can this Python script handle on a bare-metal server?

### The Metrics Breakdown:
* **Target Load:** 50,000 simultaneous device connections sending intermittent payloads.
* **Network RAM Cost:** Due to our sysctl tuning (8KB read + 8KB write per connection), 50,000 connections × 16KB = **~800MB of RAM**.
* **Disk/Memory Mapping Cost:** We allocate a 100MB WAL file. The `mmap` system maps this 100MB into the RAM Page Cache.
* **CPU Cost:** Because `asyncio` avoids thread-switching overhead, and `mmap` avoids disk I/O blocking, CPU usage is incredibly low. The CPU only processes the incoming byte streams and moves them to RAM.

### The Minimum Viable Server:
A single, cheap **1 vCPU / 2GB RAM** Virtual Machine (like a $10 DigitalOcean Droplet or an AWS t3.micro) can easily handle 50,000 devices.
- **RAM Calculation:** 800MB (Network) + 100MB (WAL Page Cache) + ~100MB (Python App) = ~1GB active RAM. The server has 2GB, leaving 1GB free for the OS.
- **CPU Calculation:** 1 vCPU is more than enough for pure `asyncio` network I/O without heavy data processing.

If you tried to build this with standard tools (threaded Python + standard file I/O), you would need a massive 16+ core, 32GB+ RAM server to handle the thread overhead and RAM exhaustion. Aegis Edge achieves it on a tiny VM.