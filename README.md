# Aegis Edge: High-Throughput Memory-Mapped Telemetry Ingestion

Welcome to **Aegis Edge**. If you are building an ingestion layer to handle thousands of concurrent edge devices (IoT, factory sensors, telemetry streams) sending high-frequency payloads, you have arrived at the right place.

This repository demonstrates how to build a lock-free, zero-copy, highly concurrent ingestion layer in pure Python by completely bypassing standard Operating System disk I/O bottlenecks.

This README is designed to explain **what** we are doing, **why** we are doing it, and the **deep OS-level mechanics** of how it works. It caters to engineers of all levels, from those just starting out (Level 0) to senior systems architects (Level 1+).

---

## The Problem (Level 0: The Basics)

Imagine you have 50,000 smart thermometers all over the world, and they all send a tiny temperature reading to your server every 10 seconds.

If you write a standard web server (like a basic Flask app) to catch this data and save it to a file, your server will crash almost immediately. Why?
1. **The Network Problem:** Handling 50,000 open connections at once requires a massive amount of memory and "File Descriptors" (the OS's way of tracking open connections).
2. **The CPU Problem:** If every thermometer connects, says "Hello" (TCP/TLS handshake), sends data, and disconnects, your CPU will spend 90% of its time just saying "Hello" and "Goodbye", and only 10% of its time actually saving data.
3. **The Disk Problem:** If 50,000 devices try to write to the same log file on your hard drive at the same time, they get in a traffic jam (Lock Contention). They have to wait in a single-file line.

---

## The Solution (Level 1: The Architecture)

Aegis Edge solves these problems using three distinct architectural strategies.

### 1. Long-Lived Keep-Alive Connections (Solving the CPU Problem)
Instead of devices connecting and disconnecting, they connect **once** and keep the connection open for 30 minutes. When they have data, they just push it down the open pipe.
* *The Result:* CPU overhead for networking drops to virtually zero.

### 2. Asynchronous Event Loops (Solving the Network Problem)
Because we have 50,000 open connections, standard Python threads would freeze. We use Python's `asyncio` (which uses the Linux `epoll` mechanism under the hood). One Python process can monitor 20,000 open pipes simultaneously while sleeping. It only wakes up the exact millisecond a pipe actually pushes data.

### 3. Memory-Mapped Write-Ahead Logs (Solving the Disk Problem)
This is the core magic of Aegis Edge. Instead of using standard `file.write()`, we use **Memory Mapping (`mmap`)**.

When the server starts, it carves out a massive 100MB chunk of the physical SSD using `os.posix_fallocate`. It then "maps" that file directly into the Operating System's RAM (the Page Cache).

When a payload arrives from a thermometer, the Python app writes it directly to that RAM array. **There is no disk I/O.** It writes at the speed of memory. The Linux Kernel quietly flushes that RAM to the physical SSD in the background.

To avoid the "traffic jam" (Lock Contention), we use a **Shared-Nothing** model: Every worker process gets its own dedicated file (e.g., Worker 1 only writes to `wal_1.dat`). No locks, no waiting.

---

## The Deep Dive (Level 1+: Systems Engineering & Math)

If you are deploying this to production, you must tune the Linux Kernel. Standard Linux distributions are tuned for general-purpose web browsing, not massive persistent telemetry.

### The Math: Tuning File Descriptors
Everything in Linux is a file, including network sockets. To hold 50,000 idle TCP connections open, you need over 50,000 File Descriptors (FDs). Standard Linux limits users to 1,024.

We must tune `LimitNOFILE` in systemd or `/etc/security/limits.conf`.
* *Formula:* `Total Devices + Buffer for churn + Internal OS files`
* *For 50k devices:* Set FD limit to `100,000`.

### The Math: Tuning TCP Memory (RAM Exhaustion)
By default, Linux allocates massive memory buffers (up to 131KB) for every open TCP connection, assuming you might want to stream a 4K video. If you have 50,000 idle sockets waiting for a 100-byte JSON payload, you will waste gigabytes of RAM.

We must tune the kernel (`/etc/sysctl.conf`) to shrink these buffers down to 8KB.
```ini
net.ipv4.tcp_rmem = 4096 8192 65536
net.ipv4.tcp_wmem = 4096 8192 65536
```
* *Result:* 50,000 connections × (8KB + 8KB) = ~800MB of RAM. (Down from 13GB!). We save that RAM and give it to our `mmap` Page Cache.

### Crash Recovery: The 8-Byte Header
Because we are writing to memory, what happens if the Python process crashes (SIGKILL)?
We dedicate the first 8 bytes of every WAL file to store the exact byte offset of the last successful write. Because `mmap` writes directly to the OS Page Cache, even if Python dies, the OS retains the data and the 8-byte header. When the worker restarts, it reads those 8 bytes, knows exactly where it left off, and resumes writing perfectly.

---

## Repository Structure

We have provided two complete, production-ready deployment strategies.

### 1. [The Non-Kubernetes Way (`non_k8s_way/`)](./non_k8s_way/)
* **For:** Bare-Metal Servers or standard Cloud VMs (EC2, Droplets).
* **Contains:** The highly-commented Python application (`aegis_edge.py`), systemd service files, and exactly how to tune `/etc/sysctl.conf`.
* **Testing:** Includes a `--dry-run` flag to validate limits without binding ports.

### 2. [The Kubernetes Way (`k8s_way/`)](./k8s_way/)
* **For:** Cloud-native deployments (EKS, GKE, self-hosted K8s).
* **The K8s Challenge:** Kubernetes uses OverlayFS for containers. OverlayFS **does not support** `posix_fallocate`, which breaks our memory-mapping strategy.
* **The Fix:** The provided manifests demonstrate how to map `emptyDir` volumes to bypass OverlayFS, allowing native ext4/xfs disk allocation. It also provides the exact `securityContext` needed to pass the unsafe TCP `sysctls` into the Pod.

---

## How to Get Started

1. Choose your deployment path (`non_k8s_way` or `k8s_way`).
2. Read the specific `README.md` inside that directory.
3. Validate your system limits using the built-in python dry-run command:
   ```bash
   python3 non_k8s_way/aegis_edge.py --dry-run
   ```
