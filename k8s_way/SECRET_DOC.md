# Secret Document: Kubernetes Way (k8s_way)

This document is intended to break down the thought process, use cases, and technical intuition behind the Kubernetes deployment choices for Aegis Edge, suitable for engineers from Level 0 to Level 1+.

## The Intuition & Narrative: Why these K8s Choices?

When deploying high-throughput, bare-metal-optimized code (like Aegis Edge) to Kubernetes, we run into containerization abstractions that fight our performance goals. Our narrative here is "bypassing the container abstractions to touch the raw metal."

### 1. Bypassing OverlayFS with `emptyDir`
**The Story:** Kubernetes uses OverlayFS to build container filesystems layer-by-layer. However, OverlayFS blocks `os.posix_fallocate`, which is the exact system call we use to instantly carve out physical disk space without fragmentation. If we don't fix this, our `mmap` Write-Ahead Log (WAL) would either fail or become extremely slow due to standard file truncation.
**The Fix:** In `deployment.yaml`, we mount an `emptyDir` volume at `/var/log/aegis_edge`. `emptyDir` bypasses OverlayFS entirely and maps directly to the Kubernetes node's underlying ext4 or xfs filesystem, allowing `posix_fallocate` to run natively.

### 2. Taming the Network: Unsafe TCP Sysctls
**The Story:** We expect 50,000 devices to maintain open, long-lived (Keep-Alive) connections. A default Linux kernel assumes every connection might download a large file, so it allocates massive memory buffers (up to 131KB) per connection. 50,000 connections * 131KB = ~6.5GB of RAM *just for idle connections*.
**The Fix:** We pass `securityContext.sysctls` into the pod to force `net.ipv4.tcp_rmem` and `net.ipv4.tcp_wmem` to `4096 8192 65536`. This shrinks the buffer down to 8KB per connection. Note: Because Kubernetes considers network sysctls "unsafe," the cluster administrator must explicitly allow them on the nodes.

### 3. Page Cache Accounting vs cgroups
**The Story:** When our Python code writes to the `mmap` file, it's actually writing to the OS Page Cache (RAM). The Linux kernel will eventually flush it to disk. However, Kubernetes tracks memory usage using cgroups. The RAM used by the Page Cache *counts against the container's memory limit*. If the container limit is too low, writing large amounts of data to the WAL will cause the container to exceed its quota and get OOMKilled (Out Of Memory Killed) before the kernel even flushes it to disk!
**The Fix:** We set explicit resource requests (`5Gi`) and limits (`8Gi`) to ensure the pod has enough headroom for the OS Page Cache to breathe without triggering an OOM kill.

---

## Hardware Quantification & Load Scaling

So, what load can this deployment handle, and why did we choose `8Gi` of memory limit?

### The Metrics Breakdown:
* **Connections:** Target is 50,000 simultaneous, long-lived device connections.
* **Network RAM Cost:** Thanks to our sysctl tuning (8KB read + 8KB write per connection), 50,000 connections × 16KB = **~800MB of RAM**.
* **WAL (Write-Ahead Log) Size:** Each worker allocates a 100MB WAL file via `mmap`. With 4 replicas/workers, that is 400MB of physical disk, mapped into RAM.
* **The Math:**
  * 800MB (Network Buffers) + 400MB (mmap WAL Page Cache) + ~100MB (Python application overhead) = **~1.3GB of active RAM usage** under optimal conditions.
* **Why 8Gi Limit?**
  * We requested `5Gi` and limited to `8Gi`. This provides a massive buffer. If the physical disk I/O slows down (e.g., EBS volume throttling on AWS), the OS Page Cache will swell as writes pile up in memory waiting to flush. The 8Gi limit gives the system ~6.5GB of "buffer space" to hold telemetry data in RAM until the disk catches up, completely preventing dropped packets or OOM crashes during disk latency spikes.

In short, a single Pod configured this way can easily handle 50,000 continuous streams, using less than 1.5GB of baseline memory, with a massive safety net for disk latency.
