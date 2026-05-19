# Aegis Edge - Bare Metal / VM Deployment (Non-K8s)

This directory contains the deployment strategy for running the Aegis Edge collector directly on a Linux Virtual Machine or bare-metal server.

## Overview
Because this application is designed to ingest massive amounts of telemetry using long-lived TCP connections and `mmap` Write-Ahead Logs, standard Linux limits will cause it to crash under load.

## System Tuning Prerequisites

### 1. Tune the Kernel (`/etc/sysctl.conf`)
You must apply these settings to prevent RAM exhaustion from idle TCP sockets and allow massive file descriptor allocation.

Add to `/etc/sysctl.conf`:
```ini
fs.file-max = 200000
net.ipv4.tcp_rmem = 4096 8192 65536
net.ipv4.tcp_wmem = 4096 8192 65536
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.core.somaxconn = 65535
vm.max_map_count = 262144
vm.dirty_background_ratio = 10
vm.dirty_ratio = 40
```
Apply with: `sudo sysctl -p`

### 2. Service Management
The `aegis-edge.service` file contains the critical `LimitNOFILE=100000` directive.
1. Place the Python script at `/opt/aegis/aegis_edge.py`.
2. Copy `aegis-edge.service` to `/etc/systemd/system/`.
3. Run:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable aegis-edge
   sudo systemctl start aegis-edge
   ```

## Testing & Dry Run

You can verify the code syntax and ensure it doesn't crash on startup by running it in `--dry-run` mode. This mode does not write any files to disk or bind any actual network ports.

```bash
python3 aegis_edge.py --dry-run
```

Expected output:
```
[INFO] System limits verified. Max open files: ...
[INFO] [DRY-RUN] Would create mmap WAL at /var/log/aegis_edge/wal_worker_XXXX.dat (100MB)
[INFO] [DRY-RUN] Server would listen on 0.0.0.0:8888
[INFO] [DRY-RUN] Dry run completed successfully.
```
