# Aegis Edge - Kubernetes Deployment (K8s)

This directory contains the deployment strategy for running the Aegis Edge collector inside a Kubernetes cluster.

## Architectural Challenges Addressed
1. **OverlayFS Limitation**: Kubernetes containers use OverlayFS, which does not support `posix_fallocate`. The `deployment.yaml` fixes this by mounting an `emptyDir` volume (which maps to the underlying node's native ext4/xfs filesystem).
2. **Page Cache Accounting**: Data written to the `mmap` WAL sits in the OS Page Cache. Kubernetes cgroups count this against the Pod's memory limit. We set resources to `5Gi` / `8Gi` to prevent the Pod from being OOMKilled before the disk flushes.
3. **TCP Memory Tuning**: Long-lived connections exhaust RAM. The `deployment.yaml` sets specific `sysctls` to shrink TCP buffers.

## Prerequisites: Unsafe Sysctls
Because `net.ipv4.tcp_rmem`, `net.ipv4.tcp_wmem`, and `net.core.somaxconn` are considered "Unsafe Sysctls" in Kubernetes, they are disabled by default.

Your cluster administrator **must** configure the underlying Node Kubelets to allow them:
```bash
--allowed-unsafe-sysctls=net.ipv4.tcp_*,net.core.somaxconn
```
If you deploy this without allowing the sysctls, the Pods will fail to start.

## Deployment & Dry Run

You can validate the manifests locally without applying them to the cluster by running a client-side dry run.

```bash
kubectl apply -f configmap.yaml --dry-run=client
kubectl apply -f deployment.yaml --dry-run=client
kubectl apply -f service.yaml --dry-run=client
```

If the syntax is correct, you will see `created (dry run)`.

To actually deploy:
```bash
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```
