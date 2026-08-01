# Ubuntu deployment with a read-only NFS handoff

The simplest two-VM deployment stores Zarr on the orchestrator VM's local disk and exports
only the publication directory to the Django VM over a private network.

## Orchestrator VM

Install NFS and create the publication directory:

```bash
sudo apt update
sudo apt install nfs-kernel-server
sudo install -d -o forecast-orchestrator -g forecast-orchestrator -m 0750 /srv/forecast-public
```

Add one export in `/etc/exports`, replacing the example address with the Django VM's private
address:

```text
/srv/forecast-public 10.0.0.22(ro,sync,no_subtree_check,root_squash)
```

Apply it:

```bash
sudo exportfs -ra
sudo systemctl enable --now nfs-server
```

Permit NFS only from that private address in the host and provider firewalls. Never expose
NFS to the public Internet.

## Django VM

```bash
sudo apt update
sudo apt install nfs-common
sudo mkdir -p /mnt/forecast-public
sudo mount -t nfs4 ORCHESTRATOR_PRIVATE_IP:/srv/forecast-public /mnt/forecast-public
```

For boot-time mounting, add this to `/etc/fstab` with the real private address:

```text
ORCHESTRATOR_PRIVATE_IP:/srv/forecast-public /mnt/forecast-public nfs4 ro,_netdev,nofail,x-systemd.automount 0 0
```

The Django service should order itself after `remote-fs.target`. It reads `current.json`,
resolves `store` below `/mnt/forecast-public`, and opens that immutable directory read-only.
The configured deletion grace gives in-flight readers time to finish after pointer rotation.

## Later S3-compatible publication

The configuration deliberately names the publication backend. A later release can upload an
immutable store prefix to Amazon S3, MinIO, or another compatible service and publish the
small pointer object last. This does not change discovery, ingest, conversion, or validation.

