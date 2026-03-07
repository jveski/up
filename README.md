# up

A lightweight Linux VM manager built on QEMU/KVM. Runs as a daemon and exposes a simple HTTP API to create, list, and destroy VMs.

## Requirements

- Linux with KVM (`/dev/kvm`)
- QEMU (`qemu-system-x86_64`, `qemu-img`)
- An SSH public key in `/root/.ssh/` (the daemon injects it into every VM)

## Build

```
go build -o up .
```

## Usage

Start the daemon as root:

```
sudo ./up
```

This creates a bridge (`upbr0` at `10.10.10.1/24`), starts the management API on `:9090`, and begins reconciling VMs.

### Create a VM

```
curl -X POST http://localhost:9090/vm \
  -H 'Content-Type: application/json' \
  -d '{"name": "myvm", "image": "alpine", "disk": "/path/to/shared/dir"}'
```

- **name** — unique VM name (`[a-z0-9-]+`)
- **image** — name of a boot image in `/var/lib/up/images/`
- **disk** — absolute path to a host directory, mounted into the VM at `/mnt/disk` via 9p

### List VMs

```
curl http://localhost:9090/vm
```

### Delete a VM

```
curl -X DELETE http://localhost:9090/vm/myvm
```

### Metadata (from inside a VM)

VMs can query their own metadata from the host:

```
curl http://10.10.10.1:9115/
```

## Preparing Boot Images

Images live in `/var/lib/up/images/` and consist of three files per image:

```
/var/lib/up/images/<name>.qcow2      # Root filesystem
/var/lib/up/images/<name>.vmlinuz     # Kernel
/var/lib/up/images/<name>.initrd      # Initramfs
```

The VM boots via QEMU direct kernel boot. The init system inside the image should parse `up.*` kernel command-line parameters to configure networking and SSH:

| Parameter | Description |
|-----------|-------------|
| `up.name` | VM name |
| `up.ip` | Assigned IP address |
| `up.gw` | Gateway (bridge IP) |
| `up.dns` | DNS server |
| `up.key` | SSH public key |

See `hack/smoke.sh` for a complete example that builds an Alpine Linux image with a minimal init script.
