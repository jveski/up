#!/usr/bin/env bash
# smoke.sh - End-to-end smoke test for the 'up' VM manager.
#
# Creates an Alpine VM, SSHes in, curls its own metadata, verifies
# directory volume mount, and deletes the VM.
#
# Must be run as root on a Linux host with KVM support.
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VM_NAME="smoke-test"
API="http://localhost:9090"
ALPINE_VERSION="${ALPINE_VERSION:-3.20}"
ALPINE_RELEASE="${ALPINE_RELEASE:-3.20.3}"
LOG_FILE="/tmp/up-smoke.log"
ROOTFS_MNT="/tmp/smoke-rootfs"
IMG_DIR="/var/lib/up/images"

# ---------------------------------------------------------------------------
# State (set during execution)
# ---------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UP_PID=""
SSH_DIR=""
DISK_DIR=""
SSH_KEY_BACKED_UP=0
VM_IP=""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log()  { printf '\033[0;32m[smoke]\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m[smoke]\033[0m %s\n' "$*"; }
fail() {
  printf '\033[0;31m[smoke] FAIL:\033[0m %s\n' "$*"
  if [ -f "$LOG_FILE" ]; then
    echo "--- daemon log (last 80 lines) ---"
    tail -80 "$LOG_FILE"
    echo "--- end daemon log ---"
  fi
  local serial_log="/tmp/up-serial-${VM_NAME}.log"
  if [ -f "$serial_log" ]; then
    echo "--- VM serial console (last 80 lines) ---"
    tail -80 "$serial_log"
    echo "--- end VM serial console ---"
  else
    echo "--- VM serial console: NOT FOUND at $serial_log ---"
  fi
  echo "--- network diagnostics ---"
  ip addr show upbr0 2>/dev/null || echo "bridge upbr0 not found"
  ip link show uptap0 2>/dev/null || echo "uptap0 not found"
  iptables -L FORWARD -n -v 2>/dev/null | head -20 || true
  cat /proc/sys/net/bridge/bridge-nf-call-iptables 2>/dev/null || echo "bridge-nf-call-iptables: N/A"
  # Try pinging the VM to test basic connectivity
  ping -c 2 -W 2 "${VM_IP:-10.10.10.10}" 2>&1 || true
  # Show ARP table for the bridge subnet
  arp -n 2>/dev/null | grep 10.10.10 || echo "no ARP entries for 10.10.10.x"
  echo "--- end network diagnostics ---"
  exit 1
}

# ---------------------------------------------------------------------------
# Cleanup (runs on EXIT)
# ---------------------------------------------------------------------------

cleanup() {
  set +e
  log "cleaning up..."

  # Stop daemon (triggers graceful shutdown: kills VMs, removes bridge, etc.)
  if [ -n "${UP_PID:-}" ] && kill -0 "$UP_PID" 2>/dev/null; then
    kill -TERM "$UP_PID"
    wait "$UP_PID" 2>/dev/null
  fi

  # Restore original SSH public key if we backed one up
  if [ "$SSH_KEY_BACKED_UP" = "1" ] && [ -n "${SSH_DIR:-}" ] && [ -f "$SSH_DIR/id_ed25519.pub.backup" ]; then
    cp "$SSH_DIR/id_ed25519.pub.backup" /root/.ssh/id_ed25519.pub
  elif [ -n "${SSH_DIR:-}" ] && [ -f /root/.ssh/id_ed25519.pub.smoke ]; then
    rm -f /root/.ssh/id_ed25519.pub
  fi
  rm -f /root/.ssh/id_ed25519.pub.smoke

  # Remove temp dirs
  [ -n "${SSH_DIR:-}" ] && rm -rf "$SSH_DIR"
  [ -n "${DISK_DIR:-}" ] && rm -rf "$DISK_DIR"

  # Clean up any leftover image-build mounts
  for mp in "$ROOTFS_MNT/dev" "$ROOTFS_MNT/proc" "$ROOTFS_MNT/sys" "$ROOTFS_MNT"; do
    mountpoint -q "$mp" 2>/dev/null && umount -l "$mp" 2>/dev/null
  done
  rm -rf "$ROOTFS_MNT"
  rm -f /tmp/smoke-rootfs.img

  rm -f "$LOG_FILE"
  rm -f /tmp/up-serial-*.log
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Prerequisites
# ---------------------------------------------------------------------------

check_prereqs() {
  log "checking prerequisites..."

  [ "$(id -u)" -eq 0 ] || fail "must run as root"

  for cmd in qemu-system-x86_64 qemu-img mkfs.ext4 curl ssh jq go; do
    command -v "$cmd" >/dev/null || fail "missing required command: $cmd"
  done

  [ -e /dev/kvm ] || fail "/dev/kvm not available — KVM is required"

  if curl -sf "$API/vm" >/dev/null 2>&1; then
    fail "port 9090 already in use — is another instance of 'up' running?"
  fi
}

# ---------------------------------------------------------------------------
# Step 2: Build
# ---------------------------------------------------------------------------

build() {
  log "building up..."
  (cd "$REPO_ROOT" && go build -o up .)
}

# ---------------------------------------------------------------------------
# Step 3: SSH key
# ---------------------------------------------------------------------------

setup_ssh() {
  log "generating ephemeral SSH keypair..."
  SSH_DIR=$(mktemp -d)
  ssh-keygen -t ed25519 -f "$SSH_DIR/id_ed25519" -N "" -q

  mkdir -p /root/.ssh
  if [ -f /root/.ssh/id_ed25519.pub ]; then
    cp /root/.ssh/id_ed25519.pub "$SSH_DIR/id_ed25519.pub.backup"
    SSH_KEY_BACKED_UP=1
  fi

  cp "$SSH_DIR/id_ed25519.pub" /root/.ssh/id_ed25519.pub
  # Marker so cleanup knows it was us
  touch /root/.ssh/id_ed25519.pub.smoke
}

# ---------------------------------------------------------------------------
# Step 4: Build Alpine image
# ---------------------------------------------------------------------------

build_image() {
  if [ -f "$IMG_DIR/alpine.qcow2" ] && [ -f "$IMG_DIR/alpine.vmlinuz" ] && [ -f "$IMG_DIR/alpine.initrd" ]; then
    log "Alpine image already exists — skipping build"
    return
  fi

  log "building Alpine image (this takes a minute)..."
  mkdir -p "$IMG_DIR"

  local mirror="https://dl-cdn.alpinelinux.org/alpine"
  local tarball="alpine-minirootfs-${ALPINE_RELEASE}-x86_64.tar.gz"

  # Download minirootfs
  if [ ! -f "/tmp/$tarball" ]; then
    log "  downloading Alpine minirootfs ${ALPINE_RELEASE}..."
    curl -fsSL -o "/tmp/$tarball" "${mirror}/v${ALPINE_VERSION}/releases/x86_64/${tarball}" \
      || fail "failed to download Alpine minirootfs — check ALPINE_VERSION/ALPINE_RELEASE"
  fi

  # Create raw disk image
  local raw_img="/tmp/smoke-rootfs.img"
  truncate -s 1G "$raw_img"
  mkfs.ext4 -F "$raw_img" >/dev/null 2>&1

  # Mount and populate
  mkdir -p "$ROOTFS_MNT"
  mount -o loop "$raw_img" "$ROOTFS_MNT"
  tar xzf "/tmp/$tarball" -C "$ROOTFS_MNT"

  # Configure APK repositories
  mkdir -p "$ROOTFS_MNT/etc/apk"
  cat > "$ROOTFS_MNT/etc/apk/repositories" <<EOF
${mirror}/v${ALPINE_VERSION}/main
${mirror}/v${ALPINE_VERSION}/community
EOF
  echo "nameserver 8.8.8.8" > "$ROOTFS_MNT/etc/resolv.conf"

  # Bind mounts for chroot
  mount -t proc proc "$ROOTFS_MNT/proc"
  mount -t sysfs sys "$ROOTFS_MNT/sys"
  mount --bind /dev "$ROOTFS_MNT/dev"

  # Install packages
  log "  installing packages in chroot..."
  chroot "$ROOTFS_MNT" apk add --no-cache openssh linux-virt curl

  # Allow root login via SSH key
  chroot "$ROOTFS_MNT" sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

  # Write custom /sbin/init
  cat > "$ROOTFS_MNT/sbin/init" <<'INITEOF'
#!/bin/sh
# Minimal init for up smoke-test Alpine VMs.

# Mount essential filesystems (may already exist after switch_root)
mountpoint -q /proc || mount -t proc proc /proc
mountpoint -q /sys  || mount -t sysfs sys /sys
mountpoint -q /dev  || mount -t devtmpfs dev /dev

# Ensure root filesystem is read-write (Alpine initramfs may mount it ro)
mount -o remount,rw / 2>/dev/null

mount -t tmpfs tmpfs /tmp  2>/dev/null
mount -t tmpfs tmpfs /run  2>/dev/null
mkdir -p /dev/pts /dev/shm
mount -t devpts devpts /dev/pts 2>/dev/null
mount -t tmpfs  tmpfs  /dev/shm 2>/dev/null

# Load modules for networking and 9p
modprobe virtio_net  2>/dev/null
modprobe 9pnet_virtio 2>/dev/null
modprobe 9p          2>/dev/null

# Parse kernel command line (up.key is last and may contain spaces)
for param in $(cat /proc/cmdline); do
  case "$param" in
    up.name=*) UP_NAME="${param#up.name=}" ;;
    up.ip=*)   UP_IP="${param#up.ip=}" ;;
    up.gw=*)   UP_GW="${param#up.gw=}" ;;
    up.dns=*)  UP_DNS="${param#up.dns=}" ;;
  esac
done
UP_KEY=$(sed 's/.*up\.key=//' /proc/cmdline)

hostname "${UP_NAME:-alpine}"

# Networking
ip link set lo up
ip addr add "${UP_IP}/24" dev eth0
ip link set eth0 up
ip route add default via "$UP_GW"
echo "nameserver $UP_DNS" > /etc/resolv.conf

# SSH
mkdir -p /root/.ssh
echo "$UP_KEY" > /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/authorized_keys
ssh-keygen -A 2>/dev/null
mkdir -p /run/sshd
/usr/sbin/sshd

# Mount 9p virtfs share (tag=disk) if available
mkdir -p /mnt/disk
mount -t 9p -o trans=virtio,version=9p2000.L disk /mnt/disk 2>/dev/null

exec sleep infinity
INITEOF
  chmod +x "$ROOTFS_MNT/sbin/init"

  # Extract kernel + initrd before unmounting
  cp "$ROOTFS_MNT/boot/vmlinuz-virt" "$IMG_DIR/alpine.vmlinuz"
  cp "$ROOTFS_MNT/boot/initramfs-virt" "$IMG_DIR/alpine.initrd"

  # Unmount chroot
  umount "$ROOTFS_MNT/dev"
  umount "$ROOTFS_MNT/proc"
  umount "$ROOTFS_MNT/sys"
  umount "$ROOTFS_MNT"

  # Convert to qcow2
  log "  converting to qcow2..."
  qemu-img convert -f raw -O qcow2 "$raw_img" "$IMG_DIR/alpine.qcow2"
  rm -f "$raw_img"
  rmdir "$ROOTFS_MNT" 2>/dev/null || true

  log "  Alpine image ready"
}

# ---------------------------------------------------------------------------
# Step 5: Start daemon
# ---------------------------------------------------------------------------

start_daemon() {
  log "starting up daemon..."

  # Pre-create bridge so the metadata server (10.10.10.1:9115) can bind
  # immediately; the daemon's reconciler will detect it and skip creation.
  if ! ip link show dev upbr0 >/dev/null 2>&1; then
    ip link add upbr0 type bridge
    ip addr add 10.10.10.1/24 dev upbr0
    ip link set upbr0 up
  fi

  "$REPO_ROOT/up" > "$LOG_FILE" 2>&1 &
  UP_PID=$!

  # Wait for the management API to become reachable
  local retries=30
  while ! curl -sf "$API/vm" >/dev/null 2>&1; do
    if ! kill -0 "$UP_PID" 2>/dev/null; then
      fail "daemon exited unexpectedly"
    fi
    retries=$((retries - 1))
    [ "$retries" -gt 0 ] || fail "API did not become ready in 30 s"
    sleep 1
  done

  log "daemon ready (pid $UP_PID)"
}

# ---------------------------------------------------------------------------
# Step 6: Create VM
# ---------------------------------------------------------------------------

create_vm() {
  log "creating VM with directory mount..."

  DISK_DIR=$(mktemp -d)
  echo "hello from host" > "$DISK_DIR/host-file.txt"

  local response
  response=$(curl -sf -X POST "$API/vm" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${VM_NAME}\",\"image\":\"alpine\",\"disk\":\"${DISK_DIR}\"}") \
    || fail "POST /vm failed"

  local status
  status=$(echo "$response" | jq -r .status)
  [ "$status" = "booting" ] || fail "expected status 'booting', got '$status'"

  VM_IP=$(echo "$response" | jq -r .ip)
  log "VM created: ip=$VM_IP status=$status"
}

# ---------------------------------------------------------------------------
# Step 7: Wait for running
# ---------------------------------------------------------------------------

wait_for_running() {
  log "waiting for VM to reach 'running'..."
  local retries=120
  while true; do
    local status
    status=$(curl -sf "$API/vm" | jq -r ".[] | select(.name==\"${VM_NAME}\") | .status")
    if [ "$status" = "running" ]; then
      log "VM is running"
      return
    fi
    retries=$((retries - 1))
    [ "$retries" -gt 0 ] || fail "VM did not reach 'running' after 120 s (last: $status)"
    sleep 1
  done
}

# ---------------------------------------------------------------------------
# Step 8: SSH
# ---------------------------------------------------------------------------

ssh_cmd() {
  ssh -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -o ConnectTimeout=10 \
      -o BatchMode=yes \
      -o LogLevel=ERROR \
      -o IdentitiesOnly=yes \
      -i "$SSH_DIR/id_ed25519" \
      root@"$VM_IP" "$@"
}

test_ssh() {
  log "testing SSH..."
  local retries=15
  while true; do
    if ssh_cmd "echo ssh-ok" 2>/dev/null | grep -q "ssh-ok"; then
      log "SSH works"
      return
    fi
    retries=$((retries - 1))
    [ "$retries" -gt 0 ] || fail "SSH did not succeed after retries"
    sleep 2
  done
}

# ---------------------------------------------------------------------------
# Step 9: Metadata
# ---------------------------------------------------------------------------

test_metadata() {
  log "testing metadata (curl from inside VM)..."

  local meta
  meta=$(ssh_cmd "curl -sf http://10.10.10.1:9115/" 2>/dev/null) \
    || fail "curl metadata from inside VM failed"

  local name status
  name=$(echo "$meta" | jq -r .name)
  status=$(echo "$meta" | jq -r .status)

  [ "$name" = "$VM_NAME" ]  || fail "metadata name: expected '$VM_NAME', got '$name'"
  [ "$status" = "running" ] || fail "metadata status: expected 'running', got '$status'"

  log "metadata OK ($meta)"
}

# ---------------------------------------------------------------------------
# Step 10: Directory volume mount
# ---------------------------------------------------------------------------

test_disk_mount() {
  log "testing directory volume mount..."

  # Read host-created file from guest
  local content
  content=$(ssh_cmd "cat /mnt/disk/host-file.txt" 2>/dev/null) \
    || fail "could not read host file from inside VM"
  [ "$content" = "hello from host" ] \
    || fail "host file mismatch: expected 'hello from host', got '$content'"

  # Write from guest, read on host
  ssh_cmd "echo 'hello from guest' > /mnt/disk/guest-file.txt" 2>/dev/null \
    || fail "could not write file from inside VM"
  local host_content
  host_content=$(cat "$DISK_DIR/guest-file.txt") \
    || fail "guest-written file not found on host"
  [ "$host_content" = "hello from guest" ] \
    || fail "guest file mismatch: expected 'hello from guest', got '$host_content'"

  log "directory volume mount works"
}

# ---------------------------------------------------------------------------
# Step 11: Delete VM
# ---------------------------------------------------------------------------

delete_vm() {
  log "deleting VM..."
  curl -sf -X DELETE "$API/vm/${VM_NAME}" >/dev/null \
    || fail "DELETE /vm/$VM_NAME failed"

  local retries=30
  while true; do
    local count
    count=$(curl -sf "$API/vm" | jq 'length')
    if [ "$count" = "0" ]; then
      log "VM deleted"
      return
    fi
    retries=$((retries - 1))
    [ "$retries" -gt 0 ] || fail "VM not fully destroyed after 30 s"
    sleep 1
  done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

check_prereqs
build
setup_ssh
build_image
start_daemon
create_vm
wait_for_running
test_ssh
test_metadata
test_disk_mount
delete_vm

log "all smoke tests passed!"
