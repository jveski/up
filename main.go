package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	BridgeName   = "upbr0"
	BridgeIP     = "10.10.10.1"
	BridgeCIDR   = "10.10.10.1/24"
	BridgeSubnet = "10.10.10.0/24"
	GuestIPBase  = 10
	MaxSlots     = 245
	APIAddr      = ":9090"
	MetadataAddr = "10.10.10.1:9115"

	SSHTimeout              = 2 * time.Second
	SSHPollRequeue          = 500 * time.Millisecond
	ReconcileTickInterval   = 30 * time.Second
	InitialBackoff          = 1 * time.Second
	MaxBackoff              = 30 * time.Second
	BackoffFactor           = 2.0
	MaxConcurrentReconciles = 4
)

// ---------------------------------------------------------------------------
// Data Types
// ---------------------------------------------------------------------------

type VMEntry struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	Slot      int    `json:"slot"`
	IP        string `json:"ip"`
	Status    string `json:"status"`
	Disk      string `json:"disk"`
	PID       int    `json:"pid"`
	TAPDevice string `json:"tap"`
	Overlay   string `json:"overlay"`
	CreatedAt string `json:"created_at"`

	ConsecutiveFailures int    `json:"consecutive_failures"`
	NextReconcileAt     string `json:"next_reconcile_at"`
}

type State struct {
	VMs map[string]*VMEntry `json:"vms"`
}

type CreateRequest struct {
	Name  string `json:"name"`
	Image string `json:"image"`
	Disk  string `json:"disk"`
}

type Result struct {
	Requeue      bool
	RequeueAfter time.Duration
}

var (
	StateFilePath = "/var/lib/up/state.json"
	ImageDir      = "/var/lib/up/images"
	OverlayDir    = "/var/lib/up/overlays"

	state     State
	stateMu   sync.Mutex
	sshPubKey string

	nameRe = regexp.MustCompile(`^[a-z0-9-]+$`)
)

// ---------------------------------------------------------------------------
// JSON Helpers
// ---------------------------------------------------------------------------

func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	jsonResponse(w, status, map[string]string{"error": msg})
}

// ---------------------------------------------------------------------------
// State File Operations
// ---------------------------------------------------------------------------

func loadState() {
	data, err := os.ReadFile(StateFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			state = State{VMs: map[string]*VMEntry{}}
			return
		}
		slog.Error("failed to read state file", "err", err)
		os.Exit(1)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		slog.Error("failed to parse state file", "err", err)
		os.Exit(1)
	}
	if state.VMs == nil {
		state.VMs = map[string]*VMEntry{}
	}
}

func saveState() {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		slog.Error("failed to marshal state", "err", err)
		return
	}
	tmp := StateFilePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		slog.Error("failed to write temp state file", "err", err)
		return
	}
	if err := os.Rename(tmp, StateFilePath); err != nil {
		slog.Error("failed to rename state file", "err", err)
	}
}

// ---------------------------------------------------------------------------
// Slot Allocator
// ---------------------------------------------------------------------------

func allocateSlot() (int, error) {
	used := make(map[int]bool)
	for _, vm := range state.VMs {
		used[vm.Slot] = true
	}
	for i := range MaxSlots {
		if !used[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no free slots")
}

func slotToIP(slot int) string {
	return fmt.Sprintf("10.10.10.%d", GuestIPBase+slot)
}

func slotToTAP(slot int) string {
	return fmt.Sprintf("uptap%d", slot)
}

func slotToOverlay(name string) string {
	return filepath.Join(OverlayDir, name+".qcow2")
}

// ---------------------------------------------------------------------------
// Shell Helper
// ---------------------------------------------------------------------------

func run(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.Error("command failed", "cmd", name, "args", args, "output", string(out), "err", err)
	}
	return out, err
}

// ---------------------------------------------------------------------------
// Bridge Reconciler
// ---------------------------------------------------------------------------

func reconcileBridge() (Result, error) {
	log := slog.With("component", "bridge")

	// 1. Bridge exists?
	if _, err := run("ip", "link", "show", "dev", BridgeName); err != nil {
		log.Info("creating bridge")
		if _, err := run("ip", "link", "add", BridgeName, "type", "bridge"); err != nil {
			return Result{}, fmt.Errorf("create bridge: %w", err)
		}
		if _, err := run("ip", "addr", "add", BridgeCIDR, "dev", BridgeName); err != nil {
			return Result{}, fmt.Errorf("assign bridge IP: %w", err)
		}
		if _, err := run("ip", "link", "set", BridgeName, "up"); err != nil {
			return Result{}, fmt.Errorf("bring up bridge: %w", err)
		}
	} else {
		// 2. Bridge has correct IP?
		out, err := run("ip", "-4", "addr", "show", "dev", BridgeName)
		if err != nil {
			return Result{}, fmt.Errorf("check bridge IP: %w", err)
		}
		if !strings.Contains(string(out), BridgeCIDR) {
			log.Info("fixing bridge IP")
			run("ip", "addr", "flush", "dev", BridgeName)
			if _, err := run("ip", "addr", "add", BridgeCIDR, "dev", BridgeName); err != nil {
				return Result{}, fmt.Errorf("reassign bridge IP: %w", err)
			}
		}
	}

	// 3. IP forwarding
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0644); err != nil {
		return Result{}, fmt.Errorf("enable IP forwarding: %w", err)
	}

	// 4. Disable bridge-nf-call-iptables so bridged frames (host↔VM) do not
	//    traverse the iptables FORWARD chain.  Docker sets the FORWARD policy
	//    to DROP, which otherwise blocks all bridge traffic.
	if err := os.WriteFile("/proc/sys/net/bridge/bridge-nf-call-iptables", []byte("0"), 0644); err != nil {
		// br_netfilter module may not be loaded; that is fine — bridged
		// traffic won't traverse iptables anyway in that case.
		log.Info("could not disable bridge-nf-call-iptables (module may not be loaded)", "err", err)
	}

	// 5. FORWARD rules — belt-and-suspenders in case bridge-nf-call-iptables
	//    cannot be disabled or is re-enabled later.
	if _, err := run("iptables", "-C", "FORWARD",
		"-i", BridgeName, "-o", BridgeName, "-j", "ACCEPT"); err != nil {
		log.Info("adding FORWARD rules for bridge")
		if _, err := run("iptables", "-I", "FORWARD",
			"-i", BridgeName, "-o", BridgeName, "-j", "ACCEPT"); err != nil {
			return Result{}, fmt.Errorf("add FORWARD intra-bridge: %w", err)
		}
	}
	if _, err := run("iptables", "-C", "FORWARD",
		"-i", BridgeName, "!", "-o", BridgeName, "-j", "ACCEPT"); err != nil {
		if _, err := run("iptables", "-I", "FORWARD",
			"-i", BridgeName, "!", "-o", BridgeName, "-j", "ACCEPT"); err != nil {
			return Result{}, fmt.Errorf("add FORWARD outbound: %w", err)
		}
	}
	if _, err := run("iptables", "-C", "FORWARD",
		"-o", BridgeName, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"); err != nil {
		if _, err := run("iptables", "-I", "FORWARD",
			"-o", BridgeName, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT"); err != nil {
			return Result{}, fmt.Errorf("add FORWARD return: %w", err)
		}
	}

	// 6. MASQUERADE rule
	if _, err := run("iptables", "-t", "nat", "-C", "POSTROUTING",
		"-s", BridgeSubnet, "!", "-d", BridgeSubnet, "-j", "MASQUERADE"); err != nil {
		log.Info("adding MASQUERADE rule")
		if _, err := run("iptables", "-t", "nat", "-A", "POSTROUTING",
			"-s", BridgeSubnet, "!", "-d", BridgeSubnet, "-j", "MASQUERADE"); err != nil {
			return Result{}, fmt.Errorf("add MASQUERADE: %w", err)
		}
	}

	return Result{}, nil
}

// ---------------------------------------------------------------------------
// TAP Device Operations
// ---------------------------------------------------------------------------

func tapExists(tap string) bool {
	_, err := run("ip", "link", "show", "dev", tap)
	return err == nil
}

func createTAP(tap string) error {
	if tapExists(tap) {
		return nil
	}
	log := slog.With("component", "tap", "device", tap)
	log.Info("creating TAP device")
	if _, err := run("ip", "tuntap", "add", "dev", tap, "mode", "tap"); err != nil {
		return fmt.Errorf("create TAP %s: %w", tap, err)
	}
	if _, err := run("ip", "link", "set", tap, "master", BridgeName); err != nil {
		return fmt.Errorf("attach TAP %s to bridge: %w", tap, err)
	}
	if _, err := run("ip", "link", "set", tap, "up"); err != nil {
		return fmt.Errorf("bring up TAP %s: %w", tap, err)
	}
	return nil
}

func destroyTAP(tap string) error {
	if !tapExists(tap) {
		return nil
	}
	log := slog.With("component", "tap", "device", tap)
	log.Info("destroying TAP device")
	if _, err := run("ip", "link", "del", tap); err != nil {
		return fmt.Errorf("delete TAP %s: %w", tap, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Overlay Operations
// ---------------------------------------------------------------------------

func createOverlay(baseImage, overlayPath string) error {
	if _, err := os.Stat(overlayPath); err == nil {
		return nil
	}
	basePath := filepath.Join(ImageDir, baseImage+".qcow2")
	log := slog.With("component", "overlay", "base", basePath, "overlay", overlayPath)
	log.Info("creating overlay")
	if _, err := run("qemu-img", "create", "-f", "qcow2", "-b", basePath, "-F", "qcow2", overlayPath); err != nil {
		return fmt.Errorf("create overlay: %w", err)
	}
	return nil
}

func destroyOverlay(overlayPath string) error {
	log := slog.With("component", "overlay", "overlay", overlayPath)
	log.Info("destroying overlay")
	if err := os.Remove(overlayPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove overlay: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// QEMU Process Management
// ---------------------------------------------------------------------------

func startQEMU(vm *VMEntry, signals *signalQueue) error {
	log := slog.With("component", "vm", "vm", vm.Name)
	log.Info("starting QEMU")

	mac := fmt.Sprintf("52:54:00:00:00:%02x", vm.Slot)
	kernelPath := filepath.Join(ImageDir, vm.Image+".vmlinuz")
	initrdPath := filepath.Join(ImageDir, vm.Image+".initrd")

	appendLine := fmt.Sprintf(
		"root=/dev/vda rw rootfstype=ext4 rootflags=rw console=ttyS0 up.name=%s up.slot=%d up.ip=%s up.gw=%s up.dns=%s up.key=%s",
		vm.Name, vm.Slot, vm.IP, BridgeIP, BridgeIP, sshPubKey,
	)

	serialLogPath := filepath.Join("/tmp", "up-serial-"+vm.Name+".log")

	args := []string{
		"-name", vm.Name,
		"-machine", "q35,accel=kvm",
		"-cpu", "host",
		"-m", "2048",
		"-smp", "2",
		"-display", "none",
		"-serial", "file:" + serialLogPath,
		"-monitor", "none",
		"-drive", fmt.Sprintf("file=%s,format=qcow2,if=virtio", vm.Overlay),
		"-netdev", fmt.Sprintf("tap,id=net0,ifname=%s,script=no,downscript=no", vm.TAPDevice),
		"-device", fmt.Sprintf("virtio-net-pci,netdev=net0,mac=%s", mac),
		"-kernel", kernelPath,
		"-initrd", initrdPath,
		"-append", appendLine,
	}

	if vm.Disk != "" {
		args = append(args,
			"-virtfs", fmt.Sprintf("local,path=%s,mount_tag=disk,security_model=none,id=disk0", vm.Disk),
		)
	}

	cmd := exec.Command("qemu-system-x86_64", args...)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start QEMU: %w", err)
	}

	vm.PID = cmd.Process.Pid

	stateMu.Lock()
	saveState()
	stateMu.Unlock()

	vmName := vm.Name
	go func() {
		cmd.Wait()
		slog.With("component", "vm", "vm", vmName).Info("QEMU process exited")
		signals.Notify(vmName)
	}()

	return nil
}

func killQEMU(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) || strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("SIGTERM: %w", err)
	}

	done := make(chan struct{})
	go func() {
		proc.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(3 * time.Second):
		if err := proc.Signal(syscall.SIGKILL); err != nil {
			if errors.Is(err, os.ErrProcessDone) || strings.Contains(err.Error(), "no such process") {
				return nil
			}
			return fmt.Errorf("SIGKILL: %w", err)
		}
		<-done
		return nil
	}
}

func isQEMUAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// ---------------------------------------------------------------------------
// SSH Probe
// ---------------------------------------------------------------------------

func probeSSH(ip string) bool {
	conn, err := net.DialTimeout("tcp", ip+":22", SSHTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ---------------------------------------------------------------------------
// VM Reconciler
// ---------------------------------------------------------------------------

func reconcileVM(name string, signals *signalQueue) (Result, error) {
	log := slog.With("component", "vm", "vm", name)

	stateMu.Lock()
	vm, ok := state.VMs[name]
	if !ok {
		stateMu.Unlock()
		return Result{}, nil
	}
	status := vm.Status
	stateMu.Unlock()

	switch status {
	case "booting":
		return reconcileBooting(vm, log, signals)
	case "running":
		return reconcileRunning(vm, log, signals)
	case "destroying":
		return reconcileDestroying(vm, name, log)
	default:
		return Result{}, fmt.Errorf("unknown status: %s", status)
	}
}

func reconcileBooting(vm *VMEntry, log *slog.Logger, signals *signalQueue) (Result, error) {
	// Overlay missing?
	if _, err := os.Stat(vm.Overlay); err != nil {
		log.Info("creating overlay")
		if err := createOverlay(vm.Image, vm.Overlay); err != nil {
			return Result{}, err
		}
		return Result{Requeue: true}, nil
	}

	// TAP missing?
	if !tapExists(vm.TAPDevice) {
		log.Info("creating TAP")
		if err := createTAP(vm.TAPDevice); err != nil {
			return Result{}, err
		}
		return Result{Requeue: true}, nil
	}

	// QEMU not alive?
	if !isQEMUAlive(vm.PID) {
		log.Info("starting QEMU")
		if err := startQEMU(vm, signals); err != nil {
			return Result{}, err
		}
		return Result{RequeueAfter: SSHPollRequeue}, nil
	}

	// QEMU alive, SSH reachable?
	if probeSSH(vm.IP) {
		log.Info("VM is running")
		stateMu.Lock()
		vm.Status = "running"
		saveState()
		stateMu.Unlock()
		return Result{}, nil
	}

	// QEMU alive, SSH not reachable — wait
	return Result{RequeueAfter: SSHPollRequeue}, nil
}

func reconcileRunning(vm *VMEntry, log *slog.Logger, signals *signalQueue) (Result, error) {
	if !isQEMUAlive(vm.PID) {
		log.Info("QEMU died, resetting to booting")
		stateMu.Lock()
		vm.Status = "booting"
		vm.PID = 0
		saveState()
		stateMu.Unlock()
		destroyOverlay(vm.Overlay)
		destroyTAP(vm.TAPDevice)
		return Result{Requeue: true}, nil
	}

	if !tapExists(vm.TAPDevice) {
		log.Info("TAP missing, killing QEMU and resetting")
		killQEMU(vm.PID)
		stateMu.Lock()
		vm.Status = "booting"
		vm.PID = 0
		saveState()
		stateMu.Unlock()
		return Result{Requeue: true}, nil
	}

	return Result{}, nil
}

func reconcileDestroying(vm *VMEntry, name string, log *slog.Logger) (Result, error) {
	if isQEMUAlive(vm.PID) {
		log.Info("killing QEMU")
		if err := killQEMU(vm.PID); err != nil {
			return Result{}, err
		}
		return Result{Requeue: true}, nil
	}

	if _, err := os.Stat(vm.Overlay); err == nil {
		log.Info("destroying overlay")
		if err := destroyOverlay(vm.Overlay); err != nil {
			return Result{}, err
		}
		return Result{Requeue: true}, nil
	}

	if tapExists(vm.TAPDevice) {
		log.Info("destroying TAP")
		if err := destroyTAP(vm.TAPDevice); err != nil {
			return Result{}, err
		}
		return Result{Requeue: true}, nil
	}

	log.Info("VM fully destroyed")
	stateMu.Lock()
	delete(state.VMs, name)
	saveState()
	stateMu.Unlock()
	return Result{}, nil
}

// ---------------------------------------------------------------------------
// Garbage Collector
// ---------------------------------------------------------------------------

func reconcileGarbage() (Result, error) {
	log := slog.With("component", "gc")

	// 1. Orphan QEMU processes
	out, err := exec.Command("pgrep", "-f", "qemu-system.*-name").CombinedOutput()
	if err == nil {
		pids := strings.Fields(strings.TrimSpace(string(out)))
		for _, pidStr := range pids {
			var pid int
			if _, err := fmt.Sscanf(pidStr, "%d", &pid); err != nil {
				continue
			}
			cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			if err != nil {
				continue
			}
			parts := strings.Split(string(cmdline), "\x00")
			vmName := ""
			for i, p := range parts {
				if p == "-name" && i+1 < len(parts) {
					vmName = parts[i+1]
					break
				}
			}
			if vmName == "" {
				continue
			}
			stateMu.Lock()
			_, known := state.VMs[vmName]
			stateMu.Unlock()
			if !known {
				log.Info("killing orphan QEMU", "pid", pid, "name", vmName)
				killQEMU(pid)
			}
		}
	}

	// 2. Orphan TAP devices
	out, err = exec.Command("ip", "-o", "link", "show", "type", "tun").CombinedOutput()
	if err == nil {
		knownTAPs := make(map[string]bool)
		stateMu.Lock()
		for _, vm := range state.VMs {
			knownTAPs[vm.TAPDevice] = true
		}
		stateMu.Unlock()

		for _, line := range strings.Split(string(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			dev := strings.TrimSuffix(fields[1], ":")
			if !strings.HasPrefix(dev, "uptap") {
				continue
			}
			if !knownTAPs[dev] {
				log.Info("deleting orphan TAP", "device", dev)
				destroyTAP(dev)
			}
		}
	}

	// 3. Orphan overlays
	entries, err := os.ReadDir(OverlayDir)
	if err == nil {
		knownOverlays := make(map[string]bool)
		stateMu.Lock()
		for _, vm := range state.VMs {
			knownOverlays[vm.Overlay] = true
		}
		stateMu.Unlock()

		for _, e := range entries {
			p := filepath.Join(OverlayDir, e.Name())
			if !knownOverlays[p] {
				log.Info("deleting orphan overlay", "path", p)
				destroyOverlay(p)
			}
		}
	}

	return Result{}, nil
}

// ---------------------------------------------------------------------------
// HTTP API Server (:9090)
// ---------------------------------------------------------------------------

func apiRoutes(signals *signalQueue) *http.ServeMux {
	log := slog.With("component", "api")
	mux := http.NewServeMux()

	mux.HandleFunc("POST /vm", func(w http.ResponseWriter, r *http.Request) {
		var req CreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if !nameRe.MatchString(req.Name) {
			jsonError(w, http.StatusBadRequest, "name must match ^[a-z0-9-]+$")
			return
		}
		if req.Image == "" {
			jsonError(w, http.StatusBadRequest, "image is required")
			return
		}
		imagePath := filepath.Join(ImageDir, req.Image+".qcow2")
		if _, err := os.Stat(imagePath); err != nil {
			jsonError(w, http.StatusBadRequest, "image not found")
			return
		}

		stateMu.Lock()
		if _, exists := state.VMs[req.Name]; exists {
			stateMu.Unlock()
			jsonError(w, http.StatusConflict, "VM already exists")
			return
		}
		slot, err := allocateSlot()
		if err != nil {
			stateMu.Unlock()
			jsonError(w, http.StatusServiceUnavailable, "no free slots")
			return
		}
		vm := &VMEntry{
			Name:      req.Name,
			Image:     req.Image,
			Slot:      slot,
			IP:        slotToIP(slot),
			Status:    "booting",
			Disk:      req.Disk,
			TAPDevice: slotToTAP(slot),
			Overlay:   slotToOverlay(req.Name),
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		state.VMs[req.Name] = vm
		saveState()
		stateMu.Unlock()

		log.Info("VM created", "vm", req.Name)
		signals.Notify(req.Name)
		jsonResponse(w, http.StatusCreated, vm)
	})

	mux.HandleFunc("GET /vm", func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		vms := make([]*VMEntry, 0, len(state.VMs))
		for _, vm := range state.VMs {
			vms = append(vms, vm)
		}
		stateMu.Unlock()

		slices.SortFunc(vms, func(a, b *VMEntry) int {
			return strings.Compare(a.Name, b.Name)
		})

		jsonResponse(w, http.StatusOK, vms)
	})

	mux.HandleFunc("DELETE /vm/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		stateMu.Lock()
		vm, exists := state.VMs[name]
		if !exists {
			stateMu.Unlock()
			jsonError(w, http.StatusNotFound, "VM not found")
			return
		}
		vm.Status = "destroying"
		saveState()
		stateMu.Unlock()

		log.Info("VM destruction requested", "vm", name)
		signals.Notify(name)
		jsonResponse(w, http.StatusOK, vm)
	})

	return mux
}

// ---------------------------------------------------------------------------
// Metadata API Server (10.10.10.1:9115)
// ---------------------------------------------------------------------------

func metadataRoutes(signals *signalQueue) *http.ServeMux {
	log := slog.With("component", "meta")
	mux := http.NewServeMux()

	findVMByIP := func(r *http.Request) *VMEntry {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return nil
		}
		stateMu.Lock()
		defer stateMu.Unlock()
		for _, vm := range state.VMs {
			if vm.IP == host {
				return vm
			}
		}
		return nil
	}

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		vm := findVMByIP(r)
		if vm == nil {
			jsonError(w, http.StatusNotFound, "VM not found for source IP")
			return
		}
		created, _ := time.Parse(time.RFC3339, vm.CreatedAt)
		uptime := time.Since(created).Truncate(time.Second).String()
		jsonResponse(w, http.StatusOK, map[string]string{
			"name":   vm.Name,
			"image":  vm.Image,
			"ip":     vm.IP,
			"status": vm.Status,
			"uptime": uptime,
		})
	})

	mux.HandleFunc("POST /destroy", func(w http.ResponseWriter, r *http.Request) {
		vm := findVMByIP(r)
		if vm == nil {
			jsonError(w, http.StatusNotFound, "VM not found for source IP")
			return
		}
		log.Info("self-teardown requested", "vm", vm.Name)
		stateMu.Lock()
		vm.Status = "destroying"
		saveState()
		stateMu.Unlock()
		signals.Notify(vm.Name)
		jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
	})

	return mux
}

// ---------------------------------------------------------------------------
// SSH Key Loading
// ---------------------------------------------------------------------------

func loadSSHKey() {
	paths := []string{
		"/root/.ssh/id_ed25519.pub",
		"/root/.ssh/id_rsa.pub",
		"/root/.ssh/authorized_keys",
	}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		line := strings.TrimSpace(strings.SplitN(string(data), "\n", 2)[0])
		if line != "" {
			sshPubKey = line
			slog.Info("loaded SSH public key", "path", p)
			return
		}
	}
	slog.Error("no SSH public key found")
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// Signal Queue
// ---------------------------------------------------------------------------

type signalQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pending  map[string]bool
	shutdown bool
}

func newSignalQueue() *signalQueue {
	q := &signalQueue{
		pending: make(map[string]bool),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *signalQueue) Notify(key string) {
	q.mu.Lock()
	q.pending[key] = true
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *signalQueue) Get() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.pending) == 0 && !q.shutdown {
		q.cond.Wait()
	}
	if q.shutdown {
		return "", false
	}
	for key := range q.pending {
		delete(q.pending, key)
		return key, true
	}
	return "", false
}

func (q *signalQueue) ShutDown() {
	q.mu.Lock()
	q.shutdown = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// ---------------------------------------------------------------------------
// Reconciliation Engine
// ---------------------------------------------------------------------------

type engine struct {
	signals *signalQueue
}

func notifyAll(q *signalQueue) {
	stateMu.Lock()
	keys := make([]string, 0, len(state.VMs)+2)
	keys = append(keys, "__bridge__")
	for name := range state.VMs {
		keys = append(keys, name)
	}
	keys = append(keys, "__gc__")
	stateMu.Unlock()
	for _, key := range keys {
		q.Notify(key)
	}
}

func (e *engine) run() {
	for range MaxConcurrentReconciles {
		go e.worker()
	}
}

func (e *engine) worker() {
	for {
		key, ok := e.signals.Get()
		if !ok {
			return
		}

		// Check NextReconcileAt
		if key != "__bridge__" && key != "__gc__" {
			stateMu.Lock()
			vm, exists := state.VMs[key]
			if exists && vm.NextReconcileAt != "" {
				nextAt, err := time.Parse(time.RFC3339, vm.NextReconcileAt)
				if err == nil && time.Now().Before(nextAt) {
					delay := time.Until(nextAt)
					stateMu.Unlock()
					signals := e.signals
					time.AfterFunc(delay, func() {
						signals.Notify(key)
					})
					continue
				}
			}
			stateMu.Unlock()
		}

		var result Result
		var err error

		switch key {
		case "__bridge__":
			result, err = reconcileBridge()
		case "__gc__":
			result, err = reconcileGarbage()
		default:
			result, err = reconcileVM(key, e.signals)
		}

		e.handleResult(key, result, err)
	}
}

func (e *engine) handleResult(key string, result Result, err error) {
	if key == "__bridge__" || key == "__gc__" {
		if err != nil {
			slog.Error("reconcile error", "key", key, "err", err)
		}
		return
	}

	stateMu.Lock()
	vm, exists := state.VMs[key]
	if !exists {
		stateMu.Unlock()
		return
	}

	if err != nil {
		vm.ConsecutiveFailures++
		backoff := float64(InitialBackoff) * math.Pow(BackoffFactor, float64(vm.ConsecutiveFailures-1))
		if backoff > float64(MaxBackoff) {
			backoff = float64(MaxBackoff)
		}
		vm.NextReconcileAt = time.Now().Add(time.Duration(backoff)).UTC().Format(time.RFC3339)
		saveState()
		stateMu.Unlock()
		slog.Error("reconcile error", "key", key, "err", err, "failures", vm.ConsecutiveFailures)
		e.signals.Notify(key)
		return
	}

	vm.ConsecutiveFailures = 0

	switch {
	case result.RequeueAfter > 0:
		vm.NextReconcileAt = time.Now().Add(result.RequeueAfter).UTC().Format(time.RFC3339)
		saveState()
		stateMu.Unlock()
		e.signals.Notify(key)
	case result.Requeue:
		vm.NextReconcileAt = ""
		saveState()
		stateMu.Unlock()
		e.signals.Notify(key)
	default:
		vm.NextReconcileAt = ""
		saveState()
		stateMu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Signal Handling and Shutdown
// ---------------------------------------------------------------------------

func shutdown(apiServer, metaServer *http.Server, signals *signalQueue) {
	slog.Info("shutting down")

	signals.ShutDown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	apiServer.Shutdown(ctx)
	metaServer.Shutdown(ctx)

	stateMu.Lock()
	for _, vm := range state.VMs {
		if isQEMUAlive(vm.PID) {
			killQEMU(vm.PID)
		}
		destroyOverlay(vm.Overlay)
		destroyTAP(vm.TAPDevice)
	}
	stateMu.Unlock()

	run("ip", "link", "del", BridgeName)
	run("iptables", "-D", "FORWARD",
		"-i", BridgeName, "-o", BridgeName, "-j", "ACCEPT")
	run("iptables", "-D", "FORWARD",
		"-i", BridgeName, "!", "-o", BridgeName, "-j", "ACCEPT")
	run("iptables", "-D", "FORWARD",
		"-o", BridgeName, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
	run("iptables", "-t", "nat", "-D", "POSTROUTING",
		"-s", BridgeSubnet, "!", "-d", BridgeSubnet, "-j", "MASQUERADE")

	stateMu.Lock()
	state = State{VMs: map[string]*VMEntry{}}
	saveState()
	stateMu.Unlock()
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	if os.Getuid() != 0 {
		slog.Error("must run as root")
		os.Exit(1)
	}

	os.MkdirAll(ImageDir, 0755)
	os.MkdirAll(OverlayDir, 0755)
	os.MkdirAll(filepath.Dir(StateFilePath), 0755)

	loadSSHKey()
	loadState()

	signals := newSignalQueue()
	eng := &engine{signals: signals}
	eng.run()
	notifyAll(signals)

	apiServer := &http.Server{
		Addr:    APIAddr,
		Handler: apiRoutes(signals),
	}
	metaServer := &http.Server{
		Addr:    MetadataAddr,
		Handler: metadataRoutes(signals),
	}

	go func() {
		slog.Info("metadata server listening", "addr", MetadataAddr)
		if err := metaServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metadata server error", "err", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		shutdown(apiServer, metaServer, signals)
		os.Exit(0)
	}()

	// Periodic tick
	go func() {
		ticker := time.NewTicker(ReconcileTickInterval)
		defer ticker.Stop()
		for range ticker.C {
			notifyAll(signals)
		}
	}()

	slog.Info("API server listening", "addr", APIAddr)
	if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("API server error", "err", err)
		os.Exit(1)
	}
}
