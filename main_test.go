package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testSetup resets global state and redirects all paths to a temp directory.
func testSetup(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	StateFilePath = filepath.Join(tmp, "state.json")
	ImageDir = filepath.Join(tmp, "images")
	OverlayDir = filepath.Join(tmp, "overlays")
	os.MkdirAll(ImageDir, 0755)
	os.MkdirAll(OverlayDir, 0755)
	stateMu.Lock()
	state = State{VMs: map[string]*VMEntry{}}
	stateMu.Unlock()
}

// addImage creates a fake .qcow2 so image-existence validation passes.
func addImage(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(ImageDir, name+".qcow2"), []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestAPICRUD(t *testing.T) {
	testSetup(t)
	addImage(t, "ubuntu")
	q := newSignalQueue()
	mux := apiRoutes(q)

	// --- Create first VM ---
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/vm",
		strings.NewReader(`{"name":"alpha","image":"ubuntu","disk":"/data"}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("create alpha: %d %s", w.Code, w.Body.String())
	}
	var created VMEntry
	json.NewDecoder(w.Body).Decode(&created)
	if created.Name != "alpha" || created.Image != "ubuntu" || created.Disk != "/data" {
		t.Fatalf("fields: %+v", created)
	}
	if created.Status != "booting" {
		t.Fatalf("status=%q", created.Status)
	}
	if created.IP != "10.10.10.10" || created.TAPDevice != "uptap0" {
		t.Fatalf("computed: ip=%s tap=%s", created.IP, created.TAPDevice)
	}
	if created.Overlay != filepath.Join(OverlayDir, "alpha.qcow2") {
		t.Fatalf("overlay=%s", created.Overlay)
	}
	if created.CreatedAt == "" {
		t.Fatal("created_at empty")
	}

	// --- Create second VM (gets next slot) ---
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/vm",
		strings.NewReader(`{"name":"beta","image":"ubuntu"}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("create beta: %d", w.Code)
	}

	// --- Duplicate rejected ---
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/vm",
		strings.NewReader(`{"name":"alpha","image":"ubuntu"}`)))
	if w.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d", w.Code)
	}

	// --- List returns sorted ---
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/vm", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("list: %d", w.Code)
	}
	var list []VMEntry
	json.NewDecoder(w.Body).Decode(&list)
	if len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "beta" {
		t.Fatalf("list: len=%d", len(list))
	}

	// --- Delete ---
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("DELETE", "/vm/alpha", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("delete: %d", w.Code)
	}
	var del VMEntry
	json.NewDecoder(w.Body).Decode(&del)
	if del.Status != "destroying" {
		t.Fatalf("delete status=%s", del.Status)
	}

	// --- State file round-trip ---
	stateMu.Lock()
	state = State{VMs: map[string]*VMEntry{}}
	stateMu.Unlock()
	loadState()
	stateMu.Lock()
	defer stateMu.Unlock()
	if len(state.VMs) != 2 {
		t.Fatalf("reload: %d vms want 2", len(state.VMs))
	}
	if state.VMs["alpha"].Status != "destroying" {
		t.Fatalf("reload alpha status=%s", state.VMs["alpha"].Status)
	}
	if state.VMs["beta"].Status != "booting" {
		t.Fatalf("reload beta status=%s", state.VMs["beta"].Status)
	}
}

func TestAPIErrors(t *testing.T) {
	testSetup(t)
	addImage(t, "ubuntu")
	q := newSignalQueue()
	mux := apiRoutes(q)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"invalid json", "POST", "/vm", "{bad", http.StatusBadRequest},
		{"bad name", "POST", "/vm", `{"name":"UPPER","image":"ubuntu"}`, http.StatusBadRequest},
		{"empty image", "POST", "/vm", `{"name":"ok"}`, http.StatusBadRequest},
		{"missing image file", "POST", "/vm", `{"name":"ok","image":"nope"}`, http.StatusBadRequest},
		{"delete nonexistent", "DELETE", "/vm/ghost", "", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body)))
			if w.Code != tt.want {
				t.Errorf("got %d want %d: %s", w.Code, tt.want, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("content-type=%q want application/json", ct)
			}
		})
	}

	// Slot exhaustion requires all slots filled.
	t.Run("slot exhaustion", func(t *testing.T) {
		stateMu.Lock()
		for i := range MaxSlots {
			name := fmt.Sprintf("vm%d", i)
			state.VMs[name] = &VMEntry{Name: name, Slot: i}
		}
		stateMu.Unlock()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("POST", "/vm",
			strings.NewReader(`{"name":"overflow","image":"ubuntu"}`)))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("got %d want %d", w.Code, http.StatusServiceUnavailable)
		}
	})
}

func TestMetadataAPI(t *testing.T) {
	testSetup(t)
	q := newSignalQueue()
	stateMu.Lock()
	state.VMs["myvm"] = &VMEntry{
		Name:      "myvm",
		Image:     "ubuntu",
		IP:        "10.10.10.10",
		Status:    "running",
		CreatedAt: time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339),
	}
	stateMu.Unlock()
	mux := metadataRoutes(q)

	// Known IP returns VM info.
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.10.10.10:12345"
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /: %d %s", w.Code, w.Body.String())
	}
	var info map[string]string
	json.NewDecoder(w.Body).Decode(&info)
	if info["name"] != "myvm" || info["status"] != "running" || info["ip"] != "10.10.10.10" {
		t.Fatalf("info=%v", info)
	}
	if info["uptime"] == "" {
		t.Fatal("uptime empty")
	}

	// Unknown IP returns 404.
	req = httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown IP: %d", w.Code)
	}

	// Self-destroy sets status to "destroying".
	req = httptest.NewRequest("POST", "/destroy", nil)
	req.RemoteAddr = "10.10.10.10:12345"
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /destroy: %d", w.Code)
	}
	stateMu.Lock()
	if state.VMs["myvm"].Status != "destroying" {
		t.Fatalf("status=%s want destroying", state.VMs["myvm"].Status)
	}
	stateMu.Unlock()
}

func TestSignalQueue(t *testing.T) {
	q := newSignalQueue()
	q.Notify("a")
	q.Notify("b")
	q.Notify("a") // duplicate, should be deduped

	got := make(map[string]bool)
	for range 2 {
		key, ok := q.Get()
		if !ok {
			t.Fatal("expected ok")
		}
		got[key] = true
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("got=%v want {a, b}", got)
	}

	// ShutDown unblocks a waiting Get.
	go func() {
		time.Sleep(10 * time.Millisecond)
		q.ShutDown()
	}()
	_, ok := q.Get()
	if ok {
		t.Fatal("expected !ok after shutdown")
	}
}

func TestHandleResult(t *testing.T) {
	testSetup(t)
	q := newSignalQueue()
	eng := &engine{signals: q}

	stateMu.Lock()
	state.VMs["test"] = &VMEntry{Name: "test", Status: "booting"}
	stateMu.Unlock()

	// Error increments failures and sets backoff.
	eng.handleResult("test", Result{}, fmt.Errorf("boom"))
	stateMu.Lock()
	vm := state.VMs["test"]
	if vm.ConsecutiveFailures != 1 || vm.NextReconcileAt == "" {
		t.Fatalf("after err1: failures=%d next=%q", vm.ConsecutiveFailures, vm.NextReconcileAt)
	}
	stateMu.Unlock()

	// Second error increments again (exponential backoff).
	eng.handleResult("test", Result{}, fmt.Errorf("boom2"))
	stateMu.Lock()
	if vm.ConsecutiveFailures != 2 {
		t.Fatalf("after err2: failures=%d want 2", vm.ConsecutiveFailures)
	}
	stateMu.Unlock()

	// Success clears failures.
	eng.handleResult("test", Result{}, nil)
	stateMu.Lock()
	if vm.ConsecutiveFailures != 0 || vm.NextReconcileAt != "" {
		t.Fatalf("after success: failures=%d next=%q", vm.ConsecutiveFailures, vm.NextReconcileAt)
	}
	stateMu.Unlock()

	// Requeue: immediate, no NextReconcileAt.
	eng.handleResult("test", Result{Requeue: true}, nil)
	stateMu.Lock()
	if vm.NextReconcileAt != "" {
		t.Fatalf("requeue: next=%q want empty", vm.NextReconcileAt)
	}
	stateMu.Unlock()

	// RequeueAfter: sets NextReconcileAt in the future.
	eng.handleResult("test", Result{RequeueAfter: 5 * time.Second}, nil)
	stateMu.Lock()
	if vm.NextReconcileAt == "" {
		t.Fatal("requeueAfter: NextReconcileAt empty")
	}
	next, _ := time.Parse(time.RFC3339, vm.NextReconcileAt)
	if d := time.Until(next); d < 3*time.Second || d > 6*time.Second {
		t.Fatalf("requeueAfter: delay=%v want ~5s", d)
	}
	stateMu.Unlock()

	// Special keys and non-existent VMs don't panic.
	eng.handleResult("__bridge__", Result{}, fmt.Errorf("err"))
	eng.handleResult("__gc__", Result{}, nil)
	eng.handleResult("ghost", Result{}, fmt.Errorf("err"))
	eng.handleResult("ghost", Result{Requeue: true}, nil)
}
