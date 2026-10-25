package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

//go:embed web/*
var webFS embed.FS

type TrackedFile struct {
	Key     string            `json:"key"`
	Hash    string            `json:"hash"`
	CASPath string            `json:"cas_path"`
	Size    int64             `json:"size"`
	Nodes   map[string]bool   `json:"nodes"`
}

type NodeStatus struct {
	Name          string `json:"name"`
	ListenAddr    string `json:"listen_addr"`
	ID            string `json:"id"`
	StorageRoot   string `json:"storage_root"`
	PeerCount     int    `json:"peer_count"`
	IsCoordinator bool   `json:"is_coordinator"`
}

type ClusterManager struct {
	mu           sync.RWMutex
	nodes        []*FileServer
	nodeMap      map[string]*FileServer
	coordinator  *FileServer
	trackedFiles map[string]*TrackedFile
	startTime    time.Time
}

func NewClusterManager(s1, s2, s3 *FileServer) *ClusterManager {
	cm := &ClusterManager{
		nodes:        []*FileServer{s1, s2, s3},
		nodeMap:      make(map[string]*FileServer),
		coordinator:  s3,
		trackedFiles: make(map[string]*TrackedFile),
		startTime:    time.Now(),
	}

	for _, s := range cm.nodes {
		cm.nodeMap[s.Addr()] = s
	}

	return cm
}

func (cm *ClusterManager) RegisterFile(key string, size int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	pathKey := CASPathTransformFunc(key)
	cm.trackedFiles[key] = &TrackedFile{
		Key:     key,
		Hash:    pathKey.Filename,
		CASPath: pathKey.FullPath(),
		Size:    size,
		Nodes:   make(map[string]bool),
	}
}

func (cm *ClusterManager) UpdateFileReplicas(key string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tf, ok := cm.trackedFiles[key]
	if !ok {
		return
	}

	for _, n := range cm.nodes {
		tf.Nodes[n.Addr()] = n.Has(key)
	}
}

func (cm *ClusterManager) HandleStatus(w http.ResponseWriter, r *http.Request) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodeStatuses := make([]NodeStatus, len(cm.nodes))
	for i, n := range cm.nodes {
		nodeStatuses[i] = NodeStatus{
			Name:          fmt.Sprintf("Node %d (%s)", i+1, n.Addr()),
			ListenAddr:    n.Addr(),
			ID:            n.ID,
			StorageRoot:   n.StorageRootPath(),
			PeerCount:     n.PeerCount(),
			IsCoordinator: n == cm.coordinator,
		}
	}

	resp := map[string]any{
		"nodes":       nodeStatuses,
		"coordinator": cm.coordinator.Addr(),
		"uptime_sec":  int(time.Since(cm.startTime).Seconds()),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (cm *ClusterManager) HandleListFiles(w http.ResponseWriter, r *http.Request) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	fileList := make([]TrackedFile, 0, len(cm.trackedFiles))
	for key, tf := range cm.trackedFiles {
		// Refresh node presence in real-time
		for _, n := range cm.nodes {
			tf.Nodes[n.Addr()] = n.Has(key)
		}
		fileList = append(fileList, *tf)
	}

	resp := map[string]any{
		"files": fileList,
		"total": len(fileList),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (cm *ClusterManager) HandleStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Support form-data or direct payload
	var (
		key     string
		content []byte
		err     error
	)

	if err := r.ParseMultipartForm(32 << 20); err == nil {
		key = r.FormValue("key")
		// Check if file was uploaded
		file, _, fileErr := r.FormFile("file")
		if fileErr == nil {
			defer file.Close()
			content, err = io.ReadAll(file)
		} else {
			content = []byte(r.FormValue("content"))
		}
	} else {
		key = r.FormValue("key")
		content = []byte(r.FormValue("content"))
	}

	if len(key) == 0 {
		key = fmt.Sprintf("shard_%d.dat", time.Now().Unix())
	}
	if len(content) == 0 {
		content = []byte("Default payload replicated across ShardVault P2P cluster.")
	}

	// Store in Coordinator, which automatically encrypts via AES-CTR and replicates via TCP
	err = cm.coordinator.Store(key, bytes.NewReader(content))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
		return
	}

	// Register file in cluster manager
	cm.RegisterFile(key, int64(len(content)))
	// Wait a tiny slice for peers to write to disk
	time.Sleep(200 * time.Millisecond)
	cm.UpdateFileReplicas(key)

	pathKey := CASPathTransformFunc(key)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success":  true,
		"key":      key,
		"size":     len(content),
		"cas_path": pathKey.FullPath(),
		"hash":     pathKey.Filename,
	})
}

func (cm *ClusterManager) HandleGetFile(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if len(key) == 0 {
		http.Error(w, "Query parameter 'key' is required", http.StatusBadRequest)
		return
	}

	// Check if file existed on coordinator before Get
	hadLocally := cm.coordinator.Has(key)

	// Fetch via coordinator
	reader, err := cm.coordinator.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch file: %v", err), http.StatusNotFound)
		return
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading stream: %v", err), http.StatusInternalServerError)
		return
	}

	if rc, ok := reader.(io.ReadCloser); ok {
		rc.Close()
	}

	cm.UpdateFileReplicas(key)

	if hadLocally {
		w.Header().Set("X-ShardVault-Source", "Local Disk Cache")
	} else {
		w.Header().Set("X-ShardVault-Source", "P2P Network (Self-Healed from Peer Replicas)")
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (cm *ClusterManager) HandleDeleteLocal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if len(key) == 0 {
		key = r.FormValue("key")
	}
	if len(key) == 0 {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Delete ONLY from coordinator's disk
	if err := cm.coordinator.DeleteLocal(key); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete local copy: %v", err), http.StatusInternalServerError)
		return
	}

	cm.UpdateFileReplicas(key)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": fmt.Sprintf("Deleted local disk copy of '%s' from Node 3 (%s). Peer replicas remain safe.", key, cm.coordinator.Addr()),
	})
}

func (cm *ClusterManager) HandleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	count := 10 // Run 10 items for quick, responsive feedback
	successCount := 0

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("benchmark_doc_%d.dat", i)
		content := []byte(fmt.Sprintf("ShardVault Benchmark Payload #%d - Verified at %s", i, time.Now().Format(time.RFC3339)))

		// 1. Store across cluster
		if err := cm.coordinator.Store(key, bytes.NewReader(content)); err != nil {
			log.Printf("[Benchmark Error] Store failed for %s: %v", key, err)
			continue
		}
		cm.RegisterFile(key, int64(len(content)))
		time.Sleep(150 * time.Millisecond)

		// 2. Delete local copy on Node 3
		if err := cm.coordinator.DeleteLocal(key); err != nil {
			log.Printf("[Benchmark Error] DeleteLocal failed for %s: %v", key, err)
			continue
		}

		// 3. Fetch back from peer network
		reader, err := cm.coordinator.Get(key)
		if err != nil {
			log.Printf("[Benchmark Error] Get from peers failed for %s: %v", key, err)
			continue
		}

		data, err := io.ReadAll(reader)
		if rc, ok := reader.(io.ReadCloser); ok {
			rc.Close()
		}

		if err == nil && string(data) == string(content) {
			successCount++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"count":   successCount,
		"message": fmt.Sprintf("Benchmark verified: %d/%d objects successfully replicated, deleted locally, and healed via P2P!", successCount, count),
	})
}

// StartHTTPServer serves the web dashboard and REST API
func StartHTTPServer(port string, cm *ClusterManager) {
	mux := http.NewServeMux()

	// Static UI
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		indexBytes, err := webFS.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "Dashboard file not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexBytes)
	})

	// API Endpoints
	mux.HandleFunc("/api/status", cm.HandleStatus)
	mux.HandleFunc("/api/files", cm.HandleListFiles)
	mux.HandleFunc("/api/store", cm.HandleStore)
	mux.HandleFunc("/api/file", cm.HandleGetFile)
	mux.HandleFunc("/api/delete-local", cm.HandleDeleteLocal)
	mux.HandleFunc("/api/benchmark", cm.HandleBenchmark)

	addr := ":" + port
	log.Printf("=====================================================")
	log.Printf("🚀 ShardVault Dashboard LIVE at http://localhost%s", addr)
	log.Printf("=====================================================")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
