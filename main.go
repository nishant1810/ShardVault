package main

import (
	"bytes"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Ansh2004P/hdfs/p2p"
)

// sanitizeRoot converts a listen address like ":5000" or "127.0.0.1:5000" into a
// filesystem-friendly folder name (e.g. "5000_network" or "127.0.0.1_5000_network").
// Windows does not allow ':' in path components, so we strip/replace it.
func sanitizeRoot(listenAddr string) string {
	addr := strings.TrimSpace(listenAddr)
	// Remove leading colon (common pattern like ":3000")
	addr = strings.TrimPrefix(addr, ":")
	// Replace remaining colons (e.g. in "127.0.0.1:3000") with underscore.
	addr = strings.ReplaceAll(addr, ":", "_")
	if addr == "" {
		addr = "node"
	}
	return addr + "_network"
}

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcptransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)

	fileServerOpts := FileServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       sanitizeRoot(listenAddr),
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":7000", "")
	s3 := makeServer(":5000", ":3000", ":7000")

	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(500 * time.Millisecond)
	go func() { log.Fatal(s2.Start()) }()

	time.Sleep(1 * time.Second)

	go func() { log.Fatal(s3.Start()) }()
	time.Sleep(1500 * time.Millisecond)

	cm := NewClusterManager(s1, s2, s3)

	// Pre-seed 3 demonstration files into the cluster so the dashboard is immediately populated
	sampleFiles := []struct {
		key     string
		content string
	}{
		{
			key:     "welcome_cluster_manifest.json",
			content: "{\n  \"cluster\": \"ShardVault-Production\",\n  \"cipher\": \"AES-256-CTR\",\n  \"hashing\": \"SHA1-CAS\",\n  \"nodes\": 3,\n  \"status\": \"healthy\"\n}",
		},
		{
			key:     "p2p_replication_proof.txt",
			content: "ShardVault distributed storage verified! Data is split into 5-character CAS chunk directories and mirrored to all connected peers.",
		},
		{
			key:     "secret_vault_payload.dat",
			content: "Top secret encrypted data stream. On-wire transport is protected with 16-byte random IVs per transfer.",
		},
	}

	for _, sf := range sampleFiles {
		data := bytes.NewReader([]byte(sf.content))
		if err := s3.Store(sf.key, data); err == nil {
			cm.RegisterFile(sf.key, int64(len(sf.content)))
		}
	}
	time.Sleep(300 * time.Millisecond)
	for _, sf := range sampleFiles {
		cm.UpdateFileReplicas(sf.key)
	}

	// Read PORT from environment (for Render / Railway / Fly.io / Heroku), default to 8080
	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "8080"
	}

	StartHTTPServer(port, cm)
}
