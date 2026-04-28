package indexer

import (
	"context"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/gorilla/websocket"
)

func TestGetHealthyRPC_NilRPC(t *testing.T) {
	c := &Client{}
	_, cleanup, err := c.GetHealthyRPC()
	if cleanup != nil {
		cleanup()
	}
	if err == nil {
		t.Fatal("expected error when RPC is nil, got nil")
	}
	if err.Error() != "rpc client not initialized" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGetHealthyRPC_NilWS(t *testing.T) {
	c := &Client{RPC: &jrpc2.Client{}}
	_, cleanup, err := c.GetHealthyRPC()
	if cleanup != nil {
		cleanup()
	}
	if err == nil {
		t.Fatal("expected error when WS is nil, got nil")
	}
	if err.Error() != "websocket connection not initialized" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGetHealthyRPC_StaleConnection(t *testing.T) {
	// Create a server that returns an error for DERO.Ping.
	cli, srv := channel.Direct()

	assigner := handler.Map{
		"DERO.Ping": handler.New(func(ctx context.Context) (string, error) {
			return "", fmt.Errorf("simulated ping failure")
		}),
	}
	server := jrpc2.NewServer(assigner, nil)
	go server.Start(srv)
	defer server.Stop()

	client := jrpc2.NewClient(cli, nil)
	defer client.Close()

	c := &Client{WS: &websocket.Conn{}, RPC: client}
	_, cleanup, err := c.GetHealthyRPC()
	if cleanup != nil {
		cleanup()
	}
	if err == nil {
		t.Fatal("expected error when ping fails, got nil")
	}
	if !contains(err.Error(), "rpc health check failed") {
		t.Fatalf("expected 'rpc health check failed' in error, got: %v", err)
	}
}

func TestGetHealthyRPC_HealthyConnection(t *testing.T) {
	cli, srv := channel.Direct()

	assigner := handler.Map{
		"DERO.Ping": handler.New(func(ctx context.Context) (string, error) {
			return "Pong", nil
		}),
	}
	server := jrpc2.NewServer(assigner, nil)
	go server.Start(srv)
	defer server.Stop()

	client := jrpc2.NewClient(cli, nil)
	defer client.Close()

	c := &Client{WS: &websocket.Conn{}, RPC: client}
	rpc, cleanup, err := c.GetHealthyRPC()
	if err != nil {
		t.Fatalf("unexpected error for healthy connection: %v", err)
	}
	if rpc == nil {
		t.Fatal("expected non-nil rpc client")
	}
	cleanup()
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
