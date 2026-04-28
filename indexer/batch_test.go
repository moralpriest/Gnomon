package indexer

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/deroproject/derohe/rpc"
	"github.com/gorilla/websocket"
)

func TestBatchGetSCData_Success(t *testing.T) {
	cli, srv := channel.Direct()

	assigner := handler.Map{
		"DERO.Ping": handler.New(func(ctx context.Context) (string, error) {
			return "Pong", nil
		}),
		"DERO.GetSC": handler.New(func(ctx context.Context, params rpc.GetSC_Params) (rpc.GetSC_Result, error) {
			result := rpc.GetSC_Result{
				Code: "Function Initialize() Uint64\nEnd Function",
				VariableStringKeys: map[string]interface{}{
					"nameHdr": "48656c6c6f", // "Hello" hex encoded
				},
				VariableUint64Keys: map[uint64]interface{}{
					1: float64(42),
				},
				Balances: map[string]uint64{"scid": 0},
			}
			return result, nil
		}),
	}
	server := jrpc2.NewServer(assigner, nil)
	go server.Start(srv)
	defer server.Stop()

	client := jrpc2.NewClient(cli, nil)
	defer client.Close()

	idx := &Indexer{
		RPC: &Client{WS: &websocket.Conn{}, RPC: client},
	}

	results, err := idx.BatchGetSCData([]string{"testscid1234567890123456789012345678901234567890123456789012345678"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	data := results["testscid1234567890123456789012345678901234567890123456789012345678"]
	if data == nil {
		t.Fatal("expected result for test scid")
	}
	if data.Code == "" {
		t.Fatal("expected non-empty code")
	}
	if len(data.Variables) == 0 {
		t.Fatal("expected variables")
	}
}

func TestBatchGetSCData_EmptyInput(t *testing.T) {
	idx := &Indexer{}
	results, err := idx.BatchGetSCData([]string{})
	if err != nil {
		t.Fatalf("unexpected error for empty input: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty input, got %d", len(results))
	}
}

func TestBatchGetSCData_UnhealthyRPC(t *testing.T) {
	idx := &Indexer{
		RPC: &Client{}, // nil RPC
	}
	_, err := idx.BatchGetSCData([]string{"scid"})
	if err == nil {
		t.Fatal("expected error when RPC is unhealthy")
	}
}
