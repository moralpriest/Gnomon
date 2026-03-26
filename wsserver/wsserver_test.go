package wsserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/civilware/Gnomon/structures"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

func init() {
	structures.Logger = *logrus.New()
}

func TestWebsocketRateLimiterHonorsContextCancellation(t *testing.T) {
	logger = structures.Logger.WithFields(logrus.Fields{})
	wss := &WSServer{}
	l := rate.NewLimiter(rate.Every(time.Hour), 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := wss.wsHandleClient(ctx, nil, l)
	if err == nil {
		t.Fatalf("expected cancelled context error")
	}
}

func TestListSCHardcoded_ReturnsHardcodedSCIDs(t *testing.T) {
	result, err := ListSCHardcoded(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.SCHardcoded) != len(structures.Hardcoded_SCIDS) {
		t.Fatalf("expected %d hardcoded SCIDs, got %d", len(structures.Hardcoded_SCIDS), len(result.SCHardcoded))
	}

	for i, scid := range structures.Hardcoded_SCIDS {
		if result.SCHardcoded[i] != scid {
			t.Fatalf("unexpected hardcoded SCID at index %d: got %q want %q", i, result.SCHardcoded[i], scid)
		}
	}
}

func TestListSCCode_RequiresSCID(t *testing.T) {
	_, err := ListSCCode(context.Background(), structures.WS_ListSCCode_Params{}, nil)
	if err == nil {
		t.Fatalf("expected missing scid error")
	}
	if !strings.Contains(err.Error(), "No SCID provided") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListSCVariables_RequiresSCID(t *testing.T) {
	_, err := ListSCVariables(context.Background(), structures.WS_ListSCVariables_Params{}, nil)
	if err == nil {
		t.Fatalf("expected missing scid error")
	}
	if !strings.Contains(err.Error(), "No SCID provided") {
		t.Fatalf("unexpected error: %v", err)
	}
}
