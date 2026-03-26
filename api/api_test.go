package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	store "github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
	"github.com/sirupsen/logrus"
)

func init() {
	structures.Logger = *logrus.New()
}

func newTestAPIServer() *ApiServer {
	return NewApiServer(&structures.APIConfig{StatsCollectInterval: "1s"}, nil, nil, "boltdb")
}

func newBoltBackedTestAPIServer(t *testing.T) *ApiServer {
	t.Helper()

	db, err := store.NewBBoltDB(t.TempDir(), "api-test.db")
	if err != nil {
		t.Fatalf("failed to create bbolt test db: %v", err)
	}
	t.Cleanup(func() {
		if db.DB != nil {
			_ = db.DB.Close()
		}
	})

	return NewApiServer(&structures.APIConfig{StatsCollectInterval: "1s"}, nil, db, "boltdb")
}

func TestIsValidSCID(t *testing.T) {
	valid := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !isValidSCID(valid) {
		t.Fatalf("expected valid SCID to pass validation")
	}

	invalidCases := []string{
		"",
		"abc",
		"zzzz456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde",
	}

	for _, tc := range invalidCases {
		if isValidSCID(tc) {
			t.Fatalf("expected invalid SCID %q to fail validation", tc)
		}
	}
}

func TestIsValidAddress(t *testing.T) {
	validCases := []string{
		"dero1qyz2p9m3j9w0exampleaddresspayload0000000000",
		"deto1ABCDEFGHIJKLMNOPQRSTUV01234567890123456789",
	}

	for _, tc := range validCases {
		if !isValidAddress(tc) {
			t.Fatalf("expected valid address %q to pass validation", tc)
		}
	}

	invalidCases := []string{
		"",
		"dero",
		"Dero1BadPrefixAndCase123456789012345678901234567890",
		"dero2notvalidbecausemissingseparatorprefix12345678901234567890",
	}

	for _, tc := range invalidCases {
		if isValidAddress(tc) {
			t.Fatalf("expected invalid address %q to fail validation", tc)
		}
	}
}

func TestStatsIndex_DefaultReplyWithoutStats(t *testing.T) {
	server := newTestAPIServer()
	req := httptest.NewRequest(http.MethodGet, "/api/indexedscs", nil)
	rr := httptest.NewRecorder()

	server.StatsIndex(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	if got := rr.Header().Get("Content-Type"); got != "application/json; charset=UTF-8" {
		t.Fatalf("unexpected content type: %q", got)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body["hello"] != "world" {
		t.Fatalf("expected default hello/world response, got %#v", body)
	}
}

func TestInvokeIndexBySCID_RejectsInvalidSCID(t *testing.T) {
	server := newTestAPIServer()
	req := httptest.NewRequest(http.MethodGet, "/api/indexbyscid?scid=not-a-valid-scid", nil)
	rr := httptest.NewRecorder()

	server.InvokeIndexBySCID(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestInvokeIndexBySCID_RejectsInvalidAddress(t *testing.T) {
	server := newTestAPIServer()
	validSCID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "/api/indexbyscid?scid="+validSCID+"&address=bad-address", nil)
	rr := httptest.NewRecorder()

	server.InvokeIndexBySCID(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestInvokeSCVarsByHeight_RequiresSCID(t *testing.T) {
	server := newTestAPIServer()
	req := httptest.NewRequest(http.MethodGet, "/api/scvarsbyheight", nil)
	rr := httptest.NewRecorder()

	server.InvokeSCVarsByHeight(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["variables"]; !ok {
		t.Fatalf("expected variables field in response, got %#v", body)
	}
	if body["variables"] != nil {
		t.Fatalf("expected variables to be nil when scid is missing, got %#v", body["variables"])
	}
}

func TestInvokeSCVarsByHeight_RejectsNegativeTopoheight(t *testing.T) {
	server := newTestAPIServer()
	validSCID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "/api/scvarsbyheight?scid="+validSCID+"&height=-1", nil)
	rr := httptest.NewRecorder()

	server.InvokeSCVarsByHeight(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestInvokeSCVarsByHeight_RejectsHugeTopoheight(t *testing.T) {
	server := newTestAPIServer()
	validSCID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	req := httptest.NewRequest(http.MethodGet, "/api/scvarsbyheight?scid="+validSCID+"&height=100000001", nil)
	rr := httptest.NewRecorder()

	server.InvokeSCVarsByHeight(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestInvalidSCIDStats_DefaultResponseShape(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/invalidscids", nil)
	rr := httptest.NewRecorder()

	server.InvalidSCIDStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["invalidscids"]; !ok {
		t.Fatalf("expected invalidscids field in response, got %#v", body)
	}
}

func TestGetInfo_DefaultResponseShape(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/getinfo", nil)
	rr := httptest.NewRecorder()

	server.GetInfo(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["getinfo"]; !ok {
		t.Fatalf("expected getinfo field in response, got %#v", body)
	}
}

func TestMBLLookupByHash_MissingBLIDReturnsNilMBL(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/getmbladdrsbyhash", nil)
	rr := httptest.NewRecorder()

	server.MBLLookupByHash(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["mbl"]; !ok {
		t.Fatalf("expected mbl field in response, got %#v", body)
	}
	if body["mbl"] != nil {
		t.Fatalf("expected nil mbl when blid is missing, got %#v", body["mbl"])
	}
	if body["hello"] != "world" {
		t.Fatalf("expected default hello/world response, got %#v", body)
	}
}

func TestMBLLookupByAddr_MissingAddressReturnsNilMBL(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/getmblcountbyaddr", nil)
	rr := httptest.NewRecorder()

	server.MBLLookupByAddr(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["mbl"]; !ok {
		t.Fatalf("expected mbl field in response, got %#v", body)
	}
	if body["mbl"] != nil {
		t.Fatalf("expected nil mbl when address is missing, got %#v", body["mbl"])
	}
	if body["hello"] != "world" {
		t.Fatalf("expected default hello/world response, got %#v", body)
	}
}

func TestMBLLookupAll_MissingAddressReturnsNilMBL(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/getmblbyaddr", nil)
	rr := httptest.NewRecorder()

	server.MBLLookupAll(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if _, ok := body["mbl"]; !ok {
		t.Fatalf("expected mbl field in response, got %#v", body)
	}
	if body["mbl"] != nil {
		t.Fatalf("expected nil mbl when address is missing, got %#v", body["mbl"])
	}
	if body["hello"] != "world" {
		t.Fatalf("expected default hello/world response, got %#v", body)
	}
}

func TestChangedSCIDsSince_MissingHeightReturnsEmptySummary(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/changedscids", nil)
	rr := httptest.NewRecorder()

	server.ChangedSCIDsSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	changes, ok := body["changes"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected changes object, got %#v", body["changes"])
	}
	if changes["count"].(float64) != 0 {
		t.Fatalf("expected zero count, got %#v", changes)
	}
}

func TestChangedSCIDsSince_RejectsInvalidHeight(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/changedscids?height=abc", nil)
	rr := httptest.NewRecorder()

	server.ChangedSCIDsSince(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rr.Code)
	}
}

func TestChangedSCIDsSince_ReturnsStoredChanges(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	if err := server.BBSBackend.StoreSCIDChange("scid-a", 5); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}
	if err := server.BBSBackend.StoreSCIDChange("scid-b", 10); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/changedscids?height=5", nil)
	rr := httptest.NewRecorder()

	server.ChangedSCIDsSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	changes, ok := body["changes"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected changes object, got %#v", body["changes"])
	}
	if changes["count"].(float64) != 1 {
		t.Fatalf("expected one changed SCID, got %#v", changes)
	}
	scids, ok := changes["scids"].([]interface{})
	if !ok || len(scids) != 1 || scids[0] != "scid-b" {
		t.Fatalf("unexpected changed scids payload: %#v", changes["scids"])
	}
}

func TestChangedSCIDsSince_FilterTelaIndex(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)

	if _, err := server.BBSBackend.StoreSCIDVariableDetails("scid-tela", []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}}, 5); err != nil {
		t.Fatalf("failed to store tela variables: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store tela interaction height: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDVariableDetails("scid-other", []*structures.SCIDVariable{{Key: "C", Value: "plain contract"}}, 7); err != nil {
		t.Fatalf("failed to store non-tela variables: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDInteractionHeight("scid-other", 7); err != nil {
		t.Fatalf("failed to store non-tela interaction height: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/changedscids?height=0&filter=tela-index", nil)
	rr := httptest.NewRecorder()

	server.ChangedSCIDsSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	changes := body["changes"].(map[string]interface{})
	scids := changes["scids"].([]interface{})
	if len(scids) != 1 || scids[0] != "scid-tela" {
		t.Fatalf("unexpected filtered changed scids payload: %#v", scids)
	}
}

func TestChangedTelaSCIDsSince_MissingHeightReturnsEmptySummary(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/telachanged", nil)
	rr := httptest.NewRecorder()

	server.ChangedTelaSCIDsSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela, ok := body["tela"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected tela object, got %#v", body["tela"])
	}
	if tela["count"].(float64) != 0 {
		t.Fatalf("expected zero count, got %#v", tela)
	}
	if tela["filter"] != "tela-index" {
		t.Fatalf("expected tela-index filter, got %#v", tela)
	}
}

func TestChangedTelaSCIDsSince_ReturnsTelaChanges(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)

	if _, err := server.BBSBackend.StoreSCIDVariableDetails("scid-tela", []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}}, 5); err != nil {
		t.Fatalf("failed to store tela vars: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store tela height: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDVariableDetails("scid-other", []*structures.SCIDVariable{{Key: "C", Value: "plain contract"}}, 7); err != nil {
		t.Fatalf("failed to store other vars: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDInteractionHeight("scid-other", 7); err != nil {
		t.Fatalf("failed to store other height: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/telachanged?height=0", nil)
	rr := httptest.NewRecorder()

	server.ChangedTelaSCIDsSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela := body["tela"].(map[string]interface{})
	scids := tela["scids"].([]interface{})
	if len(scids) != 1 || scids[0] != "scid-tela" {
		t.Fatalf("unexpected tela changed payload: %#v", tela)
	}
}

func TestTelaMetadataSince_ReturnsTelaMetadata(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	vars := []*structures.SCIDVariable{
		{Key: "C", Value: "TELA INDEX"},
		{Key: "dURL", Value: "https://example.test"},
		{Key: "NameHdr", Value: "Example App"},
	}
	if _, err := server.BBSBackend.StoreSCIDVariableDetails("scid-tela", vars, 5); err != nil {
		t.Fatalf("failed to store tela vars: %v", err)
	}
	if _, err := server.BBSBackend.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store tela height: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/telametadata?height=0", nil)
	rr := httptest.NewRecorder()

	server.TelaMetadataSince(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela := body["tela"].(map[string]interface{})
	results := tela["results"].([]interface{})
	if len(results) != 1 {
		t.Fatalf("expected one tela metadata result, got %#v", results)
	}
	result := results[0].(map[string]interface{})
	if result["scid"] != "scid-tela" || result["nameHdr"] != "Example App" || result["durl"] != "https://example.test" {
		t.Fatalf("unexpected tela metadata result: %#v", result)
	}
}

func TestTelaMetadataAll_ReturnsOnlyTelaMetadata(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	if err := server.BBSBackend.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", NameHdr: "Tela App", IsTelaIndex: true}); err != nil {
		t.Fatalf("failed to store tela metadata: %v", err)
	}
	if err := server.BBSBackend.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", NameHdr: "Other App", IsTelaIndex: false}); err != nil {
		t.Fatalf("failed to store non-tela metadata: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tela", nil)
	rr := httptest.NewRecorder()

	server.TelaMetadataAll(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela := body["tela"].(map[string]interface{})
	results := tela["results"].([]interface{})
	if len(results) != 1 {
		t.Fatalf("expected one tela metadata result, got %#v", results)
	}
	result := results[0].(map[string]interface{})
	if result["scid"] != "scid-a" {
		t.Fatalf("unexpected tela all result: %#v", result)
	}
}

func TestTelaMetadataAll_LimitApplies(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	if err := server.BBSBackend.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", NameHdr: "A", IsTelaIndex: true}); err != nil {
		t.Fatalf("failed to store tela metadata a: %v", err)
	}
	if err := server.BBSBackend.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", NameHdr: "B", IsTelaIndex: true}); err != nil {
		t.Fatalf("failed to store tela metadata b: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tela?limit=1", nil)
	rr := httptest.NewRecorder()
	server.TelaMetadataAll(rr, req)

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela := body["tela"].(map[string]interface{})
	results := tela["results"].([]interface{})
	if len(results) != 1 || tela["count"].(float64) != 1 {
		t.Fatalf("expected limited tela results, got %#v", tela)
	}
}

func TestTelaMetadataSince_LimitApplies(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	for i, scid := range []string{"scid-a", "scid-b"} {
		vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: scid}}
		if _, err := server.BBSBackend.StoreSCIDVariableDetails(scid, vars, int64(i+1)); err != nil {
			t.Fatalf("failed to store vars: %v", err)
		}
		if _, err := server.BBSBackend.StoreSCIDInteractionHeight(scid, int64(i+1)); err != nil {
			t.Fatalf("failed to store interaction height: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/telametadata?height=0&limit=1", nil)
	rr := httptest.NewRecorder()
	server.TelaMetadataSince(rr, req)

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	tela := body["tela"].(map[string]interface{})
	results := tela["results"].([]interface{})
	if len(results) != 1 || tela["count"].(float64) != 1 {
		t.Fatalf("expected limited tela metadata results, got %#v", tela)
	}
}

func TestTelaMetadataAll_OffsetAndLimitApply(t *testing.T) {
	server := newBoltBackedTestAPIServer(t)
	for _, scid := range []string{"scid-a", "scid-b", "scid-c"} {
		if err := server.BBSBackend.StoreTelaMetadata(scid, &structures.TelaMetadata{SCID: scid, NameHdr: scid, IsTelaIndex: true}); err != nil {
			t.Fatalf("failed to store tela metadata %s: %v", scid, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tela?offset=1&limit=1", nil)
	rr := httptest.NewRecorder()
	server.TelaMetadataAll(rr, req)

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	tela := body["tela"].(map[string]interface{})
	results := tela["results"].([]interface{})
	if len(results) != 1 || tela["count"].(float64) != 1 || tela["offset"].(float64) != 1 || tela["limit"].(float64) != 1 {
		t.Fatalf("unexpected paged tela results: %#v", tela)
	}
	result := results[0].(map[string]interface{})
	if result["scid"] != "scid-b" {
		t.Fatalf("unexpected paged tela result: %#v", result)
	}
}
