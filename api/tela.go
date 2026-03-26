package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/civilware/Gnomon/structures"
)

func (apiServer *ApiServer) TelaMetadataSince(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Cache-Control", "no-cache")

	reply := make(map[string]interface{})

	heightParam := r.URL.Query().Get("height")
	limit, hasLimit := parseLimitParam(r.URL.Query().Get("limit"))
	offset, hasOffset := parseOffsetParam(r.URL.Query().Get("offset"))
	if heightParam == "" {
		reply["tela"] = structures.TelaMetadata_Result{Topoheight: 0, Results: nil, Count: 0}
		_ = json.NewEncoder(writer).Encode(reply)
		return
	}

	topoheight, err := strconv.ParseInt(heightParam, 10, 64)
	if err != nil {
		http.Error(writer, "Invalid topoheight", http.StatusBadRequest)
		return
	}
	if topoheight < 0 {
		http.Error(writer, "Invalid topoheight: must be non-negative", http.StatusBadRequest)
		return
	}

	results := make([]structures.TelaMetadata, 0)
	for _, scid := range func() []string {
		switch apiServer.DBType {
		case "gravdb":
			classifier := apiServer.GravDBBackend
			return classifier.GetSCIDChangesSince(topoheight)
		case "boltdb":
			return apiServer.BBSBackend.GetSCIDChangesSince(topoheight)
		default:
			return nil
		}
	}() {
		var meta *structures.TelaMetadata
		switch apiServer.DBType {
		case "gravdb":
			meta = apiServer.GravDBBackend.GetTelaMetadata(scid)
		case "boltdb":
			meta = apiServer.BBSBackend.GetTelaMetadata(scid)
		}
		if meta != nil && meta.IsTelaIndex {
			results = append(results, *meta)
		}
	}
	if hasOffset || hasLimit {
		results = pageSlice(results, offset, limit)
	}

	reply["tela"] = structures.TelaMetadata_Result{Topoheight: topoheight, Results: results, Count: len(results), Offset: offset, Limit: limit}
	if err := json.NewEncoder(writer).Encode(reply); err != nil {
		logger.Errorf("[API] Error serializing API response: %v", err)
	}
}

func (apiServer *ApiServer) TelaMetadataAll(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Cache-Control", "no-cache")

	reply := make(map[string]interface{})
	results := make([]structures.TelaMetadata, 0)
	limit, hasLimit := parseLimitParam(r.URL.Query().Get("limit"))
	offset, hasOffset := parseOffsetParam(r.URL.Query().Get("offset"))

	switch apiServer.DBType {
	case "gravdb":
		for _, meta := range apiServer.GravDBBackend.GetAllTelaMetadata() {
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	case "boltdb":
		for _, meta := range apiServer.BBSBackend.GetAllTelaMetadata() {
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	}
	if hasOffset || hasLimit {
		results = pageSlice(results, offset, limit)
	}

	reply["tela"] = structures.TelaMetadata_Result{Topoheight: 0, Results: results, Count: len(results), Offset: offset, Limit: limit}
	if err := json.NewEncoder(writer).Encode(reply); err != nil {
		logger.Errorf("[API] Error serializing API response: %v", err)
	}
}
