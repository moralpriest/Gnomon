package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

func (apiServer *ApiServer) ChangedSCIDsSince(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Cache-Control", "no-cache")

	reply := make(map[string]interface{})

	stats := apiServer.getStats()
	if stats != nil {
		reply["numscs"] = stats["numscs"]
		reply["regTxCount"] = stats["regTxCount"]
		reply["burnTxCount"] = stats["burnTxCount"]
		reply["normTxCount"] = stats["normTxCount"]
	} else {
		reply["hello"] = "world"
	}

	heightParam := r.URL.Query().Get("height")
	classifierName := r.URL.Query().Get("filter")
	if heightParam == "" {
		reply["changes"] = structures.ChangedSCIDs_Result{Topoheight: 0, SCIDs: nil, Count: 0}
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

	var changes []string
	switch apiServer.DBType {
	case "gravdb":
		classifier := storage.NewClassifier(classifierName, apiServer.GravDBBackend)
		changes = storage.GetFilteredChangedSCIDsSince(apiServer.GravDBBackend, classifier, topoheight)
	case "boltdb":
		classifier := storage.NewClassifier(classifierName, apiServer.BBSBackend)
		changes = storage.GetFilteredChangedSCIDsSince(apiServer.BBSBackend, classifier, topoheight)
	}

	reply["changes"] = structures.ChangedSCIDs_Result{
		Topoheight: topoheight,
		SCIDs:      changes,
		Count:      len(changes),
	}

	if err := json.NewEncoder(writer).Encode(reply); err != nil {
		logger.Errorf("[API] Error serializing API response: %v", err)
	}
}

func (apiServer *ApiServer) ChangedTelaSCIDsSince(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Cache-Control", "no-cache")

	reply := make(map[string]interface{})

	stats := apiServer.getStats()
	if stats != nil {
		reply["numscs"] = stats["numscs"]
		reply["regTxCount"] = stats["regTxCount"]
		reply["burnTxCount"] = stats["burnTxCount"]
		reply["normTxCount"] = stats["normTxCount"]
	} else {
		reply["hello"] = "world"
	}

	heightParam := r.URL.Query().Get("height")
	if heightParam == "" {
		reply["tela"] = structures.TelaChanged_Result{Topoheight: 0, SCIDs: nil, Count: 0, Filter: storage.ClassifierTelaIndex}
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

	var changes []string
	switch apiServer.DBType {
	case "gravdb":
		classifier := storage.NewClassifier(storage.ClassifierTelaIndex, apiServer.GravDBBackend)
		changes = storage.GetFilteredChangedSCIDsSince(apiServer.GravDBBackend, classifier, topoheight)
	case "boltdb":
		classifier := storage.NewClassifier(storage.ClassifierTelaIndex, apiServer.BBSBackend)
		changes = storage.GetFilteredChangedSCIDsSince(apiServer.BBSBackend, classifier, topoheight)
	}

	reply["tela"] = structures.TelaChanged_Result{
		Topoheight: topoheight,
		SCIDs:      changes,
		Count:      len(changes),
		Filter:     storage.ClassifierTelaIndex,
	}

	if err := json.NewEncoder(writer).Encode(reply); err != nil {
		logger.Errorf("[API] Error serializing API response: %v", err)
	}
}
