package queryapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGraphLimit = 5000
	maxGraphLimit     = 10000
	maxGraphWindow    = 24 * time.Hour
)

type GraphStore interface {
	QueryGraphRecords(context.Context, GraphQuery) ([]GraphRecord, error)
}

type GraphQuery struct {
	TenantID  uint64
	Start     time.Time
	End       time.Time
	TraceID   string
	SessionID string
	UserID    string
	Limit     int
}

type GraphRecord struct {
	Timestamp time.Time
	Service   string
	Level     string
	TraceID   string
	IngestID  string
	Fields    map[string]any
}

type ServiceNode struct {
	Service    string `json:"service"`
	LogCount   int    `json:"log_count"`
	ErrorCount int    `json:"error_count"`
	FlowCount  int    `json:"flow_count"`
}

type ServiceEdge struct {
	Source               string `json:"source"`
	Target               string `json:"target"`
	FlowCount            int    `json:"flow_count"`
	ErrorFlowCount       int    `json:"error_flow_count"`
	PropagatedErrorCount int    `json:"propagated_error_count"`
}

type SessionGroup struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	UserID     string    `json:"user_id,omitempty"`
	TraceIDs   []string  `json:"trace_ids,omitempty"`
	Services   []string  `json:"services"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at"`
	LogCount   int       `json:"log_count"`
	ErrorCount int       `json:"error_count"`
}

type graphResponse struct {
	Nodes             []ServiceNode  `json:"nodes"`
	Edges             []ServiceEdge  `json:"edges"`
	Sessions          []SessionGroup `json:"sessions"`
	RecordCount       int            `json:"record_count"`
	Truncated         bool           `json:"truncated"`
	Partial           bool           `json:"partial"`
	UnavailableShards []string       `json:"unavailable_shards,omitempty"`
}

type GraphHandler struct{ store GraphStore }

func NewGraphHandler(store GraphStore) *GraphHandler { return &GraphHandler{store: store} }

func (h *GraphHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h == nil || h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "graph store unavailable")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant identity required")
		return
	}
	query, err := parseGraphQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	query.TenantID = tenantID
	status := clusterStatus(r.Context(), h.store)
	records, err := h.store.QueryGraphRecords(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to build service graph")
		return
	}
	truncated := len(records) > query.Limit
	if truncated {
		records = records[:query.Limit]
	}
	nodes, edges, sessions := buildServiceGraph(records)
	writeJSON(w, http.StatusOK, graphResponse{
		Nodes: nodes, Edges: edges, Sessions: sessions, RecordCount: len(records),
		Truncated: truncated, Partial: status.Partial, UnavailableShards: status.UnavailableShards,
	})
}

func parseGraphQuery(r *http.Request) (GraphQuery, error) {
	values := r.URL.Query()
	start, err := parseRequiredTimestamp(values.Get("start"), "start")
	if err != nil {
		return GraphQuery{}, err
	}
	end, err := parseRequiredTimestamp(values.Get("end"), "end")
	if err != nil {
		return GraphQuery{}, err
	}
	if !start.Before(end) {
		return GraphQuery{}, errors.New("start must be before end")
	}
	if end.Sub(start) > maxGraphWindow {
		return GraphQuery{}, errors.New("time range cannot exceed 24 hours")
	}
	traceID, err := parseSafeOptionalFilter(values.Get("trace_id"), "trace_id")
	if err != nil {
		return GraphQuery{}, err
	}
	sessionID, err := parseGraphIdentifier(values.Get("session_id"), "session_id")
	if err != nil {
		return GraphQuery{}, err
	}
	userID, err := parseGraphIdentifier(values.Get("user_id"), "user_id")
	if err != nil {
		return GraphQuery{}, err
	}
	limit := defaultGraphLimit
	if raw := strings.TrimSpace(values.Get("limit")); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return GraphQuery{}, errors.New("limit must be a positive integer")
		}
		if limit > maxGraphLimit {
			limit = maxGraphLimit
		}
	}
	return GraphQuery{Start: start, End: end, TraceID: traceID, SessionID: sessionID, UserID: userID, Limit: limit}, nil
}

func parseGraphIdentifier(value, name string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) > 256 {
		return "", errors.New(name + " cannot exceed 256 characters")
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return "", errors.New(name + " contains unsupported characters")
		}
	}
	return value, nil
}

type edgeKey struct{ source, target string }
type edgeState struct{ error, propagated bool }

func buildServiceGraph(records []GraphRecord) ([]ServiceNode, []ServiceEdge, []SessionGroup) {
	slices.SortFunc(records, func(a, b GraphRecord) int { return a.Timestamp.Compare(b.Timestamp) })
	nodeMap := map[string]*ServiceNode{}
	flows := map[string][]GraphRecord{}
	for _, record := range records {
		service := normalizeGraphService(record.Service)
		if service == "" {
			continue
		}
		node := nodeMap[service]
		if node == nil {
			node = &ServiceNode{Service: service}
			nodeMap[service] = node
		}
		node.LogCount++
		if isErrorLevel(record.Level) {
			node.ErrorCount++
		}
		key, _ := graphFlowIdentity(record)
		if key != "" {
			flows[key] = append(flows[key], record)
		}
	}

	edgeMap := map[edgeKey]*ServiceEdge{}
	sessions := make([]SessionGroup, 0, len(flows))
	for flowKey, flowRecords := range flows {
		seenServices := map[string]struct{}{}
		flowEdges := map[edgeKey]edgeState{}
		traceIDs := map[string]struct{}{}
		userID := ""
		errorCount := 0
		for index, record := range flowRecords {
			service := normalizeGraphService(record.Service)
			seenServices[service] = struct{}{}
			if record.TraceID != "" {
				traceIDs[record.TraceID] = struct{}{}
			}
			if userID == "" {
				userID = graphField(record.Fields, "user_id", "userid", "user-id")
			}
			currentError := isErrorLevel(record.Level)
			if currentError {
				errorCount++
			}
			if upstream := normalizeGraphService(graphField(record.Fields, "upstream_service", "upstream.service", "caller_service")); upstream != "" && upstream != service {
				if nodeMap[upstream] == nil {
					nodeMap[upstream] = &ServiceNode{Service: upstream}
				}
				seenServices[upstream] = struct{}{}
				state := flowEdges[edgeKey{upstream, service}]
				state.error = state.error || currentError
				flowEdges[edgeKey{upstream, service}] = state
			}
			if downstream := normalizeGraphService(graphField(record.Fields, "downstream_service", "downstream.service", "peer_service", "target_service")); downstream != "" && downstream != service {
				if nodeMap[downstream] == nil {
					nodeMap[downstream] = &ServiceNode{Service: downstream}
				}
				seenServices[downstream] = struct{}{}
				state := flowEdges[edgeKey{service, downstream}]
				state.error = state.error || currentError
				flowEdges[edgeKey{service, downstream}] = state
			}
			if index > 0 {
				previous := flowRecords[index-1]
				source := normalizeGraphService(previous.Service)
				if source != "" && service != "" && source != service {
					key := edgeKey{source, service}
					state := flowEdges[key]
					state.error = state.error || currentError
					state.propagated = state.propagated || (isErrorLevel(previous.Level) && currentError)
					flowEdges[key] = state
				}
			}
		}
		for service := range seenServices {
			nodeMap[service].FlowCount++
		}
		for key, state := range flowEdges {
			edge := edgeMap[key]
			if edge == nil {
				edge = &ServiceEdge{Source: key.source, Target: key.target}
				edgeMap[key] = edge
			}
			edge.FlowCount++
			if state.error {
				edge.ErrorFlowCount++
			}
			if state.propagated {
				edge.PropagatedErrorCount++
			}
		}
		kind, id := splitFlowKey(flowKey)
		sessions = append(sessions, SessionGroup{ID: id, Kind: kind, UserID: userID, TraceIDs: sortedSet(traceIDs), Services: sortedSet(seenServices), StartedAt: flowRecords[0].Timestamp, EndedAt: flowRecords[len(flowRecords)-1].Timestamp, LogCount: len(flowRecords), ErrorCount: errorCount})
	}

	nodes := make([]ServiceNode, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, *node)
	}
	edges := make([]ServiceEdge, 0, len(edgeMap))
	for _, edge := range edgeMap {
		edges = append(edges, *edge)
	}
	slices.SortFunc(nodes, func(a, b ServiceNode) int { return strings.Compare(a.Service, b.Service) })
	slices.SortFunc(edges, func(a, b ServiceEdge) int {
		if c := strings.Compare(a.Source, b.Source); c != 0 {
			return c
		}
		return strings.Compare(a.Target, b.Target)
	})
	slices.SortFunc(sessions, func(a, b SessionGroup) int {
		if c := b.StartedAt.Compare(a.StartedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return nodes, edges, sessions
}

func graphFlowIdentity(record GraphRecord) (string, string) {
	if id := graphField(record.Fields, "session_id", "sessionid", "session-id"); id != "" {
		return "session:" + id, "session"
	}
	if id := graphField(record.Fields, "request_id", "requestid", "request-id", "correlation_id"); id != "" {
		return "request:" + id, "request"
	}
	if record.TraceID != "" {
		return "trace:" + record.TraceID, "trace"
	}
	if id := graphField(record.Fields, "user_id", "userid", "user-id"); id != "" {
		return "user:" + id, "user"
	}
	return "", ""
}

func splitFlowKey(key string) (string, string) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return "flow", key
	}
	return parts[0], parts[1]
}
func graphField(fields map[string]any, names ...string) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		for _, name := range names {
			if normalizeGraphFieldKey(key) == normalizeGraphFieldKey(name) {
				return strings.TrimSpace(toString(fields[key]))
			}
		}
	}
	return ""
}
func normalizeGraphFieldKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("_", "", "-", "", ".", "").Replace(value)
	return value
}
func toString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
func normalizeGraphService(value string) string { return strings.ToLower(strings.TrimSpace(value)) }
func isErrorLevel(level string) bool {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "error", "fatal", "critical", "panic":
		return true
	default:
		return false
	}
}
func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return result
}
