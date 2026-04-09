package lineage

import "time"

type Table struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Schema    string    `json:"schema"`
	Database  string    `json:"database"`
	CreatedAt time.Time `json:"created_at"`
	LastSeen  time.Time `json:"last_seen"`
	TenantID  string    `json:"tenant_id"`
	RowCount  int64     `json:"row_count,omitempty"`
}

type Column struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	TableID  string `json:"table_id"`
	TenantID string `json:"tenant_id"`
}

type Transformation struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	QueryHash      string    `json:"query_hash"`
	QueryText      string    `json:"query_text,omitempty"`
	ExecutedAt     time.Time `json:"executed_at"`
	ExecutionCount int64     `json:"execution_count"`
	AvgDurationMs  float64   `json:"avg_duration_ms"`
	ReadsTables    []*Table  `json:"reads_tables,omitempty"`
	WritesTables   []*Table  `json:"writes_tables,omitempty"`
	TenantID       string    `json:"tenant_id"`
}

type Lineage struct {
	SourceTable      *Table    `json:"source_table"`
	TargetTable      *Table    `json:"target_table"`
	Transformations  []string  `json:"transformations"`
	Path             []*Table  `json:"path"`
	Distance         int       `json:"distance"`
	LastDiscoveredAt time.Time `json:"last_discovered_at"`
}

// DataSource represents a connected database
type DataSource struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	Name             string    `json:"name"`
	Type             string    `json:"type"`
	ConnectionString string    `json:"-"`
	LastSync         time.Time `json:"last_sync"`
	CreatedAt        time.Time `json:"created_at"`
}

type DiscoveryRun struct {
	ID                      string    `json:"id"`
	TenantID                string    `json:"tenant_id"`
	DataSourceID            string    `json:"data_source_id"`
	Status                  string    `json:"status"`
	TablesDiscovered        int       `json:"tables_discovered"`
	RelationshipsDiscovered int       `json:"relationships_discovered"`
	StartedAt               time.Time `json:"started_at"`
	CompletedAt             time.Time `json:"completed_at,omitempty"`
	ErrorMessage            string    `json:"error_message,omitempty"`
}
