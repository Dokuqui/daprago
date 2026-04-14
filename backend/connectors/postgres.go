package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Dokuqui/daprago/lineage"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type PostgresConnector struct {
	DB         *sql.DB
	Parser     *ParserClient
	GraphStore *lineage.GraphStore
	TenantID   string
}

type QueryStat struct {
	Query        string
	Calls        int64
	MeanExecTime float64
}

type DiscoverResult struct {
	QueriesProcessed        int `json:"queries_processed"`
	TablesDiscovered        int `json:"tables_discovered"`
	RelationshipsDiscovered int `json:"relationships_discovered"`
	TransformationsCreated  int `json:"transformations_created"`
}

func NewPostgresConnector(connectionString string, parser *ParserClient, graphStore *lineage.GraphStore, tenantID string) (*PostgresConnector, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresConnector{
		DB:         db,
		Parser:     parser,
		GraphStore: graphStore,
		TenantID:   tenantID,
	}, nil
}

func (p *PostgresConnector) Close() error {
	if p.DB != nil {
		return p.DB.Close()
	}
	return nil
}

func (p *PostgresConnector) DiscoverFromQueryHistory(ctx context.Context, limit int) (*DiscoverResult, error) {
	query := `
SELECT query, calls, mean_exec_time
FROM pg_stat_statements
WHERE query IS NOT NULL
  AND length(query) > 20
  AND query NOT ILIKE '%pg_stat_statements%'
  AND query NOT ILIKE '%information_schema%'
  AND query NOT ILIKE '%pg_catalog%'
ORDER BY calls DESC
LIMIT $1;
`

	rows, err := p.DB.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_statements: %w", err)
	}
	defer rows.Close()

	result := &DiscoverResult{}
	uniqueTables := map[string]bool{}

	for rows.Next() {
		var qs QueryStat
		if err := rows.Scan(&qs.Query, &qs.Calls, &qs.MeanExecTime); err != nil {
			continue
		}

		sqlText := strings.TrimSpace(qs.Query)
		if sqlText == "" {
			continue
		}

		parsed, err := p.Parser.ParseSQL(sqlText)
		if err != nil {
			continue
		}

		if len(parsed.ReadsTables) == 0 && len(parsed.WritesTables) == 0 {
			continue
		}

		transformation := &lineage.Transformation{
			ID:             uuid.NewString(),
			Type:           "SQL_QUERY",
			QueryHash:      parsed.QueryHash,
			QueryText:      sqlText,
			ExecutedAt:     time.Now(),
			ExecutionCount: qs.Calls,
			AvgDurationMs:  qs.MeanExecTime,
			TenantID:       p.TenantID,
		}

		if err := p.GraphStore.CreateTransformation(ctx, transformation); err != nil {
			continue
		}
		result.TransformationsCreated++

		for _, tbl := range parsed.ReadsTables {
			t := buildTable(tbl, p.TenantID)
			if t == nil {
				continue
			}
			if err := p.GraphStore.CreateTable(ctx, t); err == nil {
				uniqueTables[t.ID] = true
			}
			_ = p.GraphStore.CreateRelationship(ctx, transformation.ID, t.ID, "READS_FROM")
			result.RelationshipsDiscovered++
		}

		for _, tbl := range parsed.WritesTables {
			t := buildTable(tbl, p.TenantID)
			if t == nil {
				continue
			}
			if err := p.GraphStore.CreateTable(ctx, t); err == nil {
				uniqueTables[t.ID] = true
			}
			_ = p.GraphStore.CreateRelationship(ctx, transformation.ID, t.ID, "WRITES_TO")
			result.RelationshipsDiscovered++
		}

		result.QueriesProcessed++
	}

	result.TablesDiscovered = len(uniqueTables)
	return result, nil
}

func buildTable(fullName string, tenantID string) *lineage.Table {
	fullName = strings.TrimSpace(strings.ReplaceAll(fullName, `"`, ""))
	if fullName == "" {
		return nil
	}

	parts := strings.Split(fullName, ".")
	var schema, name string

	switch len(parts) {
	case 1:
		schema = "public"
		name = parts[0]
	case 2:
		schema = parts[0]
		name = parts[1]
	default:
		schema = parts[len(parts)-2]
		name = parts[len(parts)-1]
	}

	id := fmt.Sprintf("%s.%s", schema, name)

	return &lineage.Table{
		ID:        id,
		Name:      name,
		Schema:    schema,
		Database:  "postgres",
		TenantID:  tenantID,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}
}
