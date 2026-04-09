package lineage

import (
	"context"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

type GraphStore struct {
	driver neo4j.DriverWithContext
}

func NewGraphStore(driver neo4j.DriverWithContext) *GraphStore {
	return &GraphStore{driver: driver}
}

// CreateTable creates or updates a table node
func (g *GraphStore) CreateTable(ctx context.Context, table *Table) error {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, _ := tx.Run(ctx,
			`MERGE (t:Table {id: $id, tenant_id: $tenant_id})
			 ON CREATE SET 
				t.name = $name,
				t.schema = $schema,
				t.database = $database,
				t.created_at = $created_at,
				t.last_seen = $last_seen
			 ON MATCH SET
				t.last_seen = $last_seen
			 RETURN t`,
			map[string]interface{}{
				"id":         table.ID,
				"tenant_id":  table.TenantID,
				"name":       table.Name,
				"schema":     table.Schema,
				"database":   table.Database,
				"created_at": table.CreatedAt.Unix(),
				"last_seen":  time.Now().Unix(),
			},
		)
		return result.Consume(ctx)
	})

	return err
}

// GetTable retrieves a table by ID
func (g *GraphStore) GetTable(ctx context.Context, tableID string, tenantID string) (*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH (t:Table {id: $id, tenant_id: $tenant_id})
			 RETURN t.id, t.name, t.schema, t.database, t.created_at, t.last_seen`,
			map[string]interface{}{
				"id":        tableID,
				"tenant_id": tenantID,
			},
		)
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			record := result.Record()
			table := &Table{
				ID:       record.Values[0].(string),
				Name:     record.Values[1].(string),
				Schema:   record.Values[2].(string),
				Database: record.Values[3].(string),
				TenantID: tenantID,
			}
			return table, nil
		}
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	if table, ok := result.(*Table); ok {
		return table, nil
	}
	return nil, nil
}

// ListTables lists all tables for a tenant
func (g *GraphStore) ListTables(ctx context.Context, tenantID string, limit int) ([]*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	results, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH (t:Table {tenant_id: $tenant_id})
			 RETURN t.id, t.name, t.schema, t.database, t.created_at, t.last_seen
			 LIMIT $limit`,
			map[string]interface{}{
				"tenant_id": tenantID,
				"limit":     limit,
			},
		)
		if err != nil {
			return nil, err
		}

		var tables []*Table
		for result.Next(ctx) {
			record := result.Record()
			table := &Table{
				ID:       record.Values[0].(string),
				Name:     record.Values[1].(string),
				Schema:   record.Values[2].(string),
				Database: record.Values[3].(string),
				TenantID: tenantID,
			}
			tables = append(tables, table)
		}
		return tables, nil
	})

	if err != nil {
		return nil, err
	}

	return results.([]*Table), nil
}

// CreateRelationship creates a reads/writes relationship between transformation and table
func (g *GraphStore) CreateRelationship(ctx context.Context, transformationID string, tableID string, relationType string) error {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	query := fmt.Sprintf(
		`MATCH (t:Transformation {id: $transformation_id})
		 MATCH (table:Table {id: $table_id})
		 CREATE (t)-[:%s]->(table)
		 RETURN t, table`,
		relationType,
	)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return tx.Run(ctx,
			query,
			map[string]interface{}{
				"transformation_id": transformationID,
				"table_id":          tableID,
			},
		)
	})

	return err
}

// GetLineage finds all tables upstream of a given table (what feeds into it)
func (g *GraphStore) GetLineage(ctx context.Context, tableID string, tenantID string, depth int) ([]*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	results, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH (source:Table)-[:READS_FROM|DERIVES_FROM*1..$depth]->(target:Table {id: $table_id, tenant_id: $tenant_id})
			 RETURN DISTINCT source.id, source.name, source.schema, source.database
			 ORDER BY source.name`,
			map[string]interface{}{
				"table_id":  tableID,
				"tenant_id": tenantID,
				"depth":     depth,
			},
		)
		if err != nil {
			return nil, err
		}

		var tables []*Table
		for result.Next(ctx) {
			record := result.Record()
			table := &Table{
				ID:       record.Values[0].(string),
				Name:     record.Values[1].(string),
				Schema:   record.Values[2].(string),
				Database: record.Values[3].(string),
				TenantID: tenantID,
			}
			tables = append(tables, table)
		}
		return tables, nil
	})

	if err != nil {
		return nil, err
	}

	return results.([]*Table), nil
}

// GetDownstream finds all tables downstream (what this table feeds into)
func (g *GraphStore) GetDownstream(ctx context.Context, tableID string, tenantID string, depth int) ([]*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	results, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH (source:Table {id: $table_id, tenant_id: $tenant_id})-[:WRITES_TO|DERIVES_FROM*1..$depth]->(target:Table)
			 RETURN DISTINCT target.id, target.name, target.schema, target.database
			 ORDER BY target.name`,
			map[string]interface{}{
				"table_id":  tableID,
				"tenant_id": tenantID,
				"depth":     depth,
			},
		)
		if err != nil {
			return nil, err
		}

		var tables []*Table
		for result.Next(ctx) {
			record := result.Record()
			table := &Table{
				ID:       record.Values[0].(string),
				Name:     record.Values[1].(string),
				Schema:   record.Values[2].(string),
				Database: record.Values[3].(string),
				TenantID: tenantID,
			}
			tables = append(tables, table)
		}
		return tables, nil
	})

	if err != nil {
		return nil, err
	}

	return results.([]*Table), nil
}

// FindPath finds shortest path between two tables
func (g *GraphStore) FindPath(ctx context.Context, fromID string, toID string, tenantID string) ([]*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	results, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH path = shortestPath(
				(from:Table {id: $from_id, tenant_id: $tenant_id})-[:READS_FROM|WRITES_TO|DERIVES_FROM*]-(to:Table {id: $to_id, tenant_id: $tenant_id})
			)
			RETURN [node IN nodes(path) WHERE node:Table | {id: node.id, name: node.name, schema: node.schema, database: node.database}] as tables`,
			map[string]interface{}{
				"from_id":   fromID,
				"to_id":     toID,
				"tenant_id": tenantID,
			},
		)
		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			record := result.Record()
			value := record.Values[0]
			if value != nil {
				return value, nil
			}
		}
		return []*Table{}, nil
	})

	if err != nil {
		return nil, err
	}

	if tables, ok := results.([]*Table); ok {
		return tables, nil
	}
	return []*Table{}, nil
}

// CreateTransformation creates a transformation node
func (g *GraphStore) CreateTransformation(ctx context.Context, transformation *Transformation) error {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return tx.Run(ctx,
			`MERGE (t:Transformation {id: $id, tenant_id: $tenant_id})
			 ON CREATE SET
				t.type = $type,
				t.query_hash = $query_hash,
				t.query_text = $query_text,
				t.executed_at = $executed_at,
				t.execution_count = $execution_count,
				t.avg_duration_ms = $avg_duration_ms
			 RETURN t`,
			map[string]interface{}{
				"id":              transformation.ID,
				"tenant_id":       transformation.TenantID,
				"type":            transformation.Type,
				"query_hash":      transformation.QueryHash,
				"query_text":      transformation.QueryText,
				"executed_at":     transformation.ExecutedAt.Unix(),
				"execution_count": transformation.ExecutionCount,
				"avg_duration_ms": transformation.AvgDurationMs,
			},
		)
	})

	return err
}

// GetStatistics returns graph statistics
func (g *GraphStore) GetStatistics(ctx context.Context, tenantID string) (map[string]interface{}, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	stats := make(map[string]interface{})

	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, _ := tx.Run(ctx,
			`MATCH (t:Table {tenant_id: $tenant_id}) RETURN count(t) as count`,
			map[string]interface{}{"tenant_id": tenantID},
		)
		if result.Next(ctx) {
			stats["total_tables"] = result.Record().Values[0]
		}

		result, _ = tx.Run(ctx,
			`MATCH (t:Table {tenant_id: $tenant_id})-[r:READS_FROM|WRITES_TO|DERIVES_FROM]->() 
			 RETURN count(r) as count`,
			map[string]interface{}{"tenant_id": tenantID},
		)
		if result.Next(ctx) {
			stats["total_relationships"] = result.Record().Values[0]
		}

		result, _ = tx.Run(ctx,
			`MATCH (tr:Transformation {tenant_id: $tenant_id}) RETURN count(tr) as count`,
			map[string]interface{}{"tenant_id": tenantID},
		)
		if result.Next(ctx) {
			stats["total_transformations"] = result.Record().Values[0]
		}

		return nil, nil
	})

	return stats, err
}
