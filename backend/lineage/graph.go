package lineage

import (
	"context"
	"fmt"
	"strconv"
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
				"tenant_id":         "local-dev",
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

func (g *GraphStore) CountTables(ctx context.Context, tenantID string, search string, schema string) (int, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		query := `
MATCH (t:Table {tenant_id: $tenant_id})
WHERE ($search = '' OR toLower(t.name) CONTAINS toLower($search))
  AND ($schema = '' OR toLower(t.schema) = toLower($schema))
RETURN count(t) as total
`
		result, err := tx.Run(ctx, query, map[string]interface{}{
			"tenant_id": tenantID,
			"search":    search,
			"schema":    schema,
		})
		if err != nil {
			return 0, err
		}
		if result.Next(ctx) {
			val := result.Record().Values[0]
			switch v := val.(type) {
			case int64:
				return int(v), nil
			case int:
				return v, nil
			default:
				return 0, nil
			}
		}
		return 0, nil
	})
	if err != nil {
		return 0, err
	}

	total, ok := res.(int)
	if !ok {
		return 0, nil
	}
	return total, nil
}

func (g *GraphStore) ListTablesPaginated(ctx context.Context, tenantID string, limit int, offset int, search string, schema string) ([]*Table, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		query := `
MATCH (t:Table {tenant_id: $tenant_id})
WHERE ($search = '' OR toLower(t.name) CONTAINS toLower($search))
  AND ($schema = '' OR toLower(t.schema) = toLower($schema))
RETURN t.id, t.name, t.schema, t.database, t.created_at, t.last_seen
ORDER BY t.schema ASC, t.name ASC
SKIP $offset
LIMIT $limit
`
		result, err := tx.Run(ctx, query, map[string]interface{}{
			"tenant_id": tenantID,
			"search":    search,
			"schema":    schema,
			"offset":    offset,
			"limit":     limit,
		})
		if err != nil {
			return nil, err
		}

		var tables []*Table
		for result.Next(ctx) {
			r := result.Record()
			t := &Table{
				ID:       toString(r.Values[0]),
				Name:     toString(r.Values[1]),
				Schema:   toString(r.Values[2]),
				Database: toString(r.Values[3]),
				TenantID: tenantID,
			}
			tables = append(tables, t)
		}
		return tables, nil
	})
	if err != nil {
		return nil, err
	}

	tables, ok := res.([]*Table)
	if !ok {
		return []*Table{}, nil
	}
	return tables, nil
}

func (g *GraphStore) CountTransformations(ctx context.Context, tenantID string) (int, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx,
			`MATCH (tr:Transformation {tenant_id: $tenant_id}) RETURN count(tr)`,
			map[string]interface{}{"tenant_id": tenantID},
		)
		if err != nil {
			return 0, err
		}
		if result.Next(ctx) {
			if v, ok := result.Record().Values[0].(int64); ok {
				return int(v), nil
			}
		}
		return 0, nil
	})
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}

func (g *GraphStore) ListTransformationsPaginated(ctx context.Context, tenantID string, limit int, offset int) ([]*Transformation, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, `
MATCH (tr:Transformation {tenant_id: $tenant_id})
RETURN tr.id, tr.type, tr.query_hash, tr.execution_count, tr.avg_duration_ms, tr.executed_at
ORDER BY tr.executed_at DESC
SKIP $offset
LIMIT $limit
`, map[string]interface{}{
			"tenant_id": tenantID,
			"offset":    offset,
			"limit":     limit,
		})
		if err != nil {
			return nil, err
		}

		var items []*Transformation
		for result.Next(ctx) {
			r := result.Record()
			item := &Transformation{
				ID:             toString(r.Values[0]),
				Type:           toString(r.Values[1]),
				QueryHash:      toString(r.Values[2]),
				ExecutionCount: toInt64(r.Values[3]),
				AvgDurationMs:  toFloat64(r.Values[4]),
				TenantID:       tenantID,
			}
			items = append(items, item)
		}
		return items, nil
	})
	if err != nil {
		return nil, err
	}

	items, ok := res.([]*Transformation)
	if !ok {
		return []*Transformation{}, nil
	}
	return items, nil
}

func (g *GraphStore) GetGraphForTable(ctx context.Context, tenantID string, tableID string, depth int, direction string) (*GraphPayload, error) {
	session := g.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	if depth < 1 {
		depth = 1
	}
	if depth > 5 {
		depth = 5
	}
	depthStr := strconv.Itoa(depth)

	var cypher string

	switch direction {
	case "upstream":
		// Upstream around root:
		// sourceTable -[:READS_FROM]-> tr -[:WRITES_TO]-> root
		cypher = `
MATCH (root:Table {id: $table_id, tenant_id: $tenant_id})
OPTIONAL MATCH p=(src:Table {tenant_id: $tenant_id})<-[:READS_FROM*1..` + depthStr + `]-(tr:Transformation {tenant_id: $tenant_id})-[:WRITES_TO]->(root)
WITH collect(p) AS paths
UNWIND paths AS path
WITH path WHERE path IS NOT NULL
UNWIND relationships(path) AS rel
WITH DISTINCT startNode(rel) AS s, endNode(rel) AS t, type(rel) AS relType
RETURN
  collect(DISTINCT {
    id: s.id,
    label: CASE WHEN 'Table' IN labels(s) THEN coalesce(s.schema,'') + '.' + coalesce(s.name,'') ELSE coalesce(s.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(s) THEN 'table' ELSE 'transformation' END
  }) +
  collect(DISTINCT {
    id: t.id,
    label: CASE WHEN 'Table' IN labels(t) THEN coalesce(t.schema,'') + '.' + coalesce(t.name,'') ELSE coalesce(t.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(t) THEN 'table' ELSE 'transformation' END
  }) AS nodes,
  collect(DISTINCT {
    source: s.id,
    target: t.id,
    type: relType
  }) AS edges
`

	case "downstream":
		// Downstream around root:
		// root <-[:READS_FROM]- tr -[:WRITES_TO*1..depth]-> table
		cypher = `
MATCH (root:Table {id: $table_id, tenant_id: $tenant_id})
OPTIONAL MATCH p=(root)<-[:READS_FROM]-(tr:Transformation {tenant_id: $tenant_id})-[:WRITES_TO*1..` + depthStr + `]->(dst:Table {tenant_id: $tenant_id})
WITH collect(p) AS paths
UNWIND paths AS path
WITH path WHERE path IS NOT NULL
UNWIND relationships(path) AS rel
WITH DISTINCT startNode(rel) AS s, endNode(rel) AS t, type(rel) AS relType
RETURN
  collect(DISTINCT {
    id: s.id,
    label: CASE WHEN 'Table' IN labels(s) THEN coalesce(s.schema,'') + '.' + coalesce(s.name,'') ELSE coalesce(s.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(s) THEN 'table' ELSE 'transformation' END
  }) +
  collect(DISTINCT {
    id: t.id,
    label: CASE WHEN 'Table' IN labels(t) THEN coalesce(t.schema,'') + '.' + coalesce(t.name,'') ELSE coalesce(t.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(t) THEN 'table' ELSE 'transformation' END
  }) AS nodes,
  collect(DISTINCT {
    source: s.id,
    target: t.id,
    type: relType
  }) AS edges
`

	default: // both
		cypher = `
MATCH (root:Table {id: $table_id, tenant_id: $tenant_id})
OPTIONAL MATCH p1=(src:Table {tenant_id: $tenant_id})<-[:READS_FROM*1..` + depthStr + `]-(upTr:Transformation {tenant_id: $tenant_id})-[:WRITES_TO]->(root)
OPTIONAL MATCH p2=(root)<-[:READS_FROM]-(downTr:Transformation {tenant_id: $tenant_id})-[:WRITES_TO*1..` + depthStr + `]->(dst:Table {tenant_id: $tenant_id})
WITH collect(p1) + collect(p2) AS paths
UNWIND paths AS path
WITH path WHERE path IS NOT NULL
UNWIND relationships(path) AS rel
WITH DISTINCT startNode(rel) AS s, endNode(rel) AS t, type(rel) AS relType
RETURN
  collect(DISTINCT {
    id: s.id,
    label: CASE WHEN 'Table' IN labels(s) THEN coalesce(s.schema,'') + '.' + coalesce(s.name,'') ELSE coalesce(s.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(s) THEN 'table' ELSE 'transformation' END
  }) +
  collect(DISTINCT {
    id: t.id,
    label: CASE WHEN 'Table' IN labels(t) THEN coalesce(t.schema,'') + '.' + coalesce(t.name,'') ELSE coalesce(t.type,'TRANSFORMATION') END,
    type: CASE WHEN 'Table' IN labels(t) THEN 'table' ELSE 'transformation' END
  }) AS nodes,
  collect(DISTINCT {
    source: s.id,
    target: t.id,
    type: relType
  }) AS edges
`
	}

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, err := tx.Run(ctx, cypher, map[string]interface{}{
			"tenant_id": tenantID,
			"table_id":  tableID,
			"depth":     depth,
		})
		if err != nil {
			return nil, err
		}

		if !result.Next(ctx) {
			return &GraphPayload{
				Nodes: []GraphNode{},
				Edges: []GraphEdge{},
			}, nil
		}

		record := result.Record()

		nodes := []GraphNode{}
		edges := []GraphEdge{}

		if len(record.Values) > 0 {
			nodes = parseGraphNodes(record.Values[0])
		}
		if len(record.Values) > 1 {
			edges = parseGraphEdges(record.Values[1])
		}

		nodes = dedupeNodes(nodes)
		edges = dedupeEdges(edges)

		return &GraphPayload{
			Nodes: nodes,
			Edges: edges,
		}, nil
	})

	if err != nil {
		return nil, err
	}

	payload, ok := res.(*GraphPayload)
	if !ok {
		return &GraphPayload{Nodes: []GraphNode{}, Edges: []GraphEdge{}}, nil
	}
	return payload, nil
}

// Helpers
func parseGraphNodes(v interface{}) []GraphNode {
	out := []GraphNode{}
	arr, ok := v.([]interface{})
	if !ok {
		return out
	}

	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		node := GraphNode{
			ID:    toString(m["id"]),
			Label: toString(m["label"]),
			Type:  toString(m["type"]),
		}
		if node.ID != "" {
			out = append(out, node)
		}
	}
	return out
}

func parseGraphEdges(v interface{}) []GraphEdge {
	out := []GraphEdge{}
	arr, ok := v.([]interface{})
	if !ok {
		return out
	}

	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		edge := GraphEdge{
			Source: toString(m["source"]),
			Target: toString(m["target"]),
			Type:   toString(m["type"]),
		}
		if edge.Source != "" && edge.Target != "" {
			out = append(out, edge)
		}
	}
	return out
}

func dedupeNodes(nodes []GraphNode) []GraphNode {
	seen := map[string]bool{}
	out := []GraphNode{}
	for _, n := range nodes {
		if !seen[n.ID] {
			seen[n.ID] = true
			out = append(out, n)
		}
	}
	return out
}

func dedupeEdges(edges []GraphEdge) []GraphEdge {
	seen := map[string]bool{}
	out := []GraphEdge{}
	for _, e := range edges {
		key := e.Source + "->" + e.Target + ":" + e.Type
		if !seen[key] {
			seen[key] = true
			out = append(out, e)
		}
	}
	return out
}

func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
