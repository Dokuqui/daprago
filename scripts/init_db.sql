CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE SCHEMA IF NOT EXISTS daprago;

CREATE TABLE daprago.tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    api_key VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    plan VARCHAR(50) DEFAULT 'starter'
);

CREATE TABLE daprago.data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES daprago.tenants(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    connection_string TEXT NOT NULL,
    last_sync TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daprago.discovery_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES daprago.tenants(id) ON DELETE CASCADE,
    data_source_id UUID NOT NULL REFERENCES daprago.data_sources(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL,
    tables_discovered INT DEFAULT 0,
    relationships_discovered INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_data_sources_tenant ON daprago.data_sources(tenant_id);
CREATE INDEX idx_discovery_runs_tenant ON daprago.discovery_runs(tenant_id);
CREATE INDEX idx_discovery_runs_source ON daprago.discovery_runs(data_source_id);

INSERT INTO daprago.tenants (name, api_key, plan)
VALUES ('local-dev', 'dev-api-key-12345', 'developer')
ON CONFLICT (name) DO NOTHING;
