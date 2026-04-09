# 🏛️ DataGov (daprago)
**Open-Source Data Lineage & Governance Platform**

Auto-discover data lineage from SQL queries, track data flows, and simplify compliance audits.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)

---

## Quick Start

### Prerequisites
- **Docker & Docker Compose**
- **Go** 1.25+
- **Node.js** 18+
- **Python** 3.9+

### Local Development (5 minutes)

```bash
# 1. Clone and navigate
git clone [https://github.com/Dokuqui/daprago.git](https://github.com/Dokuqui/daprago.git)
cd daprago

# 2. Start services (Neo4j, Postgres, API)
docker-compose up -d

# 3. Verify everything is running
curl http://localhost:8080/api/health
# Expected: {"status":"ok","neo4j":"connected"}
```

### Access Points

  * **API Base URL:** `http://localhost:8080/api/v1/`
  * **Neo4j Browser:** `http://localhost:7474`
      * *User:* `neo4j` | *Password:* `daprago_dev_password`
  * **pgAdmin:** `http://localhost:5050`
      * *Email:* `admin@daprago.local` | *Password:* `admin`

-----

## Architecture

DataGov relies on a multi-service architecture separating metadata, graph relationships, and parsing logic:

```text
DataGov
├── Neo4j (Graph DB)        → Stores lineage relationships & nodes
├── Postgres (Metadata DB)  → User, tenant, and source management
├── Go Backend (API)        → REST API & core business logic
├── Python (Parser)         → SQL parsing & lineage extraction
├── React (Frontend)        → Visualization & UI (D3.js)
└── CLI Tool                → CLI interactions (e.g., daprago discover)
```

### Project Structure

```text
daprago/
├── backend/          # Go API server (Echo v5)
├── frontend/         # React UI
├── lineage_parser/   # Python SQL parser
├── cli/              # Command-line utility
└── k8s/              # Kubernetes deployment manifests
```

-----

## Development Guide

### Backend (Go)

Ensure you have a `.env` file configured in the root directory.

```bash
cd backend
go run main.go
```

### Frontend (React)

```bash
cd frontend
npm install
npm start
```

### Database Access (CLI)

**Neo4j Cypher Shell:**

```bash
docker exec -it daprago_neo4j cypher-shell -u neo4j -p daprago_dev_password
```

**Postgres CLI:**

```bash
docker exec -it daprago_postgres psql -U daprago -d daprago_metadata
```

-----

## Features & Roadmap (MVP)

  - [ ] Postgres connector (query history → lineage)
  - [ ] Neo4j graph storage
  - [ ] REST API for lineage queries
  - [ ] React UI with D3.js visualization
  - [ ] CLI tool (`daprago discover`)
  - [ ] Multi-tenant architecture
  - [ ] Authentication & RBAC

-----

## License & Authors

This project is licensed under the **MIT License**.

**Authors:**

  * [@Dokuqui](https://www.google.com/search?q=https://github.com/Dokuqui)
