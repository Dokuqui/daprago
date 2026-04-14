package api

import (
	"net/http"
	"os"

	"github.com/Dokuqui/daprago/connectors"
	"github.com/Dokuqui/daprago/lineage"
	"github.com/labstack/echo/v5"
)

type DiscoverHandler struct {
	GraphStore *lineage.GraphStore
	TenantID   string
}

type PostgresDiscoverRequest struct {
	ConnectionString string `json:"connection_string"`
	Limit            int    `json:"limit"`
}

func NewDiscoverHandler(graphStore *lineage.GraphStore, tenantID string) *DiscoverHandler {
	return &DiscoverHandler{
		GraphStore: graphStore,
		TenantID:   tenantID,
	}
}

func (h *DiscoverHandler) DiscoverPostgres(c *echo.Context) error {
	var req PostgresDiscoverRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	if req.ConnectionString == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "connection_string is required"})
	}
	if req.Limit <= 0 {
		req.Limit = 200
	}

	parserURL := os.Getenv("PARSER_URL")
	if parserURL == "" {
		parserURL = "http://localhost:8090"
	}

	parserClient := connectors.NewParserClient(parserURL)

	pgConnector, err := connectors.NewPostgresConnector(
		req.ConnectionString,
		parserClient,
		h.GraphStore,
		h.TenantID,
	)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	defer pgConnector.Close()

	result, err := pgConnector.DiscoverFromQueryHistory(c.Request().Context(), req.Limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"status": "completed",
		"result": result,
	})
}
