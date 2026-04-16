package api

import (
	"net/http"
	"strconv"

	"github.com/Dokuqui/daprago/lineage"
	"github.com/labstack/echo/v5"
)

type GraphHandler struct {
	GraphStore *lineage.GraphStore
	TenantID   string
}

func NewGraphHandler(graphStore *lineage.GraphStore, tenantID string) *GraphHandler {
	return &GraphHandler{
		GraphStore: graphStore,
		TenantID:   tenantID,
	}
}

func (h *GraphHandler) GetGraph(c *echo.Context) error {
	tableID := c.Param("tableId")
	if tableID == "" {
		return c.JSON(http.StatusBadRequest, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "MISSING_TABLE_ID", Message: "tableId is required"},
		})
	}

	depth := 2
	if d := c.QueryParam("depth"); d != "" {
		parsed, err := strconv.Atoi(d)
		if err != nil {
			return c.JSON(http.StatusBadRequest, ErrorResponse{
				Data: nil, Meta: nil,
				Error: ErrorBody{Code: "INVALID_DEPTH", Message: "depth must be an integer"},
			})
		}
		depth = parsed
	}

	direction := c.QueryParam("direction")
	if direction == "" {
		direction = "both"
	}
	if direction != "upstream" && direction != "downstream" && direction != "both" {
		return c.JSON(http.StatusBadRequest, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "INVALID_DIRECTION", Message: "direction must be upstream, downstream, or both"},
		})
	}

	graph, err := h.GraphStore.GetGraphForTable(c.Request().Context(), h.TenantID, tableID, depth, direction)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "GRAPH_QUERY_FAILED", Message: err.Error()},
		})
	}

	return c.JSON(http.StatusOK, SuccessResponse{
		Data:  graph,
		Meta:  nil,
		Error: nil,
	})
}
