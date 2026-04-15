package api

import (
	"net/http"

	"github.com/Dokuqui/daprago/lineage"
	"github.com/labstack/echo/v5"
)

type ListHandler struct {
	GraphStore *lineage.GraphStore
	TenantID   string
}

func NewListHandler(graphStore *lineage.GraphStore, tenantID string) *ListHandler {
	return &ListHandler{
		GraphStore: graphStore,
		TenantID:   tenantID,
	}
}

func (h *ListHandler) ListTables(c *echo.Context) error {
	limit, offset, err := ParseLimitOffset(c.QueryParam("limit"), c.QueryParam("offset"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "INVALID_PAGINATION", Message: "limit/offset must be valid integers"},
		})
	}

	search := c.QueryParam("search")
	schema := c.QueryParam("schema")

	total, err := h.GraphStore.CountTables(c.Request().Context(), h.TenantID, search, schema)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "TABLE_COUNT_FAILED", Message: err.Error()},
		})
	}

	tables, err := h.GraphStore.ListTablesPaginated(c.Request().Context(), h.TenantID, limit, offset, search, schema)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "TABLE_LIST_FAILED", Message: err.Error()},
		})
	}

	return c.JSON(http.StatusOK, SuccessResponse{
		Data:  tables,
		Meta:  &Meta{Limit: limit, Offset: offset, Total: total},
		Error: nil,
	})
}

func (h *ListHandler) ListTransformations(c *echo.Context) error {
	limit, offset, err := ParseLimitOffset(c.QueryParam("limit"), c.QueryParam("offset"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "INVALID_PAGINATION", Message: "limit/offset must be valid integers"},
		})
	}

	total, err := h.GraphStore.CountTransformations(c.Request().Context(), h.TenantID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "TRANSFORMATION_COUNT_FAILED", Message: err.Error()},
		})
	}

	items, err := h.GraphStore.ListTransformationsPaginated(c.Request().Context(), h.TenantID, limit, offset)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, ErrorResponse{
			Data: nil, Meta: nil,
			Error: ErrorBody{Code: "TRANSFORMATION_LIST_FAILED", Message: err.Error()},
		})
	}

	return c.JSON(http.StatusOK, SuccessResponse{
		Data:  items,
		Meta:  &Meta{Limit: limit, Offset: offset, Total: total},
		Error: nil,
	})
}
