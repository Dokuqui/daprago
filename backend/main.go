package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Dokuqui/daprago/api"
	"github.com/Dokuqui/daprago/config"
	"github.com/Dokuqui/daprago/lineage"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := config.LoadConfig()
	fmt.Println(cfg.String())

	ctx := context.Background()
	driver, err := neo4j.NewDriver(cfg.Neo4jURI, neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, ""))
	if err != nil {
		log.Fatalf("Failed to connect to Neo4j: %v", err)
	}
	defer driver.Close(ctx)

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		log.Fatalf("Neo4j connectivity check failed: %v", err)
	}
	fmt.Println("Connected to Neo4j")

	graphStore := lineage.NewGraphStore(driver)

	if cfg.AppEnv == "dev-seed" {
		testGraphStore(ctx, graphStore)
	}

	e := echo.New()

	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
	}))

	e.GET("/api/health", func(c *echo.Context) error {
		return c.JSON(200, map[string]string{
			"status": "ok",
			"neo4j":  "connected",
		})
	})

	setupRoutes(e, graphStore, cfg.TenantID)

	fmt.Printf("DataGov API starting on port %s\n", cfg.AppPort)
	if err := e.Start(":" + cfg.AppPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func setupRoutes(e *echo.Echo, graphStore *lineage.GraphStore, tenantID string) {
	listHandler := api.NewListHandler(graphStore, tenantID)

	e.GET("/api/v1/tables", listHandler.ListTables)

	e.GET("/api/v1/transformations", listHandler.ListTransformations)

	e.GET("/api/v1/tables/:id", func(c *echo.Context) error {
		tableID := c.Param("id")
		table, err := graphStore.GetTable(c.Request().Context(), tableID, tenantID)
		if err != nil {
			return c.JSON(500, map[string]string{"error": err.Error()})
		}
		if table == nil {
			return c.JSON(404, map[string]string{"error": "table not found"})
		}
		return c.JSON(200, table)
	})

	e.GET("/api/v1/lineage/:id", func(c *echo.Context) error {
		tableID := c.Param("id")
		lineageData, err := graphStore.GetLineage(c.Request().Context(), tableID, tenantID, 5)
		if err != nil {
			return c.JSON(500, map[string]string{"error": err.Error()})
		}
		return c.JSON(200, map[string]interface{}{
			"upstream": lineageData,
		})
	})

	e.GET("/api/v1/downstream/:id", func(c *echo.Context) error {
		tableID := c.Param("id")
		downstream, err := graphStore.GetDownstream(c.Request().Context(), tableID, tenantID, 5)
		if err != nil {
			return c.JSON(500, map[string]string{"error": err.Error()})
		}
		return c.JSON(200, map[string]interface{}{
			"downstream": downstream,
		})
	})

	e.GET("/api/v1/stats", func(c *echo.Context) error {
		stats, err := graphStore.GetStatistics(c.Request().Context(), tenantID)
		if err != nil {
			return c.JSON(500, map[string]string{"error": err.Error()})
		}
		return c.JSON(200, stats)
	})

	discoverHandler := api.NewDiscoverHandler(graphStore, tenantID)
	e.POST("/api/v1/discover/postgres", discoverHandler.DiscoverPostgres)
}

func testGraphStore(ctx context.Context, graphStore *lineage.GraphStore) {
	tenantID := "local-dev"

	tables := []*lineage.Table{
		{ID: "tbl_001", Name: "users", Schema: "public", Database: "production", TenantID: tenantID},
		{ID: "tbl_002", Name: "orders", Schema: "public", Database: "production", TenantID: tenantID},
		{ID: "tbl_003", Name: "user_orders", Schema: "analytics", Database: "production", TenantID: tenantID},
	}

	for _, table := range tables {
		if err := graphStore.CreateTable(ctx, table); err != nil {
			log.Printf("Error creating table %s: %v", table.Name, err)
		}
	}

	fmt.Println("Sample data loaded into Neo4j")

	stats, _ := graphStore.GetStatistics(ctx, tenantID)
	fmt.Printf("Graph Statistics: %v\n", stats)
}
