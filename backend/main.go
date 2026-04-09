package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	neo4jURI := os.Getenv("NEO4J_URI")
	neo4jUser := os.Getenv("NEO4J_USER")
	neo4jPassword := os.Getenv("NEO4J_PASSWORD")
	appPort := os.Getenv("APP_PORT")
	if appPort == "" {
		appPort = "8080"
	}

	driver, err := neo4j.NewDriver(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		log.Fatalf("Failed to connect to Neo4j: %v", err)
	}
	defer driver.Close(context.Background())

	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		log.Fatalf("Neo4j connectivity check failed: %v", err)
	}
	fmt.Println("Connected to Neo4j")

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

	setupRoutes(e, driver)

	fmt.Printf("DataGov API starting on port %s\n", appPort)
	if err := e.Start(":" + appPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func setupRoutes(e *echo.Echo, driver neo4j.Driver) {
	e.GET("/api/v1/tables", func(c *echo.Context) error {
		return c.JSON(200, map[string]string{
			"message": "Tables endpoint - coming soon",
		})
	})
}
