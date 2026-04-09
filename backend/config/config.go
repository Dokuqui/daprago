package config

import (
	"fmt"
	"os"
)

type Config struct {
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	PostgresURL   string
	AppPort       string
	AppEnv        string
	TenantID      string
}

func LoadConfig() Config {
	return Config{
		Neo4jURI:      os.Getenv("NEO4J_URI"),
		Neo4jUser:     os.Getenv("NEO4J_USER"),
		Neo4jPassword: os.Getenv("NEO4J_PASSWORD"),
		PostgresURL:   os.Getenv("POSTGRES_URL"),
		AppPort:       getEnvOrDefault("APP_PORT", "8080"),
		AppEnv:        getEnvOrDefault("APP_ENV", "development"),
		TenantID:      getEnvOrDefault("TENANT_ID", "local-dev"),
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultVal
}

func (c Config) String() string {
	return fmt.Sprintf(`
	Config:
	- Environment: %s
	- Port: %s
	- Neo4j: %s
	- Postgres: %s
	- Tenant: %s
	`, c.AppEnv, c.AppPort, c.Neo4jURI, c.PostgresURL, c.TenantID)
}
