package connectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ParserClient struct {
	BaseURL string
	Client  *http.Client
}

type ParserRequest struct {
	SQL string `json:"sql"`
}

type ParserResponse struct {
	QueryHash    string   `json:"query_hash"`
	ReadsTables  []string `json:"reads_tables"`
	WritesTables []string `json:"writes_tables"`
	ParseError   string   `json:"parse_error,omitempty"`
}

func NewParserClient(baseURL string) *ParserClient {
	return &ParserClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (p *ParserClient) ParseSQL(sql string) (*ParserResponse, error) {
	payload, _ := json.Marshal(ParserRequest{SQL: sql})

	resp, err := p.Client.Post(
		fmt.Sprintf("%s/parse", p.BaseURL),
		"application/json",
		bytes.NewBuffer(payload),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var parsed ParserResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}

	return &parsed, nil
}
