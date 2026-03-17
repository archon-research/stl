package anchorage_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client talks to the Anchorage collateral management API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new Anchorage API client.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchPackages retrieves all collateral management packages.
// Handles pagination automatically.
func (c *Client) FetchPackages(ctx context.Context) ([]Package, error) {
	var all []Package
	u := fmt.Sprintf("%s/v2/collateral_management/packages?limit=100", c.baseURL)

	for u != "" {
		var page PackagesResponse
		if err := c.doGet(ctx, u, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Data...)
		u = nextURL(page.Page)
	}

	return all, nil
}

// FetchOperations retrieves all collateral management operations.
// If afterID is non-empty (a cursor in "timestamp|id" format), only operations
// after that cursor are returned. Handles pagination automatically.
func (c *Client) FetchOperations(ctx context.Context, afterID string) ([]Operation, error) {
	var all []Operation
	u := fmt.Sprintf("%s/v2/collateral_management/operations?limit=100", c.baseURL)
	if afterID != "" {
		u += "&afterId=" + url.QueryEscape(afterID)
	}

	for u != "" {
		var page OperationsResponse
		if err := c.doGet(ctx, u, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Data...)
		u = nextURL(page.Page)
	}

	return all, nil
}

// doGet performs an authenticated GET request and decodes the JSON response.
func (c *Client) doGet(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Api-Access-Key", c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func nextURL(page PageInfo) string {
	if page.Next != nil {
		return *page.Next
	}
	return ""
}
