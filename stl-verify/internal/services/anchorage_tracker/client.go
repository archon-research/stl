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

// maxErrorBodySize limits how much of an error response body we read.
const maxErrorBodySize = 4096

// Client talks to the Anchorage collateral management API.
type Client struct {
	baseURL    string
	baseHost   string // parsed host from baseURL, used to validate pagination URLs
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new Anchorage API client.
func NewClient(baseURL, apiKey string) *Client {
	parsed, _ := url.Parse(baseURL)
	var host string
	if parsed != nil {
		host = parsed.Host
	}
	return &Client{
		baseURL:  baseURL,
		baseHost: host,
		apiKey:   apiKey,
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

		next, err := c.validatedNextURL(page.Page)
		if err != nil {
			return nil, err
		}
		u = next
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

		next, err := c.validatedNextURL(page.Page)
		if err != nil {
			return nil, err
		}
		u = next
	}

	return all, nil
}

// doGet performs an authenticated GET request and decodes the JSON response.
func (c *Client) doGet(ctx context.Context, reqURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
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
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
		if readErr != nil {
			return fmt.Errorf("unexpected status %d (failed to read body: %w)", resp.StatusCode, readErr)
		}
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

// validatedNextURL extracts and validates the next pagination URL.
// It rejects URLs pointing to a different host to prevent leaking the API key.
func (c *Client) validatedNextURL(page PageInfo) (string, error) {
	if page.Next == nil {
		return "", nil
	}

	next := *page.Next
	if next == "" {
		return "", nil
	}

	parsed, err := url.Parse(next)
	if err != nil {
		return "", fmt.Errorf("invalid pagination URL %q: %w", next, err)
	}

	if parsed.Host != "" && parsed.Host != c.baseHost {
		return "", fmt.Errorf("pagination URL host %q does not match base host %q", parsed.Host, c.baseHost)
	}

	// If the API returns a relative URL, resolve it against baseURL.
	if !parsed.IsAbs() {
		base, _ := url.Parse(c.baseURL)
		if base != nil {
			next = base.ResolveReference(parsed).String()
		}
	}

	return next, nil
}
