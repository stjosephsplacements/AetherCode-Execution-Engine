package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/requestid"
)

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

func NewClient(baseURL, authToken string) *Client {
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: 0, // per-request context.WithTimeout controls deadline
			Transport: &http.Transport{
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   256,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 0, // batched /run calls vary in duration; context deadline governs
				DisableCompression:    true,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
	}
}

// Run sends a batch of commands to go-judge and returns the results.
// The response is a bare JSON array — one Result per Cmd.
func (c *Client) Run(ctx context.Context, req Request) ([]Result, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		bufPool.Put(buf)
		return nil, fmt.Errorf("sandbox: marshal request: %w", err)
	}

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	bufPool.Put(buf)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/run", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("sandbox: create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	if reqID := requestid.FromContext(ctx); reqID != "" {
		httpReq.Header.Set("X-Request-Id", reqID)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("sandbox: request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort: the http client reuses pooled connections

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("sandbox: HTTP %d: %s", resp.StatusCode, string(errBody))
	}

	var results []Result
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&results); err != nil {
		return nil, fmt.Errorf("sandbox: decode response: %w", err)
	}
	return results, nil
}

// Ping checks connectivity to go-judge by hitting /version.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Version(ctx)
	return err
}

// Version returns the go-judge build version string by calling GET /version.
func (c *Client) Version(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/version", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.authToken)
	if reqID := requestid.FromContext(ctx); reqID != "" {
		req.Header.Set("X-Request-Id", reqID)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sandbox: version request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort: the http client reuses pooled connections

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", fmt.Errorf("sandbox: read version response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("sandbox: version returned HTTP %d", resp.StatusCode)
	}

	var ver struct {
		BuildVersion string `json:"buildVersion"`
	}
	if err := json.Unmarshal(body, &ver); err != nil {
		return "", fmt.Errorf("sandbox: unmarshal version: %w", err)
	}
	return ver.BuildVersion, nil
}

// --- Request/Response types matching go-judge REST API ---

type Request struct {
	Cmd         []Cmd     `json:"cmd"`
	PipeMapping []PipeMap `json:"pipeMapping,omitempty"`
}

type PipeIndex struct {
	Index int `json:"index"`
	Fd    int `json:"fd"`
}

type PipeMap struct {
	In    PipeIndex `json:"in"`
	Out   PipeIndex `json:"out"`
	Proxy bool      `json:"proxy,omitempty"`
	Name  string    `json:"name,omitempty"`
}

type Cmd struct {
	Args          []string          `json:"args"`
	Env           []string          `json:"env,omitempty"`
	Files         []CmdFile         `json:"files"`
	CPULimit      uint64            `json:"cpuLimit"`
	ClockLimit    uint64            `json:"clockLimit,omitempty"`
	MemoryLimit   uint64            `json:"memoryLimit"`
	ProcLimit     uint64            `json:"procLimit"`
	CopyIn        map[string]CopyIn `json:"copyIn,omitempty"`
	CopyOut       []string          `json:"copyOut,omitempty"`
	CopyOutCached []string          `json:"copyOutCached,omitempty"`
}

// CmdFile represents an entry in the Files array.
// For stdin:  set Content to the input string.
// For stdout/stderr collectors: set Name and Max.
type CmdFile struct {
	Content *string `json:"content,omitempty"`
	Name    *string `json:"name,omitempty"`
	Max     *int64  `json:"max,omitempty"`
}

// CopyIn represents a file to copy into the sandbox.
// Exactly one of Content or FileID should be set.
type CopyIn struct {
	Content *string `json:"content,omitempty"`
	FileID  *string `json:"fileId,omitempty"`
}

type Result struct {
	Status     string            `json:"status"`
	ExitStatus int               `json:"exitStatus"`
	Error      string            `json:"error,omitempty"`
	Time       uint64            `json:"time"`
	Memory     uint64            `json:"memory"`
	RunTime    uint64            `json:"runTime"`
	Files      map[string]string `json:"files,omitempty"`
	FileIDs    map[string]string `json:"fileIds,omitempty"`
}

// --- Helpers for building CmdFile and CopyIn ---

func Stdin(content string) CmdFile {
	return CmdFile{Content: &content}
}

func Collector(name string, max int64) CmdFile {
	return CmdFile{Name: &name, Max: &max}
}

func FileContent(content string) CopyIn {
	return CopyIn{Content: &content}
}

func CachedFile(fileID string) CopyIn {
	return CopyIn{FileID: &fileID}
}

const (
	StatusAccepted      = "Accepted"
	StatusNonzeroExit   = "Nonzero Exit Status"
	StatusTimeLimitEx   = "Time Limit Exceeded"
	StatusMemoryLimitEx = "Memory Limit Exceeded"
	StatusOutputLimitEx = "Output Limit Exceeded"
	StatusSignalled     = "Signalled"
	StatusInternalErr   = "Internal Error"
)
