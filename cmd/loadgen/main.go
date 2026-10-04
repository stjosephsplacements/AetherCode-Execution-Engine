package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- submission templates ---

type template struct {
	Language        string
	SourceCode      string
	Tests           []testInput
	ExpectedVerdict string
}

type testInput struct {
	Input          string `json:"input"`
	ExpectedOutput string `json:"expected_output"`
}

var templates = []struct {
	weight    int // out of 1000
	templates []template
}{
	{360, []template{ // Python accepted (36%)
		{Language: "python", SourceCode: "n=int(input())\nprint(n*2)", Tests: []testInput{{"21\n", "42\n"}, {"5\n", "10\n"}}, ExpectedVerdict: "accepted"},
	}},
	{20, []template{ // Python wrong answer (2%)
		{Language: "python", SourceCode: "n=int(input())\nprint(n+1)", Tests: []testInput{{"5\n", "10\n"}}, ExpectedVerdict: "wrong_answer"},
	}},
	{10, []template{ // Python TLE (1%)
		{Language: "python", SourceCode: "while True: pass", Tests: []testInput{{"", "done\n"}}, ExpectedVerdict: "time_limit"},
	}},
	{10, []template{ // Python runtime error (1%)
		{Language: "python", SourceCode: "print(1/0)", Tests: []testInput{{"", "0\n"}}, ExpectedVerdict: "runtime_error"},
	}},
	{180, []template{ // C accepted (18%)
		{Language: "c", SourceCode: "#include <stdio.h>\nint main() { int n; scanf(\"%d\", &n); printf(\"%d\\n\", n*2); return 0; }", Tests: []testInput{{"21\n", "42\n"}, {"5\n", "10\n"}}, ExpectedVerdict: "accepted"},
	}},
	{10, []template{ // C compile error (1%)
		{Language: "c", SourceCode: "int main() { undeclared = 42; }", Tests: []testInput{{"", "42\n"}}, ExpectedVerdict: "compilation_error"},
	}},
	{10, []template{ // C TLE (1%)
		{Language: "c", SourceCode: "#include <stdio.h>\nint main() { while(1); return 0; }", Tests: []testInput{{"", "done\n"}}, ExpectedVerdict: "time_limit"},
	}},
	{140, []template{ // C++ accepted (14%)
		{Language: "cpp", SourceCode: "#include <iostream>\nusing namespace std;\nint main() { int n; cin >> n; cout << n*2 << endl; return 0; }", Tests: []testInput{{"21\n", "42\n"}}, ExpectedVerdict: "accepted"},
	}},
	{10, []template{ // C++ CE (1%)
		{Language: "cpp", SourceCode: "#include <iostream>\nint main() { undeclared = 1; }", Tests: []testInput{{"", "1\n"}}, ExpectedVerdict: "compilation_error"},
	}},
	{140, []template{ // Java accepted (14%)
		{Language: "java", SourceCode: "import java.util.Scanner;\npublic class Main {\n  public static void main(String[] args) {\n    Scanner sc = new Scanner(System.in);\n    int n = sc.nextInt();\n    System.out.println(n * 2);\n  }\n}", Tests: []testInput{{"21\n", "42\n"}}, ExpectedVerdict: "accepted"},
	}},
	{10, []template{ // Java CE (1%)
		{Language: "java", SourceCode: "public class Main { public static void main(String[] args) { undeclared = 1; } }", Tests: []testInput{{"", "1\n"}}, ExpectedVerdict: "compilation_error"},
	}},
	{50, []template{ // JavaScript accepted (5%)
		{Language: "javascript", SourceCode: "const readline = require(\"readline\");\nconst rl = readline.createInterface({ input: process.stdin });\nrl.on(\"line\", (line) => { console.log(parseInt(line) * 2); rl.close(); });", Tests: []testInput{{"21\n", "42\n"}}, ExpectedVerdict: "accepted"},
	}},
	{20, []template{ // SQLite accepted (2%)
		{Language: "sqlite", SourceCode: "CREATE TABLE t(x INTEGER);\nINSERT INTO t VALUES(1),(2),(3);\nSELECT SUM(x) FROM t;", Tests: []testInput{{"", "6\n"}}, ExpectedVerdict: "accepted"},
	}},
	{30, []template{ // Go accepted (3%)
		{Language: "go", SourceCode: "package main\nimport \"fmt\"\nfunc main() {\n\tvar n int\n\tfmt.Scan(&n)\n\tfmt.Println(n * 2)\n}", Tests: []testInput{{"21\n", "42\n"}}, ExpectedVerdict: "accepted"},
	}},
}

func pickTemplate() template {
	total := 0
	for _, g := range templates {
		total += g.weight
	}
	r := rand.IntN(total)
	cum := 0
	for _, g := range templates {
		cum += g.weight
		if r < cum {
			return g.templates[rand.IntN(len(g.templates))]
		}
	}
	return templates[0].templates[0]
}

// --- result tracking ---

type result struct {
	Language        string
	ExpectedVerdict string
	ActualVerdict   string
	IngestLatency   time.Duration
	VerdictLatency  time.Duration
	Lost            bool
	Error           string
}

// --- main ---

func main() {
	url := flag.String("url", "http://127.0.0.1:5100", "Gateway base URL")
	concurrency := flag.Int("concurrency", 100, "Max simultaneous in-flight submissions")
	total := flag.Int("total", 1200, "Total submissions to send")
	timeout := flag.Duration("timeout", 120*time.Second, "Per-job SSE verdict timeout")
	jsonOut := flag.Bool("json", false, "Output results as JSON")
	flag.Parse()

	fmt.Printf("STJ-Exec Load Generator\n")
	fmt.Printf("  URL:         %s\n", *url)
	fmt.Printf("  Concurrency: %d\n", *concurrency)
	fmt.Printf("  Total:       %d\n", *total)
	fmt.Printf("  Timeout:     %s\n", *timeout)
	fmt.Println()

	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]result, 0, *total)

	var submitted atomic.Int64
	var completed atomic.Int64

	postTransport := &http.Transport{
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     120 * time.Second,
		DisableCompression:  true,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: postTransport}
	sseClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1024,
			IdleConnTimeout:     120 * time.Second,
			DisableCompression:  true,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
	startTime := time.Now()

	// Progress ticker
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				s := submitted.Load()
				c := completed.Load()
				elapsed := time.Since(startTime).Seconds()
				rate := float64(c) / elapsed
				fmt.Printf("\r  progress: %d/%d submitted, %d/%d completed (%.1f/s)    ",
					s, *total, c, *total, rate)
			}
		}
	}()

	for i := range *total {
		_ = i
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			tmpl := pickTemplate()
			submitted.Add(1)
			r := runSubmission(client, sseClient, *url, tmpl, *timeout)
			completed.Add(1)

			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(done)
	wallTime := time.Since(startTime)

	fmt.Printf("\r%s\n", strings.Repeat(" ", 80))
	printReport(results, wallTime, *jsonOut)
}

func runSubmission(client *http.Client, sseClient *http.Client, baseURL string, tmpl template, timeout time.Duration) result {
	r := result{
		Language:        tmpl.Language,
		ExpectedVerdict: tmpl.ExpectedVerdict,
	}

	// Build request
	body := map[string]interface{}{
		"language":    tmpl.Language,
		"source_code": tmpl.SourceCode,
		"tests":       tmpl.Tests,
	}
	payload, _ := json.Marshal(body)

	tSubmit := time.Now()

	// POST /api/v1/execute with retry on 429/500
	var resp *http.Response
	for attempt := range 8 {
		req, _ := http.NewRequest("POST", baseURL+"/api/v1/execute", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")

		var err error
		resp, err = client.Do(req)
		if err != nil {
			r.Lost = true
			r.Error = fmt.Sprintf("POST failed: %v", err)
			return r
		}

		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			resp.Body.Close()
			resp = nil
			backoff := time.Duration(1<<min(attempt, 5)) * 100 * time.Millisecond
			time.Sleep(backoff)
			continue
		}
		break
	}
	if resp == nil {
		r.Lost = true
		r.Error = "POST failed: all retries exhausted"
		return r
	}
	defer resp.Body.Close()

	tIngested := time.Now()
	r.IngestLatency = tIngested.Sub(tSubmit)

	if resp.StatusCode != 201 {
		r.Lost = true
		r.Error = fmt.Sprintf("POST status %d", resp.StatusCode)
		return r
	}

	var execResp struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
		r.Lost = true
		r.Error = "invalid POST response"
		return r
	}

	// SSE: GET /api/v1/stream?job_id=...
	sseCtx, sseCancel := context.WithTimeout(context.Background(), timeout)
	defer sseCancel()
	sseReq, _ := http.NewRequestWithContext(sseCtx, "GET", baseURL+"/api/v1/stream?job_id="+execResp.JobID, nil)
	sseResp, err := sseClient.Do(sseReq)
	if err != nil {
		r.Lost = true
		r.Error = fmt.Sprintf("SSE connect failed: %v", err)
		return r
	}
	defer sseResp.Body.Close()

	scanner := bufio.NewScanner(sseResp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := line[6:]
		var evt struct {
			Type string `json:"type"`
			Data struct {
				Verdict string `json:"verdict"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}
		if evt.Type == "VERDICT" {
			r.ActualVerdict = evt.Data.Verdict
			r.VerdictLatency = time.Since(tSubmit)
			return r
		}
	}

	r.Lost = true
	r.Error = "SSE timeout — no VERDICT"
	return r
}

func printReport(results []result, wallTime time.Duration, jsonOutput bool) {
	total := len(results)
	var lost, mismatched int
	var ingestLats, verdictLats []float64
	verdictDist := map[string]int{}
	langDist := map[string]int{}

	errorDist := map[string]int{}
	for _, r := range results {
		langDist[r.Language]++
		if r.Lost {
			lost++
			errorDist[r.Error]++
			continue
		}
		verdictDist[r.ActualVerdict]++
		ingestLats = append(ingestLats, r.IngestLatency.Seconds()*1000)
		verdictLats = append(verdictLats, r.VerdictLatency.Seconds()*1000)
		if r.ActualVerdict != r.ExpectedVerdict {
			mismatched++
		}
	}

	sort.Float64s(ingestLats)
	sort.Float64s(verdictLats)

	throughput := float64(total) / wallTime.Seconds()
	verdictRate := float64(len(verdictLats)) / wallTime.Seconds()

	if jsonOutput {
		out := map[string]interface{}{
			"total":          total,
			"completed":      total - lost,
			"lost":           lost,
			"mismatched":     mismatched,
			"wall_time_ms":   wallTime.Milliseconds(),
			"throughput":     throughput,
			"verdict_rate":   verdictRate,
			"ingest_p50_ms":  percentile(ingestLats, 50),
			"ingest_p95_ms":  percentile(ingestLats, 95),
			"ingest_p99_ms":  percentile(ingestLats, 99),
			"verdict_p50_ms": percentile(verdictLats, 50),
			"verdict_p95_ms": percentile(verdictLats, 95),
			"verdict_p99_ms": percentile(verdictLats, 99),
			"verdicts":       verdictDist,
			"languages":      langDist,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(out)
		return
	}

	fmt.Println("==========================================")
	fmt.Println(" STJ-Exec Benchmark Results")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Printf("  Total submissions:  %d\n", total)
	fmt.Printf("  Completed:          %d\n", total-lost)
	fmt.Printf("  Lost:               %d\n", lost)
	fmt.Printf("  Verdict mismatch:   %d\n", mismatched)
	fmt.Printf("  Wall time:          %s\n", wallTime.Round(time.Millisecond))
	fmt.Printf("  Throughput:         %.1f submissions/s\n", throughput)
	fmt.Printf("  Verdict rate:       %.1f verdicts/s\n", verdictRate)
	fmt.Println()

	fmt.Println("  Latency (ms)        p50      p95      p99")
	fmt.Println("  ──────────────────────────────────────────")
	fmt.Printf("  Ingestion       %7.1f  %7.1f  %7.1f\n",
		percentile(ingestLats, 50), percentile(ingestLats, 95), percentile(ingestLats, 99))
	fmt.Printf("  Verdict         %7.1f  %7.1f  %7.1f\n",
		percentile(verdictLats, 50), percentile(verdictLats, 95), percentile(verdictLats, 99))
	fmt.Println()

	fmt.Println("  Verdict distribution:")
	for v, n := range verdictDist {
		pct := float64(n) / float64(total) * 100
		fmt.Printf("    %-20s %5d  (%4.1f%%)\n", v, n, pct)
	}
	fmt.Println()

	fmt.Println("  Language distribution:")
	for l, n := range langDist {
		pct := float64(n) / float64(total) * 100
		fmt.Printf("    %-20s %5d  (%4.1f%%)\n", l, n, pct)
	}
	fmt.Println()

	if len(errorDist) > 0 {
		fmt.Println("  Error distribution:")
		for e, n := range errorDist {
			fmt.Printf("    %-50s %5d\n", e, n)
		}
		fmt.Println()
	}

	// SLO check
	fmt.Println("  SLO Check:")
	ingestP99 := percentile(ingestLats, 99)
	verdictP50 := percentile(verdictLats, 50)
	verdictP99 := percentile(verdictLats, 99)

	check := func(name string, val float64, limit float64, unit string) {
		status := "PASS"
		if val > limit || math.IsNaN(val) {
			status = "FAIL"
		}
		fmt.Printf("    %-35s %7.0f %s (limit: %.0f) [%s]\n", name, val, unit, limit, status)
	}
	check("Ingestion p99", ingestP99, 15, "ms")
	check("Verdict p50", verdictP50, 15000, "ms")
	check("Verdict p99", verdictP99, 60000, "ms")
	fmt.Printf("    %-35s %7.1f s   (limit: 120) [%s]\n", "Wall time (drain)",
		wallTime.Seconds(), passFail(wallTime.Seconds() <= 120))
	fmt.Printf("    %-35s %7d     (limit: 0)   [%s]\n", "Lost jobs", lost, passFail(lost == 0))
	fmt.Println()
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	idx := p / 100 * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
