package datapipe

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type OutputSink interface {
	WriteBatch(ctx context.Context, records []Record) error
}

// JSONLOutputSink appends one JSON object per record. Repeated records retain
// their stable identity, allowing consumers to perform idempotent deduplicate.
type JSONLOutputSink struct {
	Path string
}

func NewJSONLOutputSink(dir, source string) JSONLOutputSink {
	return JSONLOutputSink{Path: filepath.Join(dir, source+".jsonl")}
}

func (s JSONLOutputSink) WriteBatch(_ context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	file, err := os.OpenFile(s.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open output %s: %w", s.Path, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	for _, record := range records {
		if err := record.Validate(); err != nil {
			file.Close()
			return err
		}
		if err := encoder.Encode(record); err != nil {
			file.Close()
			return fmt.Errorf("write record %s: %w", record.Identity, err)
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return fmt.Errorf("flush output: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync output: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	return nil
}

func ReadJSONL(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read output %s: %w", path, err)
	}
	var records []Record
	for lineNo, line := range splitLines(data) {
		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode output line %d: %w", lineNo+1, err)
		}
		records = append(records, record)
	}
	return records, nil
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			if i > start {
				lines = append(lines, data[start:i])
			}
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
