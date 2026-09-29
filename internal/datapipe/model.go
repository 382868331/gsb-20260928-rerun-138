package datapipe

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Identity is stable across pages, retries, snapshots, and progress files.
// It deliberately does not contain a page number or cursor.
type Identity struct {
	Source string `json:"source"`
	Entity string `json:"entity"`
	ID     string `json:"id"`
}

func (i Identity) Valid() error {
	var missing []string
	if i.Source == "" {
		missing = append(missing, "source")
	}
	if i.Entity == "" {
		missing = append(missing, "entity")
	}
	if i.ID == "" {
		missing = append(missing, "id")
	}
	if len(missing) > 0 {
		return fmt.Errorf("identity is missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func (i Identity) Key() string {
	return i.Source + "\x00" + i.Entity + "\x00" + i.ID
}

func (i Identity) String() string {
	return i.Source + ":" + i.Entity + ":" + i.ID
}

// Fingerprint lets a downstream consumer distinguish a repeated version of the
// same identity from changed content. Identity remains the stable dedupe key.
func (i Identity) Fingerprint(revision string, payload any) (string, error) {
	if err := i.Valid(); err != nil {
		return "", err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode identity fingerprint payload: %w", err)
	}
	sum := sha256.Sum256([]byte(i.Key() + "\x00" + revision + "\x00" + string(payloadJSON)))
	return hex.EncodeToString(sum[:]), nil
}

type Record struct {
	Identity  Identity        `json:"identity"`
	Revision  string          `json:"revision,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt string          `json:"created_at,omitempty"`
}

func (r Record) Validate() error {
	if err := r.Identity.Valid(); err != nil {
		return err
	}
	if !json.Valid(r.Payload) {
		return fmt.Errorf("record %s has invalid JSON payload", r.Identity)
	}
	return nil
}

type Page struct {
	Records []Record
	// Next is an opaque continuation token. Empty means the scan is finished.
	Next string
}
