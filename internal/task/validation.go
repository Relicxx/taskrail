package task

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
)

// StrictJSON rejects duplicate keys throughout the document, unknown fields and trailing values.
func StrictJSON(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := uniqueValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	typ := reflect.TypeOf(dst)
	if typ != nil && typ.Kind() == reflect.Pointer && typ.Elem().Kind() == reflect.Struct {
		var fields map[string]json.RawMessage
		if e := json.Unmarshal(raw, &fields); e != nil {
			return e
		}
		allowed := map[string]bool{}
		typ = typ.Elem()
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "" {
				name = f.Name
			}
			if name != "-" {
				allowed[name] = true
			}
		}
		for name := range fields {
			if !allowed[name] {
				return fmt.Errorf("unknown field")
			}
		}
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(dst)
}
func uniqueValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return fmt.Errorf("duplicate or invalid key")
				}
				seen[s] = true
				if e = uniqueValue(d); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e := uniqueValue(d); e != nil {
					return e
				}
			}
		default:
			return fmt.Errorf("invalid delimiter")
		}
		_, err = d.Token()
	}
	return err
}

type Numbers struct {
	Numbers []float64 `json:"numbers"`
}
type Duration struct {
	DurationMS int `json:"duration_ms"`
}

func decodeNumbers(raw []byte) (Numbers, error) {
	var src struct {
		Numbers []*float64 `json:"numbers"`
	}
	if StrictJSON(raw, &src) != nil || len(src.Numbers) == 0 || len(src.Numbers) > 10000 {
		return Numbers{}, ErrInvalid
	}
	n := Numbers{Numbers: make([]float64, len(src.Numbers))}
	for i, v := range src.Numbers {
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
			return Numbers{}, ErrInvalid
		}
		n.Numbers[i] = *v
	}
	return n, nil
}

func Normalize(in CreateInput, demo bool) (CreateInput, error) {
	if in.MaxAttempts == 0 {
		in.MaxAttempts = 5
	}
	if in.TimeoutMS == 0 {
		in.TimeoutMS = 60000
	}
	if in.MaxAttempts < 1 || in.MaxAttempts > 10 || in.TimeoutMS < 1000 || in.TimeoutMS > 300000 {
		return in, ErrInvalid
	}
	if in.RunAt != nil {
		v := in.RunAt.UTC()
		in.RunAt = &v
	}
	switch in.Type {
	case "report.aggregate":
		n, err := decodeNumbers(in.Payload)
		if err != nil {
			return in, ErrInvalid
		}
		for _, v := range n.Numbers {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return in, ErrInvalid
			}
		}
		in.Payload, _ = json.Marshal(n)
	case "demo.wait":
		var d Duration
		if !demo || StrictJSON(in.Payload, &d) != nil || d.DurationMS < 1 || d.DurationMS > 300000 {
			return in, ErrInvalid
		}
		in.Payload, _ = json.Marshal(d)
	default:
		return in, ErrInvalid
	}
	return in, nil
}
func Hash(in CreateInput) (string, error) {
	b, e := json.Marshal(in)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
