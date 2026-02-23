package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// kafkaMessage represents the top-level structure of a Kafka JSON message definition.
type kafkaMessage struct {
	APIKey                int           `json:"apiKey"`
	Type                  string        `json:"type"`
	Name                  string        `json:"name"`
	ValidVersions         string        `json:"validVersions"`
	FlexibleVersions      string        `json:"flexibleVersions"`
	LatestVersionUnstable bool          `json:"latestVersionUnstable"`
	Fields                []kafkaField  `json:"fields"`
	CommonStructs         []kafkaStruct `json:"commonStructs"`
}

type kafkaField struct {
	Name             string       `json:"name"`
	Type             string       `json:"type"`
	Versions         string       `json:"versions"`
	NullableVersions string       `json:"nullableVersions"`
	TaggedVersions   string       `json:"taggedVersions"`
	Tag              *int         `json:"tag"`
	Default          any          `json:"default"`
	Fields           []kafkaField `json:"fields"`
}

type kafkaStruct struct {
	Name     string       `json:"name"`
	Versions string       `json:"versions"`
	Fields   []kafkaField `json:"fields"`
}

// versionRange represents a parsed version range like "0-18", "3+", or "none".
type versionRange struct {
	none bool
	min  int
	max  int // -1 means unbounded (N+)
}

func parseVersionRange(s string) versionRange {
	s = strings.TrimSpace(s)
	if s == "" || s == "none" {
		return versionRange{none: true}
	}
	if strings.HasSuffix(s, "+") {
		n := atoi(strings.TrimSuffix(s, "+"))
		return versionRange{min: n, max: -1}
	}
	if idx := strings.IndexByte(s, '-'); idx >= 0 {
		return versionRange{min: atoi(s[:idx]), max: atoi(s[idx+1:])}
	}
	// Single version like "0"
	n := atoi(s)
	return versionRange{min: n, max: n}
}

func atoi(s string) int {
	n, _ := fmt.Sscanf(s, "%d", new(int))
	if n == 0 {
		return 0
	}
	var v int
	fmt.Sscanf(s, "%d", &v)
	return v
}

func (vr versionRange) contains(v int) bool {
	if vr.none {
		return false
	}
	if v < vr.min {
		return false
	}
	if vr.max >= 0 && v > vr.max {
		return false
	}
	return true
}

// maxVer returns the max version of the range. Returns -1 if unbounded or none.
func (vr versionRange) maxVer() int {
	return vr.max
}

// stripJSONComments removes // line comments from Kafka JSON files.
func stripJSONComments(data []byte) []byte {
	var out []byte
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		// Also strip trailing // comments (not inside strings).
		// Simple approach: find // that's not inside a quoted string.
		if idx := indexCommentOutsideString(line); idx >= 0 {
			line = line[:idx]
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
	return out
}

// indexCommentOutsideString finds the position of // outside of JSON strings.
func indexCommentOutsideString(s string) int {
	inString := false
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '"' && (i == 0 || s[i-1] != '\\') {
			inString = !inString
		}
		if !inString && s[i] == '/' && s[i+1] == '/' {
			return i
		}
	}
	return -1
}

var initDSLOnce sync.Once

func initDSL(t *testing.T) {
	t.Helper()
	initDSLOnce.Do(func() {
		const dir = "definitions"
		const enumsFile = "enums"

		path := filepath.Join(dir, enumsFile)
		f, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading enums: %v", err)
		}
		ParseEnums(path, f)

		dirents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading definitions dir: %v", err)
		}
		for _, ent := range dirents {
			if ent.Name() == enumsFile || strings.HasPrefix(ent.Name(), ".") {
				continue
			}
			path := filepath.Join(dir, ent.Name())
			f, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			Parse(path, f)
		}
	})
}

// dslWireType returns the wire type string for a DSL Type, normalizing
// special types (Throttle, Timeout, Enum) to their underlying wire types.
func dslWireType(typ Type) string {
	switch t := typ.(type) {
	case Bool:
		return "bool"
	case Int8:
		return "int8"
	case Int16:
		return "int16"
	case Int32:
		return "int32"
	case Int64:
		return "int64"
	case Uint16:
		return "uint16"
	case Uint32:
		return "uint32"
	case Float64:
		return "float64"
	case Varint:
		return "int32"
	case Varlong:
		return "int64"
	case Uuid:
		return "[16]byte"
	case String:
		return "string"
	case NullableString:
		return "*string"
	case Bytes:
		return "[]byte"
	case NullableBytes:
		return "[]byte"
	case VarintString:
		return "string"
	case VarintBytes:
		return "[]byte"
	case FieldLengthMinusBytes:
		return "[]byte"
	case Throttle:
		return "int32"
	case Timeout:
		return "int32"
	case Enum:
		return dslWireType(t.Type)
	case Array:
		inner := dslWireType(t.Inner)
		return "[]" + inner
	case Struct:
		return "struct"
	default:
		return typ.TypeName()
	}
}

// jsonFieldWireType converts a JSON field type + properties to a wire type string
// comparable against dslWireType output.
func jsonFieldWireType(f kafkaField) string {
	jt := f.Type
	switch jt {
	case "bool":
		return "bool"
	case "int8":
		return "int8"
	case "int16":
		return "int16"
	case "int32":
		return "int32"
	case "int64":
		return "int64"
	case "uint16":
		return "uint16"
	case "uint32":
		return "uint32"
	case "float64":
		return "float64"
	case "uuid":
		return "[16]byte"
	case "varint":
		return "int32"
	case "varlong":
		return "int64"
	case "string":
		if f.NullableVersions != "" && f.NullableVersions != "none" {
			return "*string"
		}
		return "string"
	case "bytes":
		// Both nullable and non-nullable bytes map to []byte in DSL.
		return "[]byte"
	case "records":
		return "[]byte"
	}
	// Array type: []TypeName
	if strings.HasPrefix(jt, "[]") {
		inner := jt[2:]
		// If the inner type is a primitive, resolve it.
		switch inner {
		case "int32":
			return "[]int32"
		case "int64":
			return "[]int64"
		case "int16":
			return "[]int16"
		case "string":
			return "[]string"
		case "bool":
			return "[]bool"
		case "int8":
			return "[]int8"
		case "uint16":
			return "[]uint16"
		case "uint32":
			return "[]uint32"
		case "float64":
			return "[]float64"
		case "uuid":
			return "[][16]byte"
		default:
			// Named struct array
			return "[]struct"
		}
	}
	// Named struct type (non-array)
	return "struct"
}

// resolvedJSONFields returns the fields for a JSON field that references a
// named struct, looking up from commonStructs if the field has no inline fields.
func resolvedJSONFields(f kafkaField, commons map[string]kafkaStruct) []kafkaField {
	if len(f.Fields) > 0 {
		return f.Fields
	}
	jt := f.Type
	name := jt
	if strings.HasPrefix(name, "[]") {
		name = name[2:]
	}
	if cs, ok := commons[name]; ok {
		return cs.Fields
	}
	return nil
}

func TestValidateDSLAgainstKafkaJSON(t *testing.T) {
	kafkaDir := os.Getenv("KAFKA_DIR")
	if kafkaDir == "" {
		t.Skip("KAFKA_DIR not set; skipping Kafka JSON validation")
	}

	jsonDir := filepath.Join(kafkaDir, "clients", "src", "main", "resources", "common", "message")
	if _, err := os.Stat(jsonDir); err != nil {
		t.Fatalf("Kafka message dir not found at %s: %v", jsonDir, err)
	}

	initDSL(t)

	// Build a map from apiKey → DSL request and response structs.
	type dslPair struct {
		request  *Struct
		response *Struct
	}
	dslByKey := make(map[int]*dslPair)
	for i := range newStructs {
		s := &newStructs[i]
		if !s.TopLevel || s.Key < 0 {
			continue
		}
		pair, ok := dslByKey[s.Key]
		if !ok {
			pair = &dslPair{}
			dslByKey[s.Key] = pair
		}
		if s.ResponseKind != "" {
			pair.request = s
		} else if s.RequestKind != "" {
			pair.response = s
		}
	}

	// Read all JSON files from the Kafka message directory.
	dirents, err := os.ReadDir(jsonDir)
	if err != nil {
		t.Fatalf("reading JSON dir: %v", err)
	}

	// Parse all JSON files first, tracking which apiKeys have unstable
	// latest versions. The request file has latestVersionUnstable but the
	// response file may not, even though they share the same version range.
	var msgs []kafkaMessage
	unstableKeys := make(map[int]bool)
	for _, ent := range dirents {
		if !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(jsonDir, ent.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", ent.Name(), err)
		}
		cleaned := stripJSONComments(data)
		var msg kafkaMessage
		if err := json.Unmarshal(cleaned, &msg); err != nil {
			t.Fatalf("parsing %s: %v", ent.Name(), err)
		}
		if msg.Type != "request" && msg.Type != "response" {
			continue
		}
		if msg.LatestVersionUnstable {
			unstableKeys[msg.APIKey] = true
		}
		msgs = append(msgs, msg)
	}

	for _, msg := range msgs {
		// Skip messages with validVersions "none" (removed in Kafka 4.0).
		validVR := parseVersionRange(msg.ValidVersions)
		if validVR.none {
			continue
		}

		// Propagate unstable flag from request to response.
		if unstableKeys[msg.APIKey] {
			msg.LatestVersionUnstable = true
		}

		pair, ok := dslByKey[msg.APIKey]
		if !ok {
			fmt.Fprintf(os.Stderr, "MISSING  %-45s not in DSL (apiKey %d)\n", msg.Name, msg.APIKey)
			continue
		}

		var dslStruct *Struct
		if msg.Type == "request" {
			dslStruct = pair.request
		} else {
			dslStruct = pair.response
		}
		if dslStruct == nil {
			t.Errorf("%s: no DSL %s struct for apiKey %d", msg.Name, msg.Type, msg.APIKey)
			continue
		}

		t.Run(msg.Name, func(t *testing.T) {
			validateMessage(t, msg, dslStruct)
		})
	}
}

func validateMessage(t *testing.T, msg kafkaMessage, dsl *Struct) {
	t.Helper()

	validVR := parseVersionRange(msg.ValidVersions)
	flexVR := parseVersionRange(msg.FlexibleVersions)

	// Determine the JSON's stable max version.
	jsonMax := validVR.maxVer()
	if msg.LatestVersionUnstable && jsonMax > 0 {
		jsonMax--
	}

	// Error if the DSL claims a version higher than JSON knows.
	// Log (don't fail) if the DSL is behind — this is expected when
	// Kafka is ahead and we haven't caught up yet.
	if jsonMax >= 0 && dsl.MaxVersion > jsonMax {
		t.Errorf("max version: DSL %d > JSON %d", dsl.MaxVersion, jsonMax)
	} else if jsonMax >= 0 && dsl.MaxVersion < jsonMax {
		fields := collectMissingFields(msg.Name, dsl.MaxVersion+1, jsonMax, msg.Fields)
		detail := fmt.Sprintf("v%d..%d", dsl.MaxVersion, jsonMax)
		if len(fields) > 0 {
			detail += ", new fields: " + strings.Join(fields, ", ")
		}
		fmt.Fprintf(os.Stderr, "MISSING  %-45s %s\n", msg.Name, detail)
	}

	// Validate flexible version.
	if flexVR.none {
		if dsl.FlexibleAt >= 0 {
			t.Errorf("flexible version: DSL has flexible at %d but JSON has none", dsl.FlexibleAt)
		}
	} else {
		if dsl.FlexibleAt != flexVR.min {
			t.Errorf("flexible version: DSL %d != JSON %d", dsl.FlexibleAt, flexVR.min)
		}
	}

	// Build commonStructs lookup.
	commons := make(map[string]kafkaStruct)
	for _, cs := range msg.CommonStructs {
		commons[cs.Name] = cs
	}

	// Compare field structure at each version.
	maxV := dsl.MaxVersion
	if jsonMax >= 0 && jsonMax < maxV {
		maxV = jsonMax
	}
	for v := validVR.min; v <= maxV; v++ {
		compareFieldsAtVersion(t, msg.Name, v, dsl.FlexibleAt, msg.Fields, dsl.Fields, commons)
	}
}

func compareFieldsAtVersion(t *testing.T, msgName string, version, flexibleAt int, jsonFields []kafkaField, dslFields []StructField, commons map[string]kafkaStruct) {
	t.Helper()

	prefix := fmt.Sprintf("v%d", version)
	isFlexible := flexibleAt >= 0 && version >= flexibleAt

	// Filter JSON fields active at this version.
	var jsonNonTagged []kafkaField
	jsonTagged := make(map[int]kafkaField)
	for _, jf := range jsonFields {
		fvr := parseVersionRange(jf.Versions)
		if !fvr.contains(version) {
			continue
		}
		if jf.Tag != nil {
			tvr := parseVersionRange(jf.TaggedVersions)
			if tvr.contains(version) {
				jsonTagged[*jf.Tag] = jf
				continue
			}
		}
		jsonNonTagged = append(jsonNonTagged, jf)
	}

	// Filter DSL fields active at this version.
	var dslNonTagged []StructField
	dslTagged := make(map[int]StructField)
	for _, df := range dslFields {
		if df.Tag >= 0 && df.MinVersion == -1 {
			// Tag-only field: only active at flexible versions.
			if isFlexible {
				dslTagged[df.Tag] = df
			}
			continue
		}
		if !dslFieldActiveAt(df, version) {
			continue
		}
		dslNonTagged = append(dslNonTagged, df)
	}

	// Compare non-tagged fields in order.
	if len(jsonNonTagged) != len(dslNonTagged) {
		t.Errorf("%s %s: non-tagged field count: JSON %d != DSL %d", msgName, prefix, len(jsonNonTagged), len(dslNonTagged))
		var jnames, dnames []string
		for _, jf := range jsonNonTagged {
			jnames = append(jnames, jf.Name)
		}
		for _, df := range dslNonTagged {
			dnames = append(dnames, df.FieldName)
		}
		t.Errorf("  JSON fields: %v", jnames)
		t.Errorf("  DSL fields:  %v", dnames)
		return
	}

	for i := range jsonNonTagged {
		jf := jsonNonTagged[i]
		df := dslNonTagged[i]
		jType := jsonFieldWireType(jf)
		dType := dslWireType(df.Type)
		if jType != dType {
			t.Errorf("%s %s field %d (%s/%s): type mismatch: JSON %s (%s) vs DSL %s (%s)",
				msgName, prefix, i, jf.Name, df.FieldName, jType, jf.Type, dType, df.Type.TypeName())
		}

		// Recurse into struct fields.
		if jType == "struct" || strings.HasPrefix(jType, "[]struct") {
			innerJSON := resolvedJSONFields(jf, commons)
			innerDSL := innerStructFields(df.Type)
			if innerJSON != nil && innerDSL != nil {
				compareFieldsAtVersion(t, msgName+"."+jf.Name, version, flexibleAt, innerJSON, innerDSL, commons)
			}
		}
	}

	// Compare tagged fields: only report JSON tags missing from DSL.
	// DSL tags not in JSON are expected (DSL is intentionally more
	// permissive, allowing all tags at all flexible versions).
	for tag, jf := range jsonTagged {
		df, ok := dslTagged[tag]
		if !ok {
			t.Errorf("%s %s: JSON tagged field %d (%s) not found in DSL", msgName, prefix, tag, jf.Name)
			continue
		}
		jType := jsonFieldWireType(jf)
		dType := dslWireType(df.Type)
		if jType != dType {
			t.Errorf("%s %s tag %d (%s/%s): type mismatch: JSON %s (%s) vs DSL %s (%s)",
				msgName, prefix, tag, jf.Name, df.FieldName, jType, jf.Type, dType, df.Type.TypeName())
		}

		// Recurse into struct fields.
		if jType == "struct" || strings.HasPrefix(jType, "[]struct") {
			innerJSON := resolvedJSONFields(jf, commons)
			innerDSL := innerStructFields(df.Type)
			if innerJSON != nil && innerDSL != nil {
				compareFieldsAtVersion(t, msgName+"."+jf.Name, version, flexibleAt, innerJSON, innerDSL, commons)
			}
		}
	}
}

// dslFieldActiveAt returns whether a DSL field is active at the given version.
func dslFieldActiveAt(f StructField, version int) bool {
	if f.MinVersion == -1 {
		// Tag-only field: active at all versions (it's controlled by tag presence).
		return f.Tag >= 0
	}
	if version < f.MinVersion {
		return false
	}
	if f.MaxVersion >= 0 && version > f.MaxVersion {
		return false
	}
	return true
}

// innerStructFields extracts the Fields slice from a DSL type that wraps a struct
// (either a direct Struct or an Array containing a Struct).
func innerStructFields(typ Type) []StructField {
	switch t := typ.(type) {
	case Struct:
		return t.Fields
	case Array:
		return innerStructFields(t.Inner)
	}
	return nil
}

// collectMissingFields returns a summary of JSON fields new in versions fromV..toV.
func collectMissingFields(path string, fromV, toV int, jsonFields []kafkaField) []string {
	var out []string
	for _, jf := range jsonFields {
		fvr := parseVersionRange(jf.Versions)
		if fvr.none {
			continue
		}
		if fvr.min >= fromV && fvr.min <= toV {
			out = append(out, fmt.Sprintf("%s (%s, v%d+)", jf.Name, jf.Type, fvr.min))
		}
		if len(jf.Fields) > 0 {
			out = append(out, collectMissingFields(path+"."+jf.Name, fromV, toV, jf.Fields)...)
		}
	}
	return out
}
