package sr

import (
	"fmt"
	"strings"
)

// SchemaType as an enum representing schema types. The default schema type
// is avro.
type SchemaType int

const (
	TypeAvro SchemaType = iota
	TypeProtobuf
	TypeJSON
)

func (t SchemaType) String() string {
	switch t {
	case TypeAvro:
		return "AVRO"
	case TypeProtobuf:
		return "PROTOBUF"
	case TypeJSON:
		return "JSON"
	default:
		return ""
	}
}

func (t SchemaType) MarshalText() ([]byte, error) {
	s := t.String()
	if s == "" {
		return nil, fmt.Errorf("unknown schema type %d", t)
	}
	return []byte(s), nil
}

func (t *SchemaType) UnmarshalText(text []byte) error {
	switch s := strings.ToUpper(string(text)); s {
	default:
		return fmt.Errorf("unknown schema type %q", s)
	case "", "AVRO":
		*t = TypeAvro
	case "PROTOBUF":
		*t = TypeProtobuf
	case "JSON":
		*t = TypeJSON
	}
	return nil
}

// CompatibilityLevel as an enum representing config compatibility levels.
type CompatibilityLevel int

const (
	CompatNone CompatibilityLevel = 1 + iota
	CompatBackward
	CompatBackwardTransitive
	CompatForward
	CompatForwardTransitive
	CompatFull
	CompatFullTransitive
)

func (l CompatibilityLevel) String() string {
	switch l {
	case CompatNone:
		return "NONE"
	case CompatBackward:
		return "BACKWARD"
	case CompatBackwardTransitive:
		return "BACKWARD_TRANSITIVE"
	case CompatForward:
		return "FORWARD"
	case CompatForwardTransitive:
		return "FORWARD_TRANSITIVE"
	case CompatFull:
		return "FULL"
	case CompatFullTransitive:
		return "FULL_TRANSITIVE"
	default:
		return ""
	}
}

func (l CompatibilityLevel) MarshalText() ([]byte, error) {
	s := l.String()
	if s == "" {
		return nil, fmt.Errorf("unknown compatibility level %d", l)
	}
	return []byte(s), nil
}

func (l *CompatibilityLevel) UnmarshalText(text []byte) error {
	switch s := strings.ToUpper(string(text)); s {
	default:
		return fmt.Errorf("unknown compatibility level %q", s)
	case "NONE":
		*l = CompatNone
	case "BACKWARD":
		*l = CompatBackward
	case "BACKWARD_TRANSITIVE":
		*l = CompatBackwardTransitive
	case "FORWARD":
		*l = CompatForward
	case "FORWARD_TRANSITIVE":
		*l = CompatForwardTransitive
	case "FULL":
		*l = CompatFull
	case "FULL_TRANSITIVE":
		*l = CompatFullTransitive
	}
	return nil
}

// Mode as an enum representing the "mode" of the registry or a subject.
type Mode int

const (
	ModeImport Mode = iota
	ModeReadOnly
	ModeReadWrite
)

func (m Mode) String() string {
	switch m {
	case ModeImport:
		return "IMPORT"
	case ModeReadOnly:
		return "READONLY"
	case ModeReadWrite:
		return "READWRITE"
	default:
		return ""
	}
}

func (m Mode) MarshalText() ([]byte, error) {
	s := m.String()
	if s == "" {
		return nil, fmt.Errorf("unknown mode %d", m)
	}
	return []byte(s), nil
}

func (m *Mode) UnmarshalText(text []byte) error {
	switch s := strings.ToUpper(string(text)); s {
	default:
		return fmt.Errorf("unknown schema type %q", s)
	case "IMPORT":
		*m = ModeImport
	case "READONLY":
		*m = ModeReadOnly
	case "READWRITE":
		*m = ModeReadWrite
	}
	return nil
}
