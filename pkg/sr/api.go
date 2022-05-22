package sr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
)

// This file is an implementation of:
//
//     https://docs.confluent.io/platform/current/schema-registry/develop/api.html
//

// SupportedTypes returns the schema types that are supported in the schema
// registry.
func (cl *Client) SupportedTypes(ctx context.Context) ([]SchemaType, error) {
	// GET /schemas/types
	var types []SchemaType
	return types, cl.get(ctx, "/schemas/types", &types)
}

// SchemaReference is a way for a one schema to reference another. The details
// for how referencing is done are type specific; for example, JSON objects
// that use the key "$ref" can refer to another schema via URL. For more details
// on references, see the following link:
//
//     https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#schema-references
//     https://docs.confluent.io/platform/current/schema-registry/develop/api.html
//
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Schema is the object form of a schema for the HTTP API.
type Schema struct {
	// Schema is the actual unescaped text of a schema.
	Schema string `json:"schema"`

	// Type is the type of a schema. The default type is avro.
	Type SchemaType `json:"schemaType,omitempty"`

	// References declares other schemas this schema references. See the
	// docs on SchemaReference for more details.
	References []SchemaReference `json:"references,omitempty"`
}

type rawSchema struct {
	Schema     string            `json:"schema"`
	Type       SchemaType        `json:"schemaType,omitempty"`
	References []SchemaReference `json:"references,omitempty"`
}

func (s *Schema) UnmarshalJSON(b []byte) error {
	var raw rawSchema
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	*s = Schema(raw)
	if err := json.Unmarshal([]byte(raw.Schema), &s.Schema); err != nil {
		return err
	}
	return nil
}

func (s Schema) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(s.Schema)
	if err != nil {
		return nil, err
	}
	raw := rawSchema(s)
	raw.Schema = string(b)
	return json.Marshal(raw)
}

// SubjectSchema pairs the subject, global identifier, and version of a schema
// with the schema itself.
type SubjectSchema struct {
	// Subject is the subject for this schema. This usually corresponds to
	// a Kafka topic, and whether this is for a key or value. For example,
	// "foo-key" would be the subject for the foo topic for serializing the
	// key field of a record.
	Subject string `json:"subject"`

	// Version is the version of this subject.
	Version int `json:"version"`

	// ID is the globally unique ID of the schema.
	ID int `json:"id"`

	Schema
}

// Subjects returns all alive and soft-deleted subjects available in the
// registry.
func (cl *Client) Subjects(ctx context.Context) (alive, softDeleted []string, err error) {
	// GET /subjects?deleted={x}
	if err = cl.get(ctx, "/subjects", &alive); err != nil {
		return nil, nil, err
	}
	var all []string
	if err = cl.get(ctx, "/subjects?deleted=true", &all); err != nil {
		return nil, nil, err
	}
	mdeleted := make(map[string]struct{}, len(all))
	for _, subject := range all {
		mdeleted[subject] = struct{}{}
	}
	for _, subject := range alive {
		delete(mdeleted, subject)
	}
	for subject := range mdeleted {
		softDeleted = append(softDeleted, subject)
	}
	sort.Strings(alive)
	sort.Strings(softDeleted)
	return alive, softDeleted, nil
}

// SchemaTextByID returns the actual text of a schema.
//
// For example, if the schema for an ID is
//
//     "{\"type\":\"boolean\"}"
//
// this will return
//
//     {"type":"boolean"}
//
func (cl *Client) SchemaTextByID(ctx context.Context, id int) (string, error) {
	// GET /schemas/ids/{id}
	var s Schema
	if err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d", id), &s); err != nil {
		return "", err
	}
	return s.Schema, nil
}

func pathSubject(subject string) string            { return fmt.Sprintf("/subjects/%s", url.PathEscape(subject)) }
func pathSubjectWithVersion(subject string) string { return pathSubject(subject) + "/versions" }
func pathSubjectVersion(subject string, version int) string {
	if version == -1 {
		return pathSubjectWithVersion(subject) + "/latest"
	}
	return fmt.Sprintf("%s/%d", pathSubjectWithVersion(subject), version)
}

func pathConfig(subject string) string {
	if subject == "" {
		return "/config"
	}
	return fmt.Sprintf("/config/%s", url.PathEscape(subject))
}

func pathMode(subject string) string {
	if subject == "" {
		return "/mode"
	}
	return fmt.Sprintf("/mode/%s", url.PathEscape(subject))
}

// SchemaByVersion returns the schema for a given subject and version. You can
// use -1 as the version to return the latest schema.
func (cl *Client) SchemaByVersion(ctx context.Context, subject string, version int) (SubjectSchema, error) {
	// GET /subjects/{subject}/versions/{version}
	var ss SubjectSchema
	return ss, cl.get(ctx, pathSubjectVersion(subject, version), &ss)
}

// Schemas returns all schemas for the given subject.
func (cl *Client) Schemas(ctx context.Context, subject string) ([]SubjectSchema, error) {
	// GET /subjects/{subject}/versions => []int (versions)
	var versions []int
	if err := cl.get(ctx, pathSubjectWithVersion(subject), &versions); err != nil {
		return nil, err
	}
	sort.Ints(versions)

	var (
		schemas      = make([]SubjectSchema, len(versions))
		firstErr     error
		errOnce      uint32
		wg           sync.WaitGroup
		cctx, cancel = context.WithCancel(ctx)
	)
	for i := range versions {
		version := versions[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := cl.SchemaByVersion(cctx, subject, version)
			schemas[slot] = s
			if err != nil && atomic.SwapUint32(&errOnce, 1) == 0 {
				firstErr = err
				cancel()
			}
		}()
	}
	wg.Wait()

	return schemas, firstErr
}

// CreateSchema attempts to create a schema in the given subject.
func (cl *Client) CreateSchema(ctx context.Context, subject string, s Schema) (SubjectSchema, error) {
	// POST /subjects/{subject}/versions => returns ID
	path := pathSubjectWithVersion(subject)
	if cl.normalize {
		path += "?normalize=true"
	}
	var id int
	if err := cl.post(ctx, path, s, &id); err != nil {
		return SubjectSchema{}, err
	}

	usages, err := cl.SchemaUsagesByID(ctx, id)
	if err != nil {
		return SubjectSchema{}, err
	}
	for _, usage := range usages {
		if usage.Subject == subject {
			return usage, nil
		}
	}
	return SubjectSchema{}, fmt.Errorf("created schema under id %d, but unable to find SubjectSchema")
}

// LookupSchema checks to see if a schema is already registered and if so,
// returns its ID and version in the SubjectSchema.
func (cl *Client) LookupSchema(ctx context.Context, subject string, s Schema) (SubjectSchema, error) {
	// POST /subjects/{subject}/
	path := pathSubject(subject)
	if cl.normalize {
		path += "?normalize=true"
	}
	var ss SubjectSchema
	return ss, cl.post(ctx, path, s, &ss)
}

// DeleteHow is a typed bool indicating how subjects or schemas should be
// deleted.
type DeleteHow bool

const (
	// SoftDelete performs a soft deletion.
	SoftDelete = false
	// HardDelete performs a hard deletion. Values must be soft deleted
	// before they can be hard deleted.
	HardDelete = true
)

// DeleteSubjects deletes the subject. You must soft delete a subject before it
// can be hard deleted. This returns all versions that were deleted.
func (cl *Client) DeleteSubject(ctx context.Context, how DeleteHow, subject string) ([]int, error) {
	// DELETE /subjects/{subject}?permanent={x}
	path := pathSubject(subject)
	if how == HardDelete {
		path += "?permanent=true"
	}
	var versions []int
	defer func() { sort.Ints(versions) }()
	return versions, cl.delete(ctx, path, &versions)
}

// DeleteSubjects deletes the schema at the given version. You must soft delete
// a schema before it can be hard deleted. You can use -1 to delete the latest
// version.
func (cl *Client) DeleteSchema(ctx context.Context, how DeleteHow, subject string, version int) error {
	// DELETE /subjects/{subject}/versions/{version}?permanent={x}
	path := pathSubjectVersion(subject, version)
	if how == HardDelete {
		path += "?permanent=true"
	}
	return cl.delete(ctx, path, nil)
}

// SchemaReferences returns all schemas that references the input
// subject-version. You can use -1 to check the latest version.
func (cl *Client) SchemaReferences(ctx context.Context, subject string, version int) ([]SubjectSchema, error) {
	// GET /subjects/{subject}/versions/{version}/referencedby
	// SchemaUsagesByID
	var ids []int
	if err := cl.get(ctx, pathSubjectVersion(subject, version)+"/referencedby", &ids); err != nil {
		return nil, err
	}

	var (
		schemas      []SubjectSchema
		firstErr     error
		mu           sync.Mutex
		wg           sync.WaitGroup
		cctx, cancel = context.WithCancel(ctx)
	)
	for i := range ids {
		id := ids[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			idSchemas, err := cl.SchemaUsagesByID(cctx, id)
			mu.Lock()
			defer mu.Unlock()
			schemas = append(schemas, idSchemas...)
			if err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		}()
	}
	wg.Wait()

	return schemas, firstErr
}

// SchemaUsagesByID returns all usages of a given schema ID. A single schema's
// can be reused in many subject-versions; this function can be used to map a
// schema to all subject-versions that use it.
func (cl *Client) SchemaUsagesByID(ctx context.Context, id int) ([]SubjectSchema, error) {
	// GET /schemas/ids/{id}/versions
	// SchemaByVersion
	type subjectVersion struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}
	var subjectVersions []subjectVersion
	if err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d/versions", id), &subjectVersions); err != nil {
		return nil, err
	}

	var (
		schemas      = make([]SubjectSchema, len(subjectVersions))
		firstErr     error
		errOnce      uint32
		wg           sync.WaitGroup
		cctx, cancel = context.WithCancel(ctx)
	)
	for i := range subjectVersions {
		sv := subjectVersions[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := cl.SchemaByVersion(cctx, sv.Subject, sv.Version)
			schemas[slot] = s
			if err != nil && atomic.SwapUint32(&errOnce, 1) == 0 {
				firstErr = err
				cancel()
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	type ssi struct {
		subject string
		version int
		id      int
	}

	uniq := make(map[ssi]SubjectSchema)
	for _, s := range schemas {
		uniq[ssi{
			subject: s.Subject,
			version: s.Version,
			id:      s.ID,
		}] = s
	}
	schemas = nil
	for _, s := range uniq {
		schemas = append(schemas, s)
	}
	return schemas, nil
}

// GlobalSubject is a constant to make API usage of requesting global subjects
// clearer.
const GlobalSubject = ""

// CompatibilityResult is the compatibility level for a subject.
type CompatibilityResult struct {
	Subject string             // The subject this compatbility result is for, or empty for the global level.
	Level   CompatibilityLevel // The subject (or global) compatibilty level.
	Err     error              // The error received for getting this compatibility level.
}

// CompatibilityLevel returns the subject level and global level compatibility
// of each requested subject. The global level can be requested by using either
// an empty subject or by specifying no subjects.
func (cl *Client) CompatibilityLevel(ctx context.Context, subjects ...string) []CompatibilityResult {
	// GET /config/{subject}
	// GET /config
	if len(subjects) == 0 {
		subjects = append(subjects, GlobalSubject)
	}
	var (
		wg      sync.WaitGroup
		results = make([]CompatibilityResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c struct {
				Level CompatibilityLevel `json:"compatibilityLevel"`
			}
			err := cl.get(ctx, pathConfig(subject), &c)
			results[slot] = CompatibilityResult{
				Subject: subject,
				Level:   c.Level,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}

// SetCompatibilityLevel sets the compatibility level for each requested
// subject. The global level can be set by either using an empty subject or by
// specifying no subjects. If specifying no subjects, this returns one element.
func (cl *Client) SetCompatibilityLevel(ctx context.Context, level CompatibilityLevel, subjects ...string) []CompatibilityResult {
	// PUT /config/{subject}
	// PUT /config
	if len(subjects) == 0 {
		subjects = append(subjects, GlobalSubject)
	}
	var (
		wg      sync.WaitGroup
		results = make([]CompatibilityResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := struct {
				Level CompatibilityLevel `json:"compatibility"`
			}{level}
			err := cl.put(ctx, pathConfig(subject), c, &c)
			results[slot] = CompatibilityResult{
				Subject: subject,
				Level:   c.Level,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}

// ResetCompatibilityLevel deletes any subject-level compatibility level and
// reverts to the global default.
func (cl *Client) ResetCompatibilityLevel(ctx context.Context, subjects ...string) []CompatibilityResult {
	// DELETE /config/{subject}
	if len(subjects) == 0 {
		return nil
	}
	var (
		wg      sync.WaitGroup
		results = make([]CompatibilityResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var c struct {
				Level CompatibilityLevel `json:"compatibility"`
			}
			err := cl.delete(ctx, pathConfig(subject), &c)
			results[slot] = CompatibilityResult{
				Subject: subject,
				Level:   c.Level,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}

// CheckCompatibility checks if a schema is compatible with the given version
// that exists. You can use -1 to check compatibility with all versions.
func (cl *Client) CheckCompatibility(ctx context.Context, subject string, version int, s Schema) (bool, error) {
	// POST /compatibility/subjects/{subject}/versions/{version}?reason=true
	// POST /compatibility/subjects/{subject}/versions?reason=true
	path := pathSubjectVersion(subject, version)
	if version == -1 {
		path = pathSubjectWithVersion(subject)
	}
	var is bool
	return is, cl.post(ctx, path, s, &is)
}

// ModeResult is the mode for a subject.
type ModeResult struct {
	Subject string // The subject this mode result is for, or empty for the global mode.
	Mode    Mode   // The subject (or global) mode.
	Err     error  // The error received for getting this mode.
}

type modeResponse struct {
	Mode Mode `json:"mode"`
}

// Mode returns the subject and global mode of each requested subject. The
// global mode can be requested by using either an empty subject or by
// specifying no subjects.
func (cl *Client) Mode(ctx context.Context, subjects ...string) []ModeResult {
	// GET /mode/{subject}
	// GET /mode
	if len(subjects) == 0 {
		subjects = append(subjects, GlobalSubject)
	}
	var (
		wg      sync.WaitGroup
		results = make([]ModeResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m modeResponse
			err := cl.get(ctx, pathMode(subject), &m)
			results[slot] = ModeResult{
				Subject: subject,
				Mode:    m.Mode,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}

// SetMode sets the mode for each requested subject. The global mode can be set
// by either using an empty subject or by specifying no subjects. If specifying
// no subjects, this returns one element.
func (cl *Client) SetMode(ctx context.Context, mode Mode, subjects ...string) []ModeResult {
	// PUT /mode/{subject}
	// PUT /mode
	if len(subjects) == 0 {
		subjects = append(subjects, GlobalSubject)
	}
	var (
		wg      sync.WaitGroup
		results = make([]ModeResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m modeResponse
			err := cl.put(ctx, pathMode(subject), m, &m)
			results[slot] = ModeResult{
				Subject: subject,
				Mode:    m.Mode,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}

// ResetMode deletes any subject modes and reverts to the global default.
func (cl *Client) ResetMode(ctx context.Context, subjects ...string) []ModeResult {
	// DELETE /mode/{subject}
	if len(subjects) == 0 {
		return nil
	}
	var (
		wg      sync.WaitGroup
		results = make([]ModeResult, len(subjects))
	)
	for i := range subjects {
		subject := subjects[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m modeResponse
			err := cl.delete(ctx, pathMode(subject), &m)
			results[slot] = ModeResult{
				Subject: subject,
				Mode:    m.Mode,
				Err:     err,
			}
		}()
	}
	wg.Wait()

	return results
}
