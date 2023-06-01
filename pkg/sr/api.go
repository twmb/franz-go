package sr

import (
	"context"
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
	defer func() { sort.Slice(types, func(i, j int) bool { return types[i] < types[j] }) }()
	return types, cl.get(ctx, "/schemas/types", &types)
}

// SchemaReference is a way for a one schema to reference another. The details
// for how referencing is done are type specific; for example, JSON objects
// that use the key "$ref" can refer to another schema via URL. For more details
// on references, see the following link:
//
//	https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#schema-references
//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html
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

// CommSubjectSchemas splits l and r into three sets: what is unique in l, what
// is unique in r, and what is common in both. Duplicates in either map are
// eliminated.
func CommSubjectSchemas(l, r []SubjectSchema) (luniq, runiq, common []SubjectSchema) {
	type svid struct {
		s  string
		v  int
		id int
	}
	m := make(map[svid]SubjectSchema)

	for _, sl := range l {
		m[svid{
			sl.Subject,
			sl.Version,
			sl.ID,
		}] = sl
	}
	for _, sr := range r {
		k := svid{
			sr.Subject,
			sr.Version,
			sr.ID,
		}
		switch _, exists := m[k]; exists {
		case false:
			runiq = append(runiq, sr)
		case true:
			common = append(common, sr)
		}
		delete(m, k)
	}
	for _, v := range m {
		luniq = append(luniq, v)
	}

	for _, s := range [][]SubjectSchema{
		luniq,
		runiq,
		common,
	} {
		sort.Slice(s, func(i, j int) bool {
			l, r := s[i], s[j]
			return l.Subject < r.Subject ||
				l.Subject == r.Subject && l.Version > r.Version
		})
	}

	return luniq, runiq, common
}

// HideShowDeleted is a typed bool indicating whether queries should show
// or hide soft deleted schemas / subjects.
type HideShowDeleted bool

const (
	// HideDeleted hides soft deleted schemas or subjects.
	HideDeleted = false
	// ShowDeleted shows soft deleted schemas or subjects.
	ShowDeleted = true
)

// Subjects returns subjects available in the registry.
func (cl *Client) Subjects(ctx context.Context, deleted HideShowDeleted) ([]string, error) {
	// GET /subjects?deleted={x}
	var subjects []string
	path := "/subjects"
	if deleted {
		path += "?deleted=true"
	}
	return subjects, cl.get(ctx, path, &subjects)
}

// SchemaByID returns the schema for a given schema ID.
func (cl *Client) SchemaByID(ctx context.Context, id int) (Schema, error) {
	// GET /schemas/ids/{id}
	var s Schema
	return s, cl.get(ctx, fmt.Sprintf("/schemas/ids/%d", id), &s)
}

// SchemaTextByID returns the actual text of a schema.
//
// For example, if the schema for an ID is
//
//	"{\"type\":\"boolean\"}"
//
// this will return
//
//	{"type":"boolean"}
func (cl *Client) SchemaTextByID(ctx context.Context, id int) (string, error) {
	// GET /schemas/ids/{id}/schema
	var s []byte
	if err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d/schema", id), &s); err != nil {
		return "", err
	}
	return string(s), nil
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
	return fmt.Sprintf("/config/%s?defaultToGlobal=true", url.PathEscape(subject))
}

func pathMode(subject string, force bool) string {
	if subject == "" {
		if force {
			return "/mode?force=true"
		}
		return "/mode"
	}
	if force {
		return fmt.Sprintf("/mode/%s?force=true", url.PathEscape(subject))
	}
	// set (no force), or delete
	return fmt.Sprintf("/mode/%s", url.PathEscape(subject))
}

// SchemaByVersion returns the schema for a given subject and version. You can
// use -1 as the version to return the latest schema.
func (cl *Client) SchemaByVersion(ctx context.Context, subject string, version int, deleted HideShowDeleted) (SubjectSchema, error) {
	// GET /subjects/{subject}/versions/{version}
	var ss SubjectSchema
	path := pathSubjectVersion(subject, version)
	if deleted {
		path += "?deleted=true"
	}
	return ss, cl.get(ctx, path, &ss)
}

// Schemas returns all schemas for the given subject.
func (cl *Client) Schemas(ctx context.Context, subject string, deleted HideShowDeleted) ([]SubjectSchema, error) {
	// GET /subjects/{subject}/versions => []int (versions)
	var versions []int
	path := pathSubjectWithVersion(subject)
	if deleted {
		path += "?deleted=true"
	}
	if err := cl.get(ctx, path, &versions); err != nil {
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
	defer cancel()
	for i := range versions {
		version := versions[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := cl.SchemaByVersion(cctx, subject, version, deleted)
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
	var id struct {
		ID int `json:"id"`
	}
	if err := cl.post(ctx, path, s, &id); err != nil {
		return SubjectSchema{}, err
	}

	usages, err := cl.SchemaUsagesByID(ctx, id.ID, HideDeleted)
	if err != nil {
		return SubjectSchema{}, err
	}
	for _, usage := range usages {
		if usage.Subject == subject {
			return usage, nil
		}
	}
	return SubjectSchema{}, fmt.Errorf("created schema under id %d, but unable to find SubjectSchema", id.ID)
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

// DeleteSubject deletes the subject. You must soft delete a subject before it
// can be hard deleted. This returns all versions that were deleted.
func (cl *Client) DeleteSubject(ctx context.Context, subject string, how DeleteHow) ([]int, error) {
	// DELETE /subjects/{subject}?permanent={x}
	path := pathSubject(subject)
	if how == HardDelete {
		path += "?permanent=true"
	}
	var versions []int
	defer func() { sort.Ints(versions) }()
	return versions, cl.delete(ctx, path, &versions)
}

// DeleteSchema deletes the schema at the given version. You must soft delete
// a schema before it can be hard deleted. You can use -1 to delete the latest
// version.
func (cl *Client) DeleteSchema(ctx context.Context, subject string, version int, how DeleteHow) error {
	// DELETE /subjects/{subject}/versions/{version}?permanent={x}
	path := pathSubjectVersion(subject, version)
	if how == HardDelete {
		path += "?permanent=true"
	}
	return cl.delete(ctx, path, nil)
}

// SchemaReferences returns all schemas that references the input
// subject-version. You can use -1 to check the latest version.
func (cl *Client) SchemaReferences(ctx context.Context, subject string, version int, deleted HideShowDeleted) ([]SubjectSchema, error) {
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
	defer cancel()
	for i := range ids {
		id := ids[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			idSchemas, err := cl.SchemaUsagesByID(cctx, id, deleted)
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
func (cl *Client) SchemaUsagesByID(ctx context.Context, id int, deleted HideShowDeleted) ([]SubjectSchema, error) {
	// GET /schemas/ids/{id}/versions
	// SchemaByVersion
	type subjectVersion struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}
	var subjectVersions []subjectVersion
	path := fmt.Sprintf("/schemas/ids/%d/versions", id)
	if deleted {
		path += "?deleted=true"
	}
	if err := cl.get(ctx, path, &subjectVersions); err != nil {
		return nil, err
	}

	var (
		schemas      = make([]SubjectSchema, len(subjectVersions))
		firstErr     error
		errOnce      uint32
		wg           sync.WaitGroup
		cctx, cancel = context.WithCancel(ctx)
	)
	defer cancel()
	for i := range subjectVersions {
		sv := subjectVersions[i]
		slot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := cl.SchemaByVersion(cctx, sv.Subject, sv.Version, deleted)
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
	Subject string             // The subject this compatibility result is for, or empty for the global level.
	Level   CompatibilityLevel // The subject (or global) compatibility level.
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
// that exists. You can use -1 to check compatibility with the latest version,
// and -2 to check compatibility against all versions.
func (cl *Client) CheckCompatibility(ctx context.Context, subject string, version int, s Schema) (bool, error) {
	// POST /compatibility/subjects/{subject}/versions/{version}?reason=true
	// POST /compatibility/subjects/{subject}/versions?reason=true
	path := "/compatibility" + pathSubjectVersion(subject, version) + "?verbose=true"
	if version == -2 {
		path = "/compatibility" + pathSubjectWithVersion(subject) + "?verbose=true"
	}
	var is struct {
		Is bool `json:"is_compatible"`
	}
	return is.Is, cl.post(ctx, path, s, &is)
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
			err := cl.get(ctx, pathMode(subject, false), &m)
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
// no subjects, this returns one element. Force can be used to force setting
// the mode even if the registry has existing schemas.
func (cl *Client) SetMode(ctx context.Context, mode Mode, force bool, subjects ...string) []ModeResult {
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
			err := cl.put(ctx, pathMode(subject, force), mode, &m)
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
			err := cl.delete(ctx, pathMode(subject, false), &m)
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
