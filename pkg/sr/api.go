package sr

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
)

// TODO
// * /contexts (get) // looks niche right now

// This file is an implementation of:
//
//     https://docs.confluent.io/platform/current/schema-registry/develop/api.html
//

type (
	// SchemaReference is a way for a one schema to reference another. The
	// details for how referencing is done are type specific; for example,
	// JSON objects that use the key "$ref" can refer to another schema via
	// URL. For more details on references, see the following link:
	//
	//	https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#schema-references
	//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html
	SchemaReference struct {
		Name    string `json:"name"`
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}

	// SchemaMetadata is arbitrary information about the schema or its
	// constituent parts, such as whether a field contains sensitive
	// information or who created a data contract.
	SchemaMetadata struct {
		Tags       map[string][]string `json:"tags,omitempty"`
		Properties map[string]string   `json:"properties,omitempty"`
		Sensitive  []string            `json:"sensitive,omitempty"`
	}

	// SchemaRule specifies integrity constraints or data policies in a
	// data contract. These data rules or policies can enforce that a field
	// that contains sensitive information must be encrypted, or that a
	// message containing an invalid age must be sent to a dead letter
	// queue
	//
	//	https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html#rules
	SchemaRule struct {
		Name      string            `json:"name"`                // Name is a user-defined name to reference the rule.
		Doc       string            `json:"doc,omitempty"`       // Doc is an optional description of the rule.
		Kind      SchemaRuleKind    `json:"kind"`                // Kind is the type of rule.
		Mode      SchemaRuleMode    `json:"mode"`                // Mode is the mode of the rule.
		Type      string            `json:"type"`                // Type is the type of rule, which invokes a specific rule executor, such as Google Common Expression Language (CEL) or JSONata.
		Tags      []string          `json:"tags"`                // Tags to which this rule applies.
		Params    map[string]string `json:"params,omitempty"`    // Optional params for the rule.
		Expr      string            `json:"expr"`                // Expr is the rule expression.
		OnSuccess string            `json:"onSuccess,omitempty"` // OnSuccess is an optional action to execute if the rule succeeds, otherwise the built-in action type NONE is used. For UPDOWN and WRITEREAD rules, one can specify two actions separated by commas, such as "NONE,ERROR" for a WRITEREAD rule. In this case NONE applies to WRITE and ERROR applies to READ
		OnFailure string            `json:"onFailure,omitempty"` // OnFailure is an optional action to execute if the rule fails, otherwise the built-in action type NONE is used. See OnSuccess for more details.
		Disabled  bool              `json:"disabled,omitempty"`  // Disabled specifies whether the rule is disabled.
	}

	// SchemaRuleSet groups migration rules and domain validation rules.
	SchemaRuleSet struct {
		MigrationRules []SchemaRule `json:"migrationRules,omitempty"`
		DomainRules    []SchemaRule `json:"domainRules,omitempty"`
	}

	// Schema is the object form of a schema for the HTTP API.
	Schema struct {
		// Schema is the actual unescaped text of a schema.
		Schema string `json:"schema"`

		// Type is the type of a schema. The default type is avro.
		Type SchemaType `json:"schemaType,omitempty"`

		// References declares other schemas this schema references. See the
		// docs on SchemaReference for more details.
		References []SchemaReference `json:"references,omitempty"`

		// SchemaMetadata is arbitrary information about the schema.
		SchemaMetadata *SchemaMetadata `json:"metadata,omitempty"`

		// SchemaRuleSet is a set of rules that govern the schema.
		SchemaRuleSet *SchemaRuleSet `json:"ruleSet,omitempty"`
	}

	// SubjectSchema pairs the subject, global identifier, and version of a
	// schema with the schema itself.
	SubjectSchema struct {
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
)

// SupportedTypes returns the schema types that are supported in the schema
// registry.
func (cl *Client) SupportedTypes(ctx context.Context) ([]SchemaType, error) {
	// GET /schemas/types
	var types []SchemaType
	defer func() { sort.Slice(types, func(i, j int) bool { return types[i] < types[j] }) }()
	err := cl.get(ctx, "/schemas/types", &types)
	return types, err
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

// Subjects returns subjects available in the registry.
//
// This supports params [SubjectPrefix], [ShowDeleted], and [DeletedOnly].
func (cl *Client) Subjects(ctx context.Context) ([]string, error) {
	// GET /subjects
	var subjects []string
	err := cl.get(ctx, "/subjects", &subjects)
	return subjects, err
}

// SchemaByID returns the schema for a given schema ID.
//
// This supports params [Subject], [Format], and [FetchMaxID].
func (cl *Client) SchemaByID(ctx context.Context, id int) (Schema, error) {
	// GET /schemas/ids/{id}
	var s Schema
	err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d", id), &s)
	return s, err
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
//
// This supports params [Subject], [Format].
func (cl *Client) SchemaTextByID(ctx context.Context, id int) (string, error) {
	// GET /schemas/ids/{id}/schema
	var s []byte
	if err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d/schema", id), &s); err != nil {
		return "", err
	}
	return string(s), nil
}

// SubjectsByID returns the subjects associated with a schema ID.
//
// This supports params [Subject] and [ShowDeleted].
func (cl *Client) SubjectsByID(ctx context.Context, id int) ([]string, error) {
	// GET /schemas/ids/{id}/subjects
	var subjects []string
	err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d/subjects", id), &subjects)
	return subjects, err
}

// SchemaVersion is a subject version pair.
type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SchemaVersionsByID returns all subject versions associated with a schema ID.
//
// This supports params [Subject] and [ShowDeleted].
func (cl *Client) SchemaVersionsByID(ctx context.Context, id int) ([]SubjectVersion, error) {
	// GET /schemas/ids/{id}/versions
	var versions []SubjectVersion
	err := cl.get(ctx, fmt.Sprintf("/schemas/ids/%d/versions", id), &versions)
	return versions, err
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

// SubjectVersions returns all versions for a given subject.
//
// This supports params [ShowDeleted] and [DeletedOnly].
func (cl *Client) SubjectVersions(ctx context.Context, subject string) ([]int, error) {
	// GET /subjects/{subject}/versions
	var versions []int
	err := cl.get(ctx, pathSubject(subject), &versions)
	return versions, err
}

// SchemaByVersion returns the schema for a given subject and version. You can
// use -1 as the version to return the latest schema.
//
// This supports param [ShowDeleted].
func (cl *Client) SchemaByVersion(ctx context.Context, subject string, version int) (SubjectSchema, error) {
	// GET /subjects/{subject}/versions/{version}
	var ss SubjectSchema
	path := pathSubjectVersion(subject, version)
	err := cl.get(ctx, path, &ss)
	return ss, err
}

// SchemaTextByVersion returns the actual text of a schema, by subject and
// version. You can use -1 as the version to return the latest schema.
//
// For example, if the schema for an ID is
//
//	"{\"type\":\"boolean\"}"
//
// this will return
//
//	{"type":"boolean"}
//
// This supports param [ShowDeleted].
func (cl *Client) SchemaTextByVersion(ctx context.Context, subject string, version int) (string, error) {
	// GET /subjects/{subject}/versions/{version}/schema
	var s []byte
	path := pathSubjectVersion(subject, version) + "/schema"
	if err := cl.get(ctx, path, &s); err != nil {
		return "", err
	}
	return string(s), nil
}

// AllSchemas returns all schemas for all subjects.
//
// This supports params [SubjectPrefix], [ShowDeleted], and [LatestOnly].
func (cl *Client) AllSchemas(ctx context.Context) ([]SubjectSchema, error) {
	// GET /schemas => []SubjectSchema
	var ss []SubjectSchema
	err := cl.get(ctx, "/schemas", &ss)
	return ss, err
}

// Schemas returns all schemas for the given subject.
//
// This supports param [ShowDeleted].
func (cl *Client) Schemas(ctx context.Context, subject string) ([]SubjectSchema, error) {
	// GET /subjects/{subject}/versions => []int (versions)
	var versions []int
	path := pathSubjectWithVersion(subject)
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
//
// This supports param [Normalize].
func (cl *Client) CreateSchema(ctx context.Context, subject string, s Schema) (SubjectSchema, error) {
	// POST /subjects/{subject}/versions => returns ID
	// Newer SR returns the full SubjectSchema, but old does not, so we
	// re-request to find the full information.
	path := pathSubjectWithVersion(subject)
	var id struct {
		ID int `json:"id"`
	}
	if err := cl.post(ctx, path, s, &id); err != nil {
		return SubjectSchema{}, err
	}

	usages, err := cl.SchemaUsagesByID(ctx, id.ID)
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
//
// This supports params Normalize and Deleted.
func (cl *Client) LookupSchema(ctx context.Context, subject string, s Schema) (SubjectSchema, error) {
	// POST /subjects/{subject}/
	var ss SubjectSchema
	err := cl.post(ctx, pathSubject(subject), s, &ss)
	return ss, err
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
	err := cl.delete(ctx, path, &versions)
	return versions, err
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
//
// This supports param [ShowDeleted].
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
	defer cancel()
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
//
// This supports param [ShowDeleted].
func (cl *Client) SchemaUsagesByID(ctx context.Context, id int) ([]SubjectSchema, error) {
	// GET /schemas/ids/{id}/versions
	// SchemaByVersion
	type subjectVersion struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}
	var subjectVersions []subjectVersion
	path := fmt.Sprintf("/schemas/ids/%d/versions", id)
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
	Subject string `json:"-"` // The subject this compatibility result is for, or empty for the global compatibility..

	Level            CompatibilityLevel `json:"compatibilityLevel"` // The subject (or global) compatibility level.
	Alias            string             `json:"alias"`              // The subject alias, if any.
	Normalize        bool               `json:"normalize"`          // Whether or not schemas are normalized by default.
	Group            string             `json:"compatibilityGroup"` // The compatibility group, if any. Only schemas in the same group are checked for compatibility.
	DefaultMetadata  *SchemaMetadata    `json:"defaultMetadata"`    // Default metadata used for schema registration.
	OverrideMetadata *SchemaMetadata    `json:"overrideMetadata"`   // Override metadata used for schema registration.
	DefaultRuleSet   *SchemaRuleSet     `json:"defaultRuleSet"`     // Default rule set used for schema registration.
	OverrideRuleSet  *SchemaRuleSet     `json:"overrideRuleSet"`    // Override rule set used for schema registration.

	Err error `json:"-"` // The error received for getting this compatibility.
}

// Compatibility returns the subject compatibility and global compatibility of
// each requested subject. The global compatibility can be requested by using
// either an empty subject or by specifying no subjects.
//
// This supports params [DefaultToGlobal].
//
// This can return 200 or 500 per result.
func (cl *Client) Compatibility(ctx context.Context, subjects ...string) []CompatibilityResult {
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
			var c CompatibilityResult
			err := cl.get(ctx, pathConfig(subject), &c)
			c.Subject = subject
			c.Err = err
			results[slot] = c
		}()
	}
	wg.Wait()

	return results
}

// SetCompatibility contains information used for setting global or per-subject
// compatibility configuration.
//
// The main difference between this and the CompatibilityResult is that this
// struct marshals the compatibility level as "compatibility".
type SetCompatibility struct {
	Subject string `json:"-"` // The subject this compatibility set is for, or empty for the global compatibility..

	Level            CompatibilityLevel `json:"compatibility"`                // The subject (or global) compatibility level.
	Alias            string             `json:"alias,omitempty"`              // The subject alias, if any.
	Normalize        bool               `json:"normalize,omitempty"`          // Whether or not schemas are normalized by default.
	Group            string             `json:"compatibilityGroup,omitempty"` // The compatibility group, if any. Only schemas in the same group are checked for compatibility.
	DefaultMetadata  *SchemaMetadata    `json:"defaultMetadata,omitempty"`    // Default metadata used for schema registration.
	OverrideMetadata *SchemaMetadata    `json:"overrideMetadata,omitempty"`   // Override metadata used for schema registration.
	DefaultRuleSet   *SchemaRuleSet     `json:"defaultRuleSet,omitempty"`     // Default rule set used for schema registration.
	OverrideRuleSet  *SchemaRuleSet     `json:"overrideRuleSet,omitempty"`    // Override rule set used for schema registration.

	Err error `json:"-"` // The error received for setting this compatibility.
}

// SetCompatibilitysets the compatibility for each requested subject. The
// global compatibility can be set by either using an empty subject or by
// specifying no subjects. If specifying no subjects, this returns one element.
func (cl *Client) SetCompatibility(ctx context.Context, compat SetCompatibility, subjects ...string) []CompatibilityResult {
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
			c := compat
			err := cl.put(ctx, pathConfig(subject), c, &c)
			results[slot] = CompatibilityResult{
				Subject:          subject,
				Level:            c.Level,
				Alias:            c.Alias,
				Normalize:        c.Normalize,
				Group:            c.Group,
				DefaultMetadata:  c.DefaultMetadata,
				OverrideMetadata: c.OverrideMetadata,
				DefaultRuleSet:   c.DefaultRuleSet,
				OverrideRuleSet:  c.OverrideRuleSet,
				Err:              err,
			}
		}()
	}
	wg.Wait()

	return results
}

// ResetCompatibility deletes any subject-level compatibility and reverts to the
// global default. The global compatibility can be reset by either using an
// empty subject or by specifying no subjects.
//
// This can return 200 or 500.
func (cl *Client) ResetCompatibility(ctx context.Context, subjects ...string) []CompatibilityResult {
	// DELETE /config/{subject}
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
			var c SetCompatibility // unmarshals with "compatibility"
			err := cl.delete(ctx, pathConfig(subject), &c)
			results[slot] = CompatibilityResult{
				Subject:          subject,
				Level:            c.Level,
				Alias:            c.Alias,
				Group:            c.Group,
				DefaultMetadata:  c.DefaultMetadata,
				OverrideMetadata: c.OverrideMetadata,
				DefaultRuleSet:   c.DefaultRuleSet,
				OverrideRuleSet:  c.OverrideRuleSet,
				Err:              err,
			}
		}()
	}
	wg.Wait()

	return results
}

// CheckCompatibilityResult is the response from the check compatibility endpoint.
type CheckCompatibilityResult struct {
	Is       bool     `json:"is_compatible"` // Is is true if the schema is compatible.
	Messages []string `json:"messages"`      // Messages contains reasons a schema is not compatible.
}

// CheckCompatibility checks if a schema is compatible with the given version
// that exists. You can use -1 to check compatibility with the latest version,
// and -2 to check compatibility against all versions.
//
// This supports params [Normalize] and [Verbose].
func (cl *Client) CheckCompatibility(ctx context.Context, subject string, version int, s Schema) (CheckCompatibilityResult, error) {
	// POST /compatibility/subjects/{subject}/versions/{version}
	// POST /compatibility/subjects/{subject}/versions
	path := "/compatibility" + pathSubjectVersion(subject, version)
	if version == -2 {
		path = "/compatibility" + pathSubjectWithVersion(subject)
	}
	var is CheckCompatibilityResult
	err := cl.post(ctx, path, s, &is)
	return is, err
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
//
// This supports params [DefaultToGlobal].
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
//
// This supports params [Force].
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
			err := cl.put(ctx, pathMode(subject), mode, &m)
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
