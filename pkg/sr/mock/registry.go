// Package mock provides an in-memory, concurrency-safe mock implementation of
// the Confluent Schema Registry REST API for unit and integration testing.
//
// The mock spins up an `httptest.Server`, persists all data in process memory,
// and exposes helpers for seeding or registering schemas programmatically. All
// public methods are safe for concurrent use and the implementation depends
// only on the Go standard library plus franz-go’s sr package.
//
// See the README for complete API coverage, limitations, and detailed examples.
package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// versionData holds the schema and its soft-delete status.
// We use a wrapper struct because we can't add fields to sr.SubjectSchema.
type versionData struct {
	schema    sr.SubjectSchema
	isDeleted bool
}

// subjectData holds all information related to a single subject.
type subjectData struct {
	versions       map[int]*versionData   // Using a pointer to modify isDeleted in place
	latestVersion  int                    // The highest version number among non-soft-deleted versions (0 means no active versions)
	highestVersion int                    // The highest version number ever assigned (persists through deletes to prevent reuse)
	isDeleted      bool                   // Whether the subject is soft-deleted
	compatLevel    *sr.CompatibilityLevel // Pointer to distinguish "not set" from a zero-value
}

// recalculateLatestVersion updates the latestVersion field to the highest
// version among non-soft-deleted versions. Sets to 0 if no active versions exist.
func (s *subjectData) recalculateLatestVersion() {
	maxVersion := 0
	for v, vData := range s.versions {
		if !vData.isDeleted && v > maxVersion {
			maxVersion = v
		}
	}
	s.latestVersion = maxVersion
}

// getOrCreateSubject returns the subjectData for the given subject, creating
// a new one if it doesn't exist. This method assumes the caller already holds
// the necessary lock.
func (r *Registry) getOrCreateSubject(subject string) *subjectData {
	subj := r.subjects[subject]
	if subj == nil {
		subj = &subjectData{
			versions: make(map[int]*versionData),
		}
		r.subjects[subject] = subj
	}
	return subj
}

// Registry is an in-memory Schema-Registry suitable for tests. All methods are
// safe for concurrent use.
type Registry struct {
	srv *httptest.Server

	mu sync.RWMutex
	// Consolidated state
	subjects    map[string]*subjectData
	schemasByID map[int]sr.Schema
	nextID      int

	// Global/unrelated state
	globalCompat sr.CompatibilityLevel

	// middleware
	expectedAuth string
	interceptors []Interceptor
	ictMu        sync.RWMutex
}

// Interceptor gets the first opportunity to handle a request. If it writes a
// response it must return true so that the default handler chain is skipped.
type Interceptor func(w http.ResponseWriter, r *http.Request) (handled bool)

// New spins up a Registry and applies any functional options.
func New(opts ...Option) *Registry {
	r := &Registry{
		// Simplified state initialization
		subjects:     make(map[string]*subjectData),
		schemasByID:  make(map[int]sr.Schema),
		nextID:       1,
		globalCompat: sr.CompatBackward,
	}

	for _, opt := range opts {
		opt(r)
	}

	mux := http.NewServeMux()

	// schema routes
	mux.HandleFunc("GET /schemas/ids/{id}", r.handleGetSchemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/schema", r.handleGetRawSchemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/versions", r.handleGetSchemaVersionsByID)

	// subject routes
	mux.HandleFunc("GET /subjects", r.handleGetSubjects)
	mux.HandleFunc("GET /subjects/{subject}/versions", r.handleGetSubjectVersions)
	mux.HandleFunc("POST /subjects/{subject}/versions", r.handlePostSubjectVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}", r.handleGetSubjectSchemaByVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}/schema", r.handleGetSubjectRawSchemaByVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}/referencedby", r.handleGetReferencedBy)
	mux.HandleFunc("DELETE /subjects/{subject}", r.handleDeleteSubject)
	mux.HandleFunc("DELETE /subjects/{subject}/versions/{version}", r.handleDeleteVersion)
	mux.HandleFunc("POST /subjects/{subject}", r.handleCheckSchema)

	// config routes
	mux.HandleFunc("GET /config", r.handleGetGlobalConfig)
	mux.HandleFunc("PUT /config", r.handlePutGlobalConfig)
	mux.HandleFunc("GET /config/{subject}", r.handleGetSubjectConfig)
	mux.HandleFunc("PUT /config/{subject}", r.handlePutSubjectConfig)
	mux.HandleFunc("DELETE /config/{subject}", r.handleDeleteSubjectConfig)

	// compatibility route
	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions/{version}", r.handleCheckCompatibility)

	// mode route
	mux.HandleFunc("GET /mode", r.handleGetMode)

	var h http.Handler = mux
	if r.expectedAuth != "" {
		h = r.authMiddleware(h)
	}
	h = r.interceptorMiddleware(h)

	r.srv = httptest.NewServer(h)
	return r
}

// Close shuts down the underlying server.
func (r *Registry) Close() { r.srv.Close() }

// URL returns the base URL of the running mock.
func (r *Registry) URL() string { return r.srv.URL }

// Intercept appends handler to the interceptor chain.
func (r *Registry) Intercept(i Interceptor) {
	r.ictMu.Lock()
	r.interceptors = append(r.interceptors, i)
	r.ictMu.Unlock()
}

// ClearInterceptors removes every registered interceptor.
func (r *Registry) ClearInterceptors() {
	r.ictMu.Lock()
	r.interceptors = nil
	r.ictMu.Unlock()
}

// SeedSchema inserts an existing schema at subject/version/id. It panics if the
// slot or id is already populated or if the schema is invalid.
func (r *Registry) SeedSchema(subject string, version, id int, sch sr.Schema) {
	if err := r.validateSchema(sch); err != nil {
		panic(fmt.Sprintf("invalid schema: %v", err))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.schemasByID[id]; ok {
		panic(fmt.Sprintf("schema id %d already seeded", id))
	}

	// Get or create subject data
	subj := r.getOrCreateSubject(subject)

	if _, dup := subj.versions[version]; dup {
		panic(fmt.Sprintf("subject %q version %d already seeded", subject, version))
	}

	if err := r.validateReferences(sch); err != nil {
		panic(fmt.Sprintf("invalid references: %v", err))
	}

	// Store schema globally
	r.schemasByID[id] = sch

	// Store schema version data
	subj.versions[version] = &versionData{
		schema: sr.SubjectSchema{
			Subject: subject,
			Version: version,
			ID:      id,
			Schema:  sch,
		},
		isDeleted: false,
	}

	// Update version tracking
	if version > subj.latestVersion {
		subj.latestVersion = version
	}
	if version > subj.highestVersion {
		subj.highestVersion = version
	}
	if id >= r.nextID {
		r.nextID = id + 1
	}
}

// RegisterSchema emulates POST /subjects/{subject}/versions.
func (r *Registry) RegisterSchema(subject string, schema sr.Schema) (id, version int, err error) {
	if err = r.validateSubject(subject); err != nil {
		return 0, 0, err
	}
	if err = r.validateSchema(schema); err != nil {
		return 0, 0, err
	}

	// Hold write lock for entire operation to prevent race conditions.
	// The "check-then-act" sequence must be atomic to ensure data integrity.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Validate references under write lock to prevent dangling references
	if err = r.validateReferences(schema); err != nil {
		return 0, 0, err
	}

	// Get or create subject data
	subj := r.getOrCreateSubject(subject)

	// Check if subject is soft-deleted
	if subj.isDeleted {
		return 0, 0, errSubjectNotFound(subject)
	}

	// existing identical schema under same subject?
	for v, versionData := range subj.versions {
		if !versionData.isDeleted && r.schemasEqual(versionData.schema.Schema, schema) {
			return versionData.schema.ID, v, nil
		}
	}

	// reuse id from other subject?
	for existingID, existing := range r.schemasByID {
		if !r.schemasEqual(existing, schema) {
			continue
		}
		version = subj.highestVersion + 1

		// Check for circular dependencies before storing the schema
		if err = r.validateCircularReferences(schema, subject, version); err != nil {
			return 0, 0, err
		}

		// Store schema version data
		subj.versions[version] = &versionData{
			schema: sr.SubjectSchema{
				Subject: subject,
				Version: version,
				ID:      existingID,
				Schema:  schema,
			},
			isDeleted: false,
		}
		subj.latestVersion = version
		subj.highestVersion = version
		return existingID, version, nil
	}

	// brand-new schema
	id = r.nextID
	r.nextID++
	version = subj.highestVersion + 1

	// Check for circular dependencies before storing the schema
	if err = r.validateCircularReferences(schema, subject, version); err != nil {
		return 0, 0, err
	}

	// Store schema globally
	r.schemasByID[id] = schema

	// Store schema version data
	subj.versions[version] = &versionData{
		schema: sr.SubjectSchema{
			Subject: subject,
			Version: version,
			ID:      id,
			Schema:  schema,
		},
		isDeleted: false,
	}
	subj.latestVersion = version
	subj.highestVersion = version
	return id, version, nil
}

// SetCompat emulates PUT /config/{subject}.
func (r *Registry) SetCompat(subject string, c sr.CompatibilityLevel) error {
	if c.String() == "" {
		return fmt.Errorf("empty compatibility level")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	// Get or create subject data
	subj := r.getOrCreateSubject(subject)

	subj.compatLevel = &c
	return nil
}

// GetSchema retrieves a schema by subject and version.
func (r *Registry) GetSchema(subject string, version int) (sr.SubjectSchema, bool) {
	return r.getSchemaBySubjectVersion(subject, version)
}

// SubjectExists returns true if subject exists and has not been hard-deleted.
func (r *Registry) SubjectExists(subject string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	return ok && !subj.isDeleted
}

// Reset wipes all in-memory state. Useful between tests.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.subjects = make(map[string]*subjectData)
	r.schemasByID = make(map[int]sr.Schema)
	r.nextID = 1
	r.globalCompat = sr.CompatBackward
}

// getSchemaBySubjectVersionLocked is like getSchemaBySubjectVersion but assumes caller holds lock
func (r *Registry) getSchemaBySubjectVersionLocked(subject string, version int) (sr.SubjectSchema, bool) {
	subj, ok := r.subjects[subject]
	if !ok || subj.isDeleted {
		return sr.SubjectSchema{}, false
	}

	versionData, ok := subj.versions[version]
	if !ok || versionData.isDeleted {
		return sr.SubjectSchema{}, false
	}

	return versionData.schema, true
}

// isSchemaReferenced checks if the given schema version is referenced by any other schemas.
// Assumes the caller already holds a lock.
func (r *Registry) isSchemaReferenced(targetSubject string, targetVersion int) (bool, []int) {
	var referencingIDs []int

	for _, subj := range r.subjects {
		// Skip soft-deleted subjects
		if subj.isDeleted {
			continue
		}

		for _, versionData := range subj.versions {
			// Skip soft-deleted versions
			if versionData.isDeleted {
				continue
			}

			// Check if this schema references our target
			for _, ref := range versionData.schema.References {
				if ref.Subject == targetSubject && ref.Version == targetVersion {
					referencingIDs = append(referencingIDs, versionData.schema.ID)
					break // Only add each schema ID once
				}
			}
		}
	}

	return len(referencingIDs) > 0, referencingIDs
}

// deleteSubject performs a soft or permanent delete of a subject.
// This method assumes the caller already holds the necessary lock.
// Returns the list of version numbers that were deleted.
func (r *Registry) deleteSubject(subject string, permanent bool) ([]int, error) {
	subj, ok := r.subjects[subject]
	if !ok {
		return nil, errSubjectNotFound(subject)
	}

	var versions []int
	for v := range subj.versions {
		versions = append(versions, v)
	}
	sort.Ints(versions)

	// Check if any version of this subject is still referenced
	var allReferencingIDs []int
	for _, version := range versions {
		if isReferenced, referencingIDs := r.isSchemaReferenced(subject, version); isReferenced {
			allReferencingIDs = append(allReferencingIDs, referencingIDs...)
		}
	}
	if len(allReferencingIDs) > 0 {
		return nil, newErr(http.StatusConflict, errCodeInvalidSchema,
			"Cannot delete subject %s as its schemas are still referenced by schema IDs: %v",
			subject, allReferencingIDs)
	}

	if permanent {
		// Hard delete - remove entire subject
		delete(r.subjects, subject)
	} else {
		// Soft delete - mark subject as deleted
		subj.isDeleted = true
	}

	return versions, nil
}

// deleteVersion performs a soft or permanent delete of a specific version.
// This method assumes the caller already holds the necessary lock.
func (r *Registry) deleteVersion(subject string, version int, permanent bool) error {
	subj, ok := r.subjects[subject]
	if !ok {
		return errSubjectNotFound(subject)
	}

	versionData, ok := subj.versions[version]
	if !ok {
		return errVersionNotFound(subject, version)
	}

	// Check if schema is still referenced
	if isReferenced, referencingIDs := r.isSchemaReferenced(subject, version); isReferenced {
		return errSchemaIsReferenced(subject, version, referencingIDs)
	}

	if permanent {
		// Hard delete - remove version data
		delete(subj.versions, version)
	} else {
		// Soft delete - mark version as deleted
		versionData.isDeleted = true
	}

	// Recalculate latest version if we deleted the current latest
	if subj.latestVersion == version {
		subj.recalculateLatestVersion()
	}

	return nil
}

func (r *Registry) resolveVersion(subject, versionStr string) (int, error) {
	if versionStr == "latest" {
		r.mu.RLock()
		defer r.mu.RUnlock()

		subj, ok := r.subjects[subject]
		if !ok {
			// Subject truly doesn't exist
			return 0, errSubjectNotFound(subject)
		}
		if subj.latestVersion == 0 {
			// Subject exists but has no active versions - for "latest" requests,
			// this should be treated as "subject not found" per Confluent API behavior
			return 0, errSubjectNotFound(subject)
		}
		return subj.latestVersion, nil
	}
	v, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, errInvalidVersion("invalid version")
	}
	return v, nil
}

func (r *Registry) getSchemaBySubjectVersion(subject string, version int) (sr.SubjectSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getSchemaBySubjectVersionLocked(subject, version)
}

func (*Registry) schemasEqual(a, b sr.Schema) bool {
	ta := a.Type
	if ta == 0 {
		ta = sr.TypeAvro
	}
	tb := b.Type
	if tb == 0 {
		tb = sr.TypeAvro
	}
	if ta != tb {
		return false
	}

	normA, errA := normalizeSchema(a.Schema, ta)
	normB, errB := normalizeSchema(b.Schema, tb)
	if errA != nil || errB != nil {
		return a.Schema == b.Schema
	}
	if normA != normB {
		return false
	}

	if len(a.References) != len(b.References) {
		return false
	}
	ra := append([]sr.SchemaReference(nil), a.References...)
	rb := append([]sr.SchemaReference(nil), b.References...)
	sort.Slice(ra, func(i, j int) bool { return ra[i].Name < ra[j].Name })
	sort.Slice(rb, func(i, j int) bool { return rb[i].Name < rb[j].Name })
	for i := range ra {
		if ra[i] != rb[i] {
			return false
		}
	}
	return true
}

func (*Registry) validateSubject(subject string) error {
	if subject == "" {
		return errInvalidSchema("subject name cannot be empty")
	}
	if len(subject) > 255 {
		return errInvalidSchema("subject name too long (max 255 characters)")
	}
	// Check for null bytes which can cause issues
	if strings.Contains(subject, "\x00") {
		return errInvalidSchema("subject name cannot contain null bytes")
	}
	return nil
}

func (*Registry) validateSchema(s sr.Schema) error {
	switch s.Type {
	case sr.TypeAvro, sr.TypeJSON:
		var tmp any
		if err := json.Unmarshal([]byte(s.Schema), &tmp); err != nil {
			return errInvalidSchemaWithCause(err, fmt.Sprintf("invalid %s schema: %v", s.Type.String(), err))
		}
	case sr.TypeProtobuf:
		if strings.TrimSpace(s.Schema) == "" {
			return errInvalidSchema("empty protobuf schema")
		}
	default:
		return errInvalidSchema(fmt.Sprintf("unsupported schema type: %s", s.Type.String()))
	}
	return nil
}

func (r *Registry) validateReferences(s sr.Schema) error {
	for _, ref := range s.References {
		if ref.Name == "" {
			return errInvalidSchema("reference missing name")
		}
		if ref.Subject == "" {
			return errInvalidSchema(fmt.Sprintf("reference %q missing subject", ref.Name))
		}
		if ref.Version <= 0 {
			return errInvalidSchema(fmt.Sprintf("reference %q has invalid version %d", ref.Name, ref.Version))
		}

		// Check if the referenced schema exists and is not soft-deleted
		// This assumes the caller already holds the necessary lock
		subj, ok := r.subjects[ref.Subject]
		if !ok || subj.isDeleted {
			return errInvalidReference(ref)
		}

		versionData, ok := subj.versions[ref.Version]
		if !ok || versionData.isDeleted {
			return errInvalidReference(ref)
		}
	}
	return nil
}

// validateCircularReferences checks for circular dependencies in schema references.
// This should be called after validateReferences to ensure all references exist.
func (r *Registry) validateCircularReferences(schema sr.Schema, subject string, version int) error {
	visited := make(map[string]bool)
	subjectStack := make(map[string]bool)

	return r.detectCycle(schema, subject, version, visited, subjectStack)
}

// detectCycle performs depth-first search to detect cycles in the reference graph
func (r *Registry) detectCycle(schema sr.Schema, subject string, version int, visited, subjectStack map[string]bool) error {
	// Create a unique key for this schema version
	key := fmt.Sprintf("%s:%d", subject, version)

	// If this subject is already in our traversal stack, we have a cycle
	if subjectStack[subject] {
		return errCircularDependency(subject)
	}

	// Mark current node as visited and current subject in stack
	visited[key] = true
	subjectStack[subject] = true

	// Visit all references
	for _, ref := range schema.References {
		refKey := fmt.Sprintf("%s:%d", ref.Subject, ref.Version)

		// If not visited, recursively check this reference
		if !visited[refKey] {
			// Get the referenced schema
			if refSubject, ok := r.subjects[ref.Subject]; ok {
				if refVersionData, ok := refSubject.versions[ref.Version]; ok {
					if err := r.detectCycle(refVersionData.schema.Schema, ref.Subject, ref.Version, visited, subjectStack); err != nil {
						return err
					}
				}
			}
		}
	}

	// Remove from subject stack before returning
	delete(subjectStack, subject)
	return nil
}

func normalizeSchema(raw string, t sr.SchemaType) (string, error) {
	switch t {
	case sr.TypeAvro, sr.TypeJSON:
		var v any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return "", err
		}
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default: // protobuf or anything else: return as-is
		return raw, nil
	}
}
