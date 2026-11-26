// Package srfake provides an in-memory, concurrency-safe mock implementation of
// the Confluent Schema Registry REST API for unit and integration testing.
//
// The mock spins up an `httptest.Server`, persists all data in process memory,
// and exposes helpers for seeding or registering schemas programmatically. All
// public methods are safe for concurrent use and the implementation depends
// only on the Go standard library plus franz-go’s sr package.
//
// See the README for complete API coverage, limitations, and detailed examples.
package srfake

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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
	versions       map[int]*versionData // Using a pointer to modify isDeleted in place
	latestVersion  int                  // The highest version number among non-soft-deleted versions (0 means no active versions)
	highestVersion int                  // The highest version, soft-deleted or not; a permanent delete of it frees its number for reuse, as in Confluent's registry
}

// Reports whether the subject has a version that is not soft-deleted. As in
// Confluent's registry, a subject whose versions are all soft-deleted is
// soft-deleted.
func (s *subjectData) active() bool { return s.latestVersion > 0 }

// Returns the highest version, soft-deleted or not, or 0 if there is none.
func (s *subjectData) highestExisting() int {
	highest := 0
	for v := range s.versions {
		highest = max(highest, v)
	}
	return highest
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
	srv     *httptest.Server
	handler http.Handler

	mu sync.RWMutex
	// Consolidated state
	subjects  map[string]*subjectData
	highestID map[string]int // the highest schema ID ever used in each context

	// idsByGUID is the ID that a registration of a schema GUID in a context
	// reuses, while a version still carries the ID. As in Confluent's
	// registry, it is the ID of the last version seeded, registered, or
	// soft-deleted with the GUID.
	idsByGUID map[contextGUID]int

	// contexts are the contexts besides the default context that a schema
	// was seeded or registered in. As in Confluent's registry, a context
	// stays until DELETE /contexts removes it.
	contexts map[string]bool

	// compats and modes are the compatibility levels and modes that are set,
	// by subject. As in Confluent's registry, the empty subject is the global
	// setting, and ":.ctx:" is the setting of context .ctx. A zero level is a
	// subject that is configured without a level.
	compats       map[string]sr.CompatibilityLevel
	modes         map[string]sr.Mode
	defaultCompat sr.CompatibilityLevel // the global level when none is set

	// middleware
	expectedAuth string
	interceptors []Interceptor
	ictMu        sync.RWMutex
}

// contextGUID is a schema GUID in a context.
type contextGUID struct {
	ctx, guid string
}

// Interceptor gets the first opportunity to handle a request. If it writes a
// response it must return true so that the default handler chain is skipped.
type Interceptor func(w http.ResponseWriter, r *http.Request) (handled bool)

// New spins up a Registry backed by an httptest.Server (on a random port) and
// applies any functional options. Use URL to get its address.
//
// To run the registry on a listener you control (for example a fixed port),
// use NewRegistry instead and serve its Handler yourself.
func New(opts ...Option) *Registry {
	r := NewRegistry(opts...)
	r.srv = httptest.NewServer(r.handler)
	return r
}

// NewRegistry builds a Registry without starting any server. The caller serves
// its Handler (for example via http.Serve on a net.Listener bound to a chosen
// port). URL returns "" since no server is started and Close is a no-op; the
// in-memory seeding/inspection methods (SeedSchema, RegisterSchema, etc.) work
// as usual.
func NewRegistry(opts ...Option) *Registry {
	r := &Registry{
		// Simplified state initialization
		subjects:      make(map[string]*subjectData),
		highestID:     make(map[string]int),
		idsByGUID:     make(map[contextGUID]int),
		contexts:      make(map[string]bool),
		compats:       make(map[string]sr.CompatibilityLevel),
		modes:         make(map[string]sr.Mode),
		defaultCompat: sr.CompatBackward,
	}

	for _, opt := range opts {
		opt(r)
	}

	mux := http.NewServeMux()

	// schema routes
	mux.HandleFunc("GET /schemas", r.handleGetSchemas)
	mux.HandleFunc("GET /schemas/ids/{id}", r.handleGetSchemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/schema", r.handleGetRawSchemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/versions", r.handleGetSchemaVersionsByID)
	mux.HandleFunc("GET /schemas/ids/{id}/subjects", r.handleGetSubjectsByID)
	mux.HandleFunc("GET /schemas/guids/{guid}", r.handleGetSchemaByGUID)
	mux.HandleFunc("GET /schemas/guids/{guid}/ids", r.handleGetSchemaIDsByGUID)

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
	mux.HandleFunc("DELETE /config", r.handleDeleteGlobalConfig)
	mux.HandleFunc("GET /config/{subject}", r.handleGetSubjectConfig)
	mux.HandleFunc("PUT /config/{subject}", r.handlePutSubjectConfig)
	mux.HandleFunc("DELETE /config/{subject}", r.handleDeleteSubjectConfig)

	// compatibility routes (against one version, or against all versions)
	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions/{version}", r.handleCheckCompatibility)
	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions", r.handleCheckCompatibility)

	// context routes (standalone endpoints, not subject to prefix stripping)
	mux.HandleFunc("GET /contexts", r.handleGetContexts)
	mux.HandleFunc("DELETE /contexts/{context}", r.handleDeleteContext)

	// mode routes (global and per-subject)
	mux.HandleFunc("GET /mode", r.handleGetMode)
	mux.HandleFunc("PUT /mode", r.handlePutMode)
	mux.HandleFunc("DELETE /mode", r.handleDeleteMode)
	mux.HandleFunc("GET /mode/{subject}", r.handleGetSubjectMode)
	mux.HandleFunc("PUT /mode/{subject}", r.handlePutSubjectMode)
	mux.HandleFunc("DELETE /mode/{subject}", r.handleDeleteSubjectMode)

	var h http.Handler = mux
	h = r.contextMiddleware(h)
	if r.expectedAuth != "" {
		h = r.authMiddleware(h)
	}
	h = r.interceptorMiddleware(h)

	r.handler = h
	return r
}

// Handler returns the registry's HTTP handler. This is the same handler served
// by New's internal httptest.Server, exposed so callers can serve it on their
// own listener.
func (r *Registry) Handler() http.Handler { return r.handler }

// Close shuts down the underlying server, if New started one. It is a no-op
// for registries created via NewHandler.
func (r *Registry) Close() {
	if r.srv != nil {
		r.srv.Close()
	}
}

// URL returns the base URL of the running mock, or "" if the registry was
// created via NewHandler (no server started).
func (r *Registry) URL() string {
	if r.srv != nil {
		return r.srv.URL
	}
	return ""
}

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
// slot is already populated, if the id in the subject's context carries a
// different schema, or if the schema is invalid. As in Confluent's registry,
// one id can carry an equal schema in several subjects.
func (r *Registry) SeedSchema(subject string, version, id int, sch sr.Schema) {
	if err := r.validateSchema(sch); err != nil {
		panic(fmt.Sprintf("invalid schema: %v", err))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	ctx := subjectContext(subject)
	if ss, ok := r.versionByIDLocked(ctx, id, ""); ok && !r.schemasEqual(ss.Schema, sch) {
		panic(fmt.Sprintf("schema id %d already seeded", id))
	}

	// Get or create subject data
	subj := r.getOrCreateSubject(subject)

	if _, dup := subj.versions[version]; dup {
		panic(fmt.Sprintf("subject %q version %d already seeded", subject, version))
	}

	sch, err := r.validateReferences(subject, sch)
	if err != nil {
		panic(fmt.Sprintf("invalid references: %v", err))
	}

	guid := schemaGUID(sch)
	r.idsByGUID[contextGUID{ctx, guid}] = id
	if ctx != "." {
		r.contexts[ctx] = true
	}

	// Store schema version data
	subj.versions[version] = &versionData{
		schema: sr.SubjectSchema{
			Subject: subject,
			Version: version,
			ID:      id,
			GUID:    guid,
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
	if id > r.highestID[ctx] {
		r.highestID[ctx] = id
	}
}

// RegisterSchema emulates POST /subjects/{subject}/versions.
func (r *Registry) RegisterSchema(subject string, schema sr.Schema) (id, version int, err error) {
	return r.register(subject, schema, -1, 0)
}

// Registers schema under subject, as POST /subjects/{subject}/versions does
// with reqID and reqVersion from the id and version fields of its body, which
// are -1 and 0 when the body has neither.
func (r *Registry) register(subject string, schema sr.Schema, reqID, reqVersion int) (id, version int, err error) {
	if err = r.validateSubject(subject); err != nil {
		return 0, 0, err
	}

	// Hold write lock for entire operation to prevent race conditions.
	// The "check-then-act" sequence must be atomic to ensure data integrity.
	r.mu.Lock()
	defer r.mu.Unlock()

	// As in Confluent's registry, the mode is checked before the schema: a
	// request with an ID needs import mode, and any other request needs
	// read-write mode. srfake does not support import mode.
	switch mode := r.modeInScopeLocked(subject); {
	case mode == sr.ModeReadOnly:
		return 0, 0, errModeRefuses(subject, "is in read-only mode")
	case reqID >= 0 && mode != sr.ModeImport:
		return 0, 0, errModeRefuses(subject, "is not in import mode")
	case reqID >= 0:
		return 0, 0, errImportUnsupported()
	case mode != sr.ModeReadWrite:
		return 0, 0, errModeRefuses(subject, "is not in read-write mode")
	}
	if err = r.validateSchema(schema); err != nil {
		return 0, 0, err
	}

	// Validate references under write lock to prevent dangling references
	if schema, err = r.validateReferences(subject, schema); err != nil {
		return 0, 0, err
	}

	// Get subject data, which the registry keeps only once a version is
	// stored. A soft-deleted subject gets the next version, as in Confluent's
	// registry.
	subj := r.subjects[subject]
	if subj == nil {
		subj = &subjectData{versions: make(map[int]*versionData)}
	}

	// existing identical schema under same subject? As in Confluent's
	// registry, a request with a version skips this lookup, and its version
	// must be the next one.
	for v, versionData := range subj.versions {
		if reqVersion == 0 && !versionData.isDeleted && r.schemasEqual(versionData.schema.Schema, schema) {
			return versionData.schema.ID, v, nil
		}
	}
	if reqVersion > 0 && reqVersion != subj.highestVersion+1 {
		return 0, 0, errInvalidSchema("Version is not one more than previous version")
	}

	// reuse id from other subject in the same context?
	ctx := subjectContext(subject)
	guid := schemaGUID(schema)
	if existingID, ok := r.idsByGUID[contextGUID{ctx, guid}]; ok && r.idInUseLocked(ctx, existingID) {
		version = subj.highestVersion + 1

		// Check for circular dependencies before storing the schema
		if err = r.validateCircularReferences(schema, subject, version); err != nil {
			return 0, 0, err
		}

		// Store schema version data
		r.keepSubjectLocked(subject, subj)
		subj.versions[version] = &versionData{
			schema: sr.SubjectSchema{
				Subject: subject,
				Version: version,
				ID:      existingID,
				GUID:    guid,
				Schema:  schema,
			},
			isDeleted: false,
		}
		subj.latestVersion = version
		subj.highestVersion = version
		return existingID, version, nil
	}

	// brand-new schema
	r.highestID[ctx]++
	id = r.highestID[ctx]
	version = subj.highestVersion + 1

	// Check for circular dependencies before storing the schema
	if err = r.validateCircularReferences(schema, subject, version); err != nil {
		return 0, 0, err
	}

	r.idsByGUID[contextGUID{ctx, guid}] = id

	// Store schema version data
	r.keepSubjectLocked(subject, subj)
	subj.versions[version] = &versionData{
		schema: sr.SubjectSchema{
			Subject: subject,
			Version: version,
			ID:      id,
			GUID:    guid,
			Schema:  schema,
		},
		isDeleted: false,
	}
	subj.latestVersion = version
	subj.highestVersion = version
	return id, version, nil
}

// SetCompat sets the compatibility level of subject, as PUT /config/{subject}
// does, without checking the mode. The empty subject sets the global level.
func (r *Registry) SetCompat(subject string, c sr.CompatibilityLevel) error {
	if c.String() == "" {
		return fmt.Errorf("empty compatibility level")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.compats[subject] = c
	return nil
}

// SetMode sets the mode of subject. The empty subject sets the global mode.
func (r *Registry) SetMode(subject string, m sr.Mode) error {
	if m.String() == "" {
		return fmt.Errorf("invalid mode")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.modes[subject] = m
	return nil
}

// GetSchema retrieves a schema by subject and version.
func (r *Registry) GetSchema(subject string, version int) (sr.SubjectSchema, bool) {
	return r.getSchemaBySubjectVersion(subject, version, false)
}

// SubjectExists returns true if subject has a version that is not deleted.
func (r *Registry) SubjectExists(subject string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	return ok && subj.active()
}

// Reset wipes all in-memory state, which restores the initial global
// compatibility level and mode. Useful between tests.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.subjects = make(map[string]*subjectData)
	r.highestID = make(map[string]int)
	r.idsByGUID = make(map[contextGUID]int)
	r.contexts = make(map[string]bool)
	r.compats = make(map[string]sr.CompatibilityLevel)
	r.modes = make(map[string]sr.Mode)
}

// Returns the keys whose setting applies to subject, nearest first, as in
// Confluent's registry: the subject, then its context (the global setting for
// the default context), then the .__GLOBAL context.
func scopeKeys(subject string) []string {
	return []string{subject, qualifiedContext(subjectContext(subject)), ":.__GLOBAL:"}
}

// Returns the compatibility level that applies to subject. The caller must
// hold the lock.
func (r *Registry) compatInScopeLocked(subject string) sr.CompatibilityLevel {
	for _, key := range scopeKeys(subject) {
		if c, ok := r.compats[key]; ok {
			return r.levelOf(c)
		}
	}
	return r.defaultCompat
}

// Returns the level that a set compatibility c answers with: as in
// Confluent's registry, a subject configured without a level answers with the
// default level.
func (r *Registry) levelOf(c sr.CompatibilityLevel) sr.CompatibilityLevel {
	if c == 0 {
		return r.defaultCompat
	}
	return c
}

// Returns the mode that applies to subject. The caller must hold the lock.
func (r *Registry) modeInScopeLocked(subject string) sr.Mode {
	for _, key := range scopeKeys(subject) {
		if m, ok := r.modes[key]; ok {
			return m
		}
	}
	return sr.ModeReadWrite
}

// Returns an error if subject is in read-only mode, which refuses deletes and
// compatibility changes, as in Confluent's registry. The caller must hold the
// lock.
func (r *Registry) errReadOnlyLocked(subject string) error {
	if r.modeInScopeLocked(subject) == sr.ModeReadOnly {
		return errModeRefuses(subject, "is in read-only mode")
	}
	return nil
}

// Keeps subj as subject, whose context becomes known. The caller must hold the
// lock.
func (r *Registry) keepSubjectLocked(subject string, subj *subjectData) {
	r.subjects[subject] = subj
	if ctx := subjectContext(subject); ctx != "." {
		r.contexts[ctx] = true
	}
}

// Returns the known contexts besides the default context, sorted. The caller
// must hold the lock.
func (r *Registry) sortedContextsLocked() []string {
	contexts := make([]string, 0, len(r.contexts))
	for c := range r.contexts {
		contexts = append(contexts, c)
	}
	sort.Strings(contexts)
	return contexts
}

// Removes the settings of subject, as Confluent's registry does when the last
// version of subject is permanently deleted. The caller must hold the lock.
func (r *Registry) removeSubjectLocked(subject string) {
	delete(r.subjects, subject)
	delete(r.compats, subject)
	delete(r.modes, subject)
}

// getSchemaBySubjectVersionLocked is like getSchemaBySubjectVersion but assumes
// caller holds lock. If includeDeleted is true, soft-deleted subjects/versions
// are returned (matching the ?deleted=true query parameter).
func (r *Registry) getSchemaBySubjectVersionLocked(subject string, version int, includeDeleted bool) (sr.SubjectSchema, bool) {
	subj, ok := r.subjects[subject]
	if !ok {
		return sr.SubjectSchema{}, false
	}

	versionData, ok := subj.versions[version]
	if !ok || (versionData.isDeleted && !includeDeleted) {
		return sr.SubjectSchema{}, false
	}

	return versionData.schema, true
}

// Reports whether a version in ctx, soft-deleted or not, carries id. A schema
// ID stops resolving once every version that carries it is hard-deleted. The
// caller must hold the lock.
func (r *Registry) idInUseLocked(ctx string, id int) bool {
	_, ok := r.versionByIDLocked(ctx, id, "")
	return ok
}

// Returns the latest version in ctx that carries id, of subject if subject is
// not empty, else of the first such subject in sorted order. Soft-deleted
// versions count. The caller must hold the lock.
func (r *Registry) versionByIDLocked(ctx string, id int, subject string) (sr.SubjectSchema, bool) {
	var found sr.SubjectSchema
	ok := false
	for s, subj := range r.subjects {
		if subjectContext(s) != ctx || subject != "" && s != subject {
			continue
		}
		if ok && s > found.Subject {
			continue // a lower subject carries id
		}
		for _, vd := range subj.versions {
			if vd.schema.ID == id && (!ok || s != found.Subject || vd.schema.Version > found.Version) {
				found, ok = vd.schema, true
			}
		}
	}
	return found, ok
}

// Returns the version that a lookup of schema id resolves to, as Confluent's
// registry resolves it. Without a subject, the ID is in the default context.
// A subject with a context but no name means any subject of the context. A
// subject of the default context that does not carry the ID is tried in every
// other context, and then any subject of the default context with the ID
// answers. The caller must hold the lock.
func (r *Registry) schemaByIDLocked(id int, subject string, hasSubject bool) (sr.SubjectSchema, bool) {
	if !hasSubject {
		return r.versionByIDLocked(".", id, "")
	}
	ctx, name := splitSubject(subject)
	if name == "" {
		return r.versionByIDLocked(ctx, id, "")
	}
	if ss, ok := r.versionByIDLocked(ctx, id, subject); ok || ctx != "." {
		return ss, ok
	}
	for _, c := range r.sortedContextsLocked() {
		if ss, ok := r.versionByIDLocked(c, id, qualifiedContext(c)+name); ok {
			return ss, true
		}
	}
	return r.versionByIDLocked(".", id, "")
}

// Returns the subjects with a version that references version targetVersion
// of targetSubject, each mapped to whether one of those versions is not
// soft-deleted. Assumes the caller already holds a lock.
func (r *Registry) referrersLocked(targetSubject string, targetVersion int) map[string]bool {
	referrers := make(map[string]bool)
	for subject, subj := range r.subjects {
		for _, versionData := range subj.versions {
			for _, ref := range versionData.schema.References {
				if refSubject(subject, ref.Subject) == targetSubject && ref.Version == targetVersion {
					referrers[subject] = referrers[subject] || !versionData.isDeleted
				}
			}
		}
	}
	return referrers
}

// Reports whether referrers keep a version from being deleted. As in
// Confluent's registry, a soft delete counts only referrers that are not
// soft-deleted, and a permanent delete counts every referrer.
func blocksDelete(referrers map[string]bool, permanent bool) bool {
	for _, active := range referrers {
		if active || permanent {
			return true
		}
	}
	return false
}

// deleteSubject performs a soft or permanent delete of a subject, as
// Confluent's registry does: a soft delete marks every version that is not
// soft-deleted as soft-deleted, and a permanent delete requires every version
// to be soft-deleted first. Returns the deleted versions. This method assumes
// the caller already holds the necessary lock.
func (r *Registry) deleteSubject(subject string, permanent bool) ([]int, error) {
	subj, ok := r.subjects[subject]
	if !ok || len(subj.versions) == 0 {
		return nil, errSubjectNotFound(subject)
	}
	if !permanent && !subj.active() {
		return nil, errSubjectSoftDeleted(subject)
	}
	if err := r.errReadOnlyLocked(subject); err != nil {
		return nil, err
	}

	var versions []int
	for v, vd := range subj.versions {
		if permanent || !vd.isDeleted {
			versions = append(versions, v)
		}
	}
	sort.Ints(versions)

	for _, version := range versions {
		// References from the subject itself do not count, as in
		// Confluent's registry.
		referrers := r.referrersLocked(subject, version)
		delete(referrers, subject)
		if blocksDelete(referrers, permanent) {
			return nil, errSchemaIsReferenced(subject, version)
		}
		if permanent && !subj.versions[version].isDeleted {
			return nil, errSubjectNotSoftDeleted(subject)
		}
	}

	if permanent {
		// Hard delete - remove entire subject
		r.removeSubjectLocked(subject)
		return versions, nil
	}
	for _, v := range versions {
		vd := subj.versions[v]
		vd.isDeleted = true
		r.idsByGUID[contextGUID{subjectContext(subject), vd.schema.GUID}] = vd.schema.ID
	}
	subj.latestVersion = 0
	return versions, nil
}

// deleteVersion performs a soft or permanent delete of a specific version, as
// Confluent's registry does: a permanent delete requires a soft delete first,
// and a soft-deleted version cannot be soft-deleted again. Version -1 is the
// latest version that is not soft-deleted. Returns the deleted version. This
// method assumes the caller already holds the necessary lock.
func (r *Registry) deleteVersion(subject string, version int, permanent bool) (int, error) {
	subj, ok := r.subjects[subject]
	if !ok || len(subj.versions) == 0 {
		return 0, errSubjectNotFound(subject)
	}
	if version == -1 && subj.active() {
		version = subj.latestVersion
	}

	versionData, ok := subj.versions[version]
	if !ok {
		return 0, errVersionNotFound(version)
	}
	if !permanent && versionData.isDeleted {
		return 0, errVersionSoftDeleted(subject, version)
	}
	if err := r.errReadOnlyLocked(subject); err != nil {
		return 0, err
	}

	// Check if schema is still referenced
	if blocksDelete(r.referrersLocked(subject, version), permanent) {
		return 0, errSchemaIsReferenced(subject, version)
	}
	if permanent && !versionData.isDeleted {
		return 0, errVersionNotSoftDeleted(subject, version)
	}

	if permanent {
		// Hard delete - remove version data, and the subject once it has
		// no versions
		delete(subj.versions, version)
		subj.highestVersion = subj.highestExisting()
		if len(subj.versions) == 0 {
			r.removeSubjectLocked(subject)
		}
	} else {
		// Soft delete - mark version as deleted
		versionData.isDeleted = true
		r.idsByGUID[contextGUID{subjectContext(subject), versionData.schema.GUID}] = versionData.schema.ID
	}

	// Recalculate latest version if we deleted the current latest
	if subj.latestVersion == version {
		subj.recalculateLatestVersion()
	}

	return version, nil
}

// Returns the version that s names, as Confluent's registry parses it: a
// positive number, or -1 or "latest" in any case for the latest version.
func parseVersion(s string) (int, error) {
	t := strings.TrimSpace(s)
	if strings.EqualFold(t, "latest") {
		return -1, nil
	}
	v, err := strconv.Atoi(t)
	if err != nil || v <= 0 && v != -1 {
		return 0, errInvalidVersion(s)
	}
	return v, nil
}

// Returns what find returns for the first of subject and, for a subject of the
// default context, the same name in every other context, for which find
// succeeds. This is how Confluent's registry looks a subject up. The caller
// must hold the lock.
func (r *Registry) findUsingContextsLocked(subject string, find func(subject string) (sr.SubjectSchema, bool)) (sr.SubjectSchema, bool) {
	if ss, ok := find(subject); ok {
		return ss, true
	}
	if ctx, name := splitSubject(subject); ctx == "." {
		for _, c := range r.sortedContextsLocked() {
			if ss, ok := find(qualifiedContext(c) + name); ok {
				return ss, true
			}
		}
	}
	return sr.SubjectSchema{}, false
}

// Returns version of subject, with -1 for the latest version, looking the
// subject up as findUsingContextsLocked does. As in Confluent's registry,
// latest is the highest version that is not soft-deleted, or with
// includeDeleted the highest version. The caller must hold the lock.
func (r *Registry) lookupVersionLocked(subject string, version int, includeDeleted bool) (sr.SubjectSchema, bool) {
	return r.findUsingContextsLocked(subject, func(s string) (sr.SubjectSchema, bool) {
		subj, ok := r.subjects[s]
		if !ok {
			return sr.SubjectSchema{}, false
		}
		v := version
		if v == -1 {
			v = subj.latestVersion
			if includeDeleted {
				v = subj.highestVersion
			}
		}
		vd, ok := subj.versions[v]
		if !ok || vd.isDeleted && !includeDeleted {
			return sr.SubjectSchema{}, false
		}
		return vd.schema, true
	})
}

// Returns the error for a version of subject that a lookup did not find. As in
// Confluent's registry, the subject is not found if it has no version that the
// lookup could return, else the version is not found. The caller must hold
// the lock.
func (r *Registry) errMissingVersionLocked(subject string, version int, includeDeleted bool) error {
	if subj, ok := r.subjects[subject]; ok && (subj.active() || includeDeleted && len(subj.versions) > 0) {
		return errVersionNotFound(version)
	}
	return errSubjectNotFound(subject)
}

func (r *Registry) getSchemaBySubjectVersion(subject string, version int, includeDeleted bool) (sr.SubjectSchema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getSchemaBySubjectVersionLocked(subject, version, includeDeleted)
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

// Returns s with its references checked. As in Confluent's registry, a
// reference to version -1 is to the latest version that is not soft-deleted,
// and s comes back with that version in place of -1. The caller must hold the
// lock.
func (r *Registry) validateReferences(subject string, s sr.Schema) (sr.Schema, error) {
	refs := append([]sr.SchemaReference(nil), s.References...)
	for i, ref := range refs {
		if ref.Name == "" {
			return s, errInvalidSchema("reference missing name")
		}
		if ref.Subject == "" {
			return s, errInvalidSchema(fmt.Sprintf("reference %q missing subject", ref.Name))
		}
		if ref.Version <= 0 && ref.Version != -1 {
			return s, errInvalidSchema(fmt.Sprintf("reference %q has invalid version %d", ref.Name, ref.Version))
		}

		// Check if the referenced schema exists and is not soft-deleted
		subj, ok := r.subjects[refSubject(subject, ref.Subject)]
		if !ok {
			return s, errInvalidReference(ref)
		}
		if ref.Version == -1 {
			refs[i].Version = subj.latestVersion
		}

		versionData, ok := subj.versions[refs[i].Version]
		if !ok || versionData.isDeleted {
			return s, errInvalidReference(ref)
		}
	}
	s.References = refs
	return s, nil
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
		refName := refSubject(subject, ref.Subject)
		refKey := fmt.Sprintf("%s:%d", refName, ref.Version)

		// If not visited, recursively check this reference
		if !visited[refKey] {
			// Get the referenced schema
			if refSubj, ok := r.subjects[refName]; ok {
				if refVersionData, ok := refSubj.versions[ref.Version]; ok {
					if err := r.detectCycle(refVersionData.schema.Schema, refName, ref.Version, visited, subjectStack); err != nil {
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

// subjectContext extracts the context from a ":.name:subject" string, defaulting to ".".
func subjectContext(subject string) string {
	if strings.HasPrefix(subject, ":.") {
		if idx := strings.Index(subject[2:], ":"); idx >= 0 {
			return subject[1 : idx+2] // e.g. ":.myctx:topic" -> ".myctx"
		}
	}
	return "."
}

// Returns the context and the name of subject, as Confluent's registry splits
// it: ":.ctx:name", or ":.ctx" for a context without a name. Any other subject
// is a name in the default context.
func splitSubject(subject string) (ctx, name string) {
	if !strings.HasPrefix(subject, ":.") {
		return ".", subject
	}
	if i := strings.Index(subject[2:], ":"); i >= 0 {
		return subject[1 : i+2], subject[i+3:]
	}
	return subject[1:], ""
}

// Returns subject normalized as Confluent's registry normalizes it: the
// default context is dropped, so ":.:foo" is foo, and a context without a
// name gets its closing colon, so ":.ctx" is ":.ctx:".
func normalizeSubject(subject string) string {
	if !strings.HasPrefix(subject, ":.") {
		return subject
	}
	ctx, name := splitSubject(subject)
	return qualifiedContext(ctx) + name
}

// Returns the subject that a reference named ref in a schema of subject
// resolves to. As in Confluent's registry, a ref without a context is in the
// context of subject.
func refSubject(subject, ref string) string {
	if ctx := subjectContext(subject); ctx != "." && subjectContext(ref) == "." {
		return qualifiedContext(ctx) + ref
	}
	return ref
}

// schemaGUID deterministically derives a schema's GUID from its canonical
// content, mirroring a real registry where identical schema content always maps
// to the same GUID regardless of the subject it is registered under.
func schemaGUID(s sr.Schema) string {
	t := s.Type
	if t == 0 {
		t = sr.TypeAvro
	}
	norm, err := normalizeSchema(s.Schema, t)
	if err != nil {
		norm = s.Schema
	}
	refs := append([]sr.SchemaReference(nil), s.References...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })

	h := fnv.New128a()
	fmt.Fprintf(h, "%d\x00%s", t, norm)
	for _, ref := range refs {
		fmt.Fprintf(h, "\x00%s\x00%s\x00%d", ref.Name, ref.Subject, ref.Version)
	}
	var b [16]byte
	copy(b[:], h.Sum(nil))
	b[6] = (b[6] & 0x0f) | 0x80 // version 8 (custom, non-namespace)
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// subjectSchemaByGUIDLocked returns a representative active SubjectSchema for the
// given GUID. A GUID identifies a schema globally, so all contexts are searched;
// a default-context match is preferred, otherwise the lowest-sorted subject and
// version wins. The caller must hold the lock.
func (r *Registry) subjectSchemaByGUIDLocked(guid string) (sr.SubjectSchema, bool) {
	subjects := make([]string, 0, len(r.subjects))
	for s := range r.subjects {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)

	var fallback sr.SubjectSchema
	haveFallback := false
	for _, s := range subjects {
		subj := r.subjects[s]
		if !subj.active() {
			continue
		}
		versions := make([]int, 0, len(subj.versions))
		for v := range subj.versions {
			versions = append(versions, v)
		}
		sort.Ints(versions)
		for _, v := range versions {
			vd := subj.versions[v]
			if vd.isDeleted || vd.schema.GUID != guid {
				continue
			}
			if subjectContext(s) == "." {
				return vd.schema, true
			}
			if !haveFallback {
				fallback, haveFallback = vd.schema, true
			}
		}
	}
	return fallback, haveFallback
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
