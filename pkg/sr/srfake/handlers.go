package srfake

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/sr"
)

type ctxKey struct{}

func requestContext(req *http.Request) string {
	v, _ := req.Context().Value(ctxKey{}).(string)
	return v
}

/* -------------------------------------------------------------------------
   Middleware
   ------------------------------------------------------------------------- */

func (r *Registry) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") == "" {
			writeError(w, http.StatusUnauthorized, sr.ErrOperationTimeout.Code, "Missing Authorization header")
			return
		}
		if req.Header.Get("Authorization") != r.expectedAuth {
			writeError(w, http.StatusForbidden, sr.ErrOperationTimeout.Code, "User not authorized")
			return
		}
		next.ServeHTTP(w, req)
	})
}

func (r *Registry) interceptorMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.ictMu.RLock()
		handlers := r.interceptors
		r.ictMu.RUnlock()

		for _, h := range handlers {
			if h(w, req) {
				return
			}
		}
		next.ServeHTTP(w, req)
	})
}

func (*Registry) contextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := req.URL.Path
		if !strings.HasPrefix(p, "/contexts/") {
			next.ServeHTTP(w, req)
			return
		}

		rest := p[len("/contexts/"):]
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			next.ServeHTTP(w, req)
			return
		}

		name := rest[:slash]
		remaining := rest[slash:]

		ctx := context.WithValue(req.Context(), ctxKey{}, name)
		req = req.WithContext(ctx)
		req.URL.Path = remaining
		if req.URL.RawPath != "" {
			raw := req.URL.RawPath
			if strings.HasPrefix(raw, "/contexts/") {
				rawRest := raw[len("/contexts/"):]
				if i := strings.IndexByte(rawRest, '/'); i >= 0 {
					req.URL.RawPath = rawRest[i:]
				}
			}
		}

		next.ServeHTTP(w, req)
	})
}

// Caller must hold r.mu (at least RLock).
func (r *Registry) schemaInContext(id int, ctx string) bool {
	for subject, subj := range r.subjects {
		if subj.isDeleted || subjectContext(subject) != ctx {
			continue
		}
		for _, vd := range subj.versions {
			if !vd.isDeleted && vd.schema.ID == id {
				return true
			}
		}
	}
	return false
}

/* -------------------------------------------------------------------------
   Handlers – Schemas
   ------------------------------------------------------------------------- */

// handleGetSchemaByID emulates GET /schemas/ids/{id} to retrieve a full schema
// definition by its global ID.
func (r *Registry) handleGetSchemaByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	sch, ok := r.schemasByID[id]
	if !ok {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	if ctx := requestContext(req); ctx != "" {
		if !r.schemaInContext(id, ctx) {
			r.handleAPIError(w, errSchemaNotFound())
			return
		}
	}
	if subject := req.URL.Query().Get("subject"); subject != "" && !r.schemaIDInSubjectLocked(id, subject) {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	respondJSON(w, http.StatusOK, sch)
}

// handleGetRawSchemaByID emulates GET /schemas/ids/{id}/schema to retrieve
// only the schema string by its global ID.
func (r *Registry) handleGetRawSchemaByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	sch, ok := r.schemasByID[id]
	if !ok {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	if ctx := requestContext(req); ctx != "" {
		if !r.schemaInContext(id, ctx) {
			r.handleAPIError(w, errSchemaNotFound())
			return
		}
	}
	if subject := req.URL.Query().Get("subject"); subject != "" && !r.schemaIDInSubjectLocked(id, subject) {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	respondJSON(w, http.StatusOK, sch.Schema)
}

// handleGetSchemaVersionsByID emulates GET /schemas/ids/{id}/versions to retrieve
// all subject-version pairs that use the given schema ID.
func (r *Registry) handleGetSchemaVersionsByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}

	ctx := requestContext(req)
	del := parseDeleted(req)
	subjectParam := req.URL.Query().Get("subject")

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if schema exists
	if _, ok := r.schemasByID[id]; !ok {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}

	// Find all subject-version pairs that use this schema ID
	var usages []map[string]any
	for subject, subj := range r.subjects {
		if !del.keep(subj.isDeleted) {
			continue
		}
		if ctx != "" && subjectContext(subject) != ctx {
			continue
		}
		if subjectParam != "" && subject != subjectParam {
			continue
		}
		for version, versionData := range subj.versions {
			if versionData.schema.ID == id && del.keep(versionData.isDeleted) {
				usages = append(usages, map[string]any{
					"subject": subject,
					"version": version,
				})
			}
		}
	}

	if ctx != "" && len(usages) == 0 {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}

	respondJSON(w, http.StatusOK, usages)
}

// handleGetSubjectsByID emulates GET /schemas/ids/{id}/subjects.
func (r *Registry) handleGetSubjectsByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}

	ctx := requestContext(req)
	del := parseDeleted(req)
	subjectParam := req.URL.Query().Get("subject")

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.schemasByID[id]; !ok {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}

	subjectSet := make(map[string]bool)
	for subject, subj := range r.subjects {
		if !del.keep(subj.isDeleted) {
			continue
		}
		if ctx != "" && subjectContext(subject) != ctx {
			continue
		}
		if subjectParam != "" && subject != subjectParam {
			continue
		}
		for _, vd := range subj.versions {
			if del.keep(vd.isDeleted) && vd.schema.ID == id {
				subjectSet[subject] = true
				break
			}
		}
	}

	if ctx != "" && len(subjectSet) == 0 {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}

	subjects := make([]string, 0, len(subjectSet))
	for s := range subjectSet {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)

	respondJSON(w, http.StatusOK, subjects)
}

// handleGetSchemaByGUID emulates GET /schemas/guids/{guid} to retrieve a schema
// by its globally unique GUID. The GUID identifies a schema across all contexts,
// so the lookup is not scoped to the request context.
func (r *Registry) handleGetSchemaByGUID(w http.ResponseWriter, req *http.Request) {
	guid := req.PathValue("guid")

	r.mu.RLock()
	defer r.mu.RUnlock()

	ss, ok := r.subjectSchemaByGUIDLocked(guid)
	if !ok {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	respondJSON(w, http.StatusOK, ss)
}

// handleGetSchemaIDsByGUID emulates GET /schemas/guids/{guid}/ids to retrieve the
// (context, schema ID) pairs that the GUID resolves to. A GUID is global, so
// every context is searched; each context that contains the schema contributes
// its own ID.
func (r *Registry) handleGetSchemaIDsByGUID(w http.ResponseWriter, req *http.Request) {
	guid := req.PathValue("guid")

	r.mu.RLock()
	defer r.mu.RUnlock()

	byContext := make(map[string]int)
	for subject, subj := range r.subjects {
		if subj.isDeleted {
			continue
		}
		for _, vd := range subj.versions {
			if !vd.isDeleted && vd.schema.GUID == guid {
				byContext[subjectContext(subject)] = vd.schema.ID
				break
			}
		}
	}
	if len(byContext) == 0 {
		r.handleAPIError(w, errSchemaNotFound())
		return
	}
	ids := make([]sr.ContextID, 0, len(byContext))
	for c, id := range byContext {
		ids = append(ids, sr.ContextID{Context: c, ID: id})
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Context < ids[j].Context })
	respondJSON(w, http.StatusOK, ids)
}

/* -------------------------------------------------------------------------
   Handlers – Subjects
   ------------------------------------------------------------------------- */

// handleGetSubjects emulates GET /subjects to return a list of all registered
// subjects.
// deletedFilter captures the ?deleted and ?deletedOnly query parameters.
// deletedOnly returns only soft-deleted items, deleted returns active and
// soft-deleted, and neither returns only active items.
type deletedFilter struct {
	includeDeleted bool
	onlyDeleted    bool
}

func parseDeleted(req *http.Request) deletedFilter {
	q := req.URL.Query()
	return deletedFilter{
		includeDeleted: q.Get("deleted") == "true",
		onlyDeleted:    q.Get("deletedOnly") == "true",
	}
}

// keep reports whether an item with the given soft-delete state is included.
func (f deletedFilter) keep(isDeleted bool) bool {
	if f.onlyDeleted {
		return isDeleted
	}
	if f.includeDeleted {
		return true
	}
	return !isDeleted
}

// any reports whether soft-deleted items are requested at all, used by
// single-item lookups to decide whether a soft-deleted item may be returned.
func (f deletedFilter) any() bool { return f.includeDeleted || f.onlyDeleted }

// handleGetSchemas implements GET /schemas, returning every schema across all
// subjects and versions. It supports the subjectPrefix, deleted, deletedOnly,
// and latestOnly query parameters.
func (r *Registry) handleGetSchemas(w http.ResponseWriter, req *http.Request) {
	includeDeleted := req.URL.Query().Get("deleted") == "true"
	subjPrefix := req.URL.Query().Get("subjectPrefix")
	latestOnly := req.URL.Query().Get("latestOnly") == "true"
	ctx := requestContext(req)

	r.mu.RLock()
	defer r.mu.RUnlock()

	subjects := make([]string, 0, len(r.subjects))
	for s := range r.subjects {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)

	out := make([]sr.SubjectSchema, 0)
	for _, s := range subjects {
		subj := r.subjects[s]
		if !includeDeleted && subj.isDeleted {
			continue
		}
		if ctx != "" && subjectContext(s) != ctx {
			continue
		}
		if subjPrefix != "" && !strings.HasPrefix(s, subjPrefix) {
			continue
		}

		versions := make([]int, 0, len(subj.versions))
		for v := range subj.versions {
			versions = append(versions, v)
		}
		sort.Ints(versions)
		for _, v := range versions {
			vd := subj.versions[v]
			if !includeDeleted && vd.isDeleted {
				continue
			}
			if latestOnly && v != subj.latestVersion {
				continue
			}
			out = append(out, vd.schema)
		}
	}

	respondJSON(w, http.StatusOK, out)
}

func (r *Registry) handleGetSubjects(w http.ResponseWriter, req *http.Request) {
	del := parseDeleted(req)
	subjPrefix := req.URL.Query().Get("subjectPrefix")
	ctx := requestContext(req)

	r.mu.RLock()
	subjects := make([]string, 0, len(r.subjects))
	for s, subj := range r.subjects {
		if !del.keep(subj.isDeleted) {
			continue
		}
		if ctx != "" && subjectContext(s) != ctx {
			continue
		}
		if subjPrefix != "" && !strings.HasPrefix(s, subjPrefix) {
			continue
		}
		subjects = append(subjects, s)
	}
	r.mu.RUnlock()

	if subjects == nil {
		subjects = []string{}
	}
	sort.Strings(subjects)
	respondJSON(w, http.StatusOK, subjects)
}

// handleGetSubjectVersions emulates GET /subjects/{subject}/versions to return
// a list of all versions for a given subject.
func (r *Registry) handleGetSubjectVersions(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	del := parseDeleted(req)

	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	if !ok || (subj.isDeleted && !del.any()) {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}

	var versions []int
	for v, versionData := range subj.versions {
		if !del.keep(versionData.isDeleted) {
			continue
		}
		versions = append(versions, v)
	}
	sort.Ints(versions)
	respondJSON(w, http.StatusOK, versions)
}

// handlePostSubjectVersion emulates POST /subjects/{subject}/versions to
// register a new schema under a subject.
func (r *Registry) handlePostSubjectVersion(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<20)
	subject := req.PathValue("subject")

	var body sr.Schema
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidSchemaWithCause(err, err.Error()))
		return
	}

	id, _, err := r.RegisterSchema(subject, body)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]int{"id": id})
}

// handleGetSubjectSchemaByVersion emulates GET /subjects/{subject}/versions/{version}
// to return a specific schema version for a subject.
func (r *Registry) handleGetSubjectSchemaByVersion(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	versionStr := req.PathValue("version")
	del := parseDeleted(req)

	version, err := r.resolveVersion(subject, versionStr, del.any())
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	sch, ok := r.getSchemaBySubjectVersion(subject, version, del.any())
	if !ok {
		r.handleAPIError(w, errVersionNotFound(subject, version))
		return
	}
	respondJSON(w, http.StatusOK, sch)
}

// handleGetSubjectRawSchemaByVersion emulates GET /subjects/{subject}/versions/{version}/schema
// to return the raw schema string for a specific version.
func (r *Registry) handleGetSubjectRawSchemaByVersion(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	versionStr := req.PathValue("version")
	del := parseDeleted(req)

	version, err := r.resolveVersion(subject, versionStr, del.any())
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	sch, ok := r.getSchemaBySubjectVersion(subject, version, del.any())
	if !ok {
		r.handleAPIError(w, errVersionNotFound(subject, version))
		return
	}
	respondJSON(w, http.StatusOK, sch.Schema)
}

// handleGetReferencedBy emulates GET /subjects/{subject}/versions/{version}/referencedby
// to return a list of schema IDs that reference the specified schema.
func (r *Registry) handleGetReferencedBy(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	versionStr := req.PathValue("version")
	del := parseDeleted(req)

	version, err := r.resolveVersion(subject, versionStr, del.any())
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if the target schema exists
	_, exists := r.getSchemaBySubjectVersionLocked(subject, version, del.any())
	if !exists {
		r.handleAPIError(w, errVersionNotFound(subject, version))
		return
	}

	// Find all schemas that reference this target schema
	referencingIDs := make([]int, 0) // Initialize as empty slice, not nil
	for _, subj := range r.subjects {
		if !del.keep(subj.isDeleted) {
			continue
		}

		for _, versionData := range subj.versions {
			if !del.keep(versionData.isDeleted) {
				continue
			}

			// Check if this schema references our target
			for _, ref := range versionData.schema.References {
				if ref.Subject == subject && ref.Version == version {
					referencingIDs = append(referencingIDs, versionData.schema.ID)
					break // Only add each schema ID once
				}
			}
		}
	}

	sort.Ints(referencingIDs)
	respondJSON(w, http.StatusOK, referencingIDs)
}

// handleDeleteSubject emulates DELETE /subjects/{subject} and performs a soft
// or permanent delete of a subject.
func (r *Registry) handleDeleteSubject(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	permanent := req.URL.Query().Get("permanent") == "true"

	r.mu.Lock()
	defer r.mu.Unlock()

	versions, err := r.deleteSubject(subject, permanent)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	respondJSON(w, http.StatusOK, versions)
}

// handleDeleteVersion emulates DELETE /subjects/{subject}/versions/{version}
// and performs a soft or permanent delete on a version.
func (r *Registry) handleDeleteVersion(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	verStr := req.PathValue("version")
	permanent := req.URL.Query().Get("permanent") == "true"

	version, err := strconv.Atoi(verStr)
	if err != nil {
		r.handleAPIError(w, errInvalidVersion("invalid version"))
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.deleteVersion(subject, version, permanent); err != nil {
		r.handleAPIError(w, err)
		return
	}

	respondJSON(w, http.StatusOK, version)
}

// handleCheckSchema emulates POST /subjects/{subject} to check if a schema is
// already registered under a subject.
func (r *Registry) handleCheckSchema(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<20)
	subject := req.PathValue("subject")

	if err := r.validateSubject(subject); err != nil {
		r.handleAPIError(w, err)
		return
	}

	var body sr.Schema
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidSchemaWithCause(err, err.Error()))
		return
	}

	del := parseDeleted(req)

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if subject exists and is not soft-deleted
	subj, ok := r.subjects[subject]
	if !ok || (subj.isDeleted && !del.any()) {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}

	for v, versionData := range subj.versions {
		if !del.keep(versionData.isDeleted) || !r.schemasEqual(versionData.schema.Schema, body) {
			continue
		}
		resp := map[string]any{
			"subject": subject,
			"version": v,
			"id":      versionData.schema.ID,
			"guid":    versionData.schema.GUID,
			"schema":  versionData.schema.Schema.Schema,
		}
		if versionData.schema.Type != sr.TypeAvro {
			resp["schemaType"] = versionData.schema.Type.String()
		}
		if len(versionData.schema.References) > 0 {
			resp["references"] = versionData.schema.References
		}
		respondJSON(w, http.StatusOK, resp)
		return
	}
	r.handleAPIError(w, errSchemaNotFound())
}

// handleCheckCompatibility emulates POST /compatibility/subjects/{subject}/versions/{version}
// to check schema compatibility. Note: This is a stub that always returns true.
func (r *Registry) handleCheckCompatibility(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<20)
	var schema sr.Schema
	if err := decodeJSONRequest(req.Body, &schema); err != nil {
		r.handleAPIError(w, errInvalidSchemaWithCause(err, err.Error()))
		return
	}
	respondJSON(w, http.StatusOK, map[string]bool{"is_compatible": true})
}

/* -------------------------------------------------------------------------
   Handlers – Config
   ------------------------------------------------------------------------- */

// handleGetGlobalConfig emulates GET /config to retrieve the global
// compatibility level.
func (r *Registry) handleGetGlobalConfig(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.globalCompat})
}

// handlePutGlobalConfig emulates PUT /config to update the global
// compatibility level.
func (r *Registry) handlePutGlobalConfig(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	var body struct {
		Compatibility sr.CompatibilityLevel `json:"compatibility"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidCompatLevel(err.Error()))
		return
	}
	r.mu.Lock()
	r.globalCompat = body.Compatibility
	r.mu.Unlock()
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibility": body.Compatibility})
}

// handleGetSubjectConfig emulates GET /config/{subject} to retrieve a
// subject's compatibility level.
func (r *Registry) handleGetSubjectConfig(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	useDefault := req.URL.Query().Get("defaultToGlobal") != "false"

	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	switch {
	case ok && subj.compatLevel != nil:
		respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": *subj.compatLevel})
	case useDefault:
		respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.globalCompat})
	default:
		r.handleAPIError(w, errSubjectNotFound(subject))
	}
}

// handlePutSubjectConfig emulates PUT /config/{subject} to update a subject's
// compatibility level.
func (r *Registry) handlePutSubjectConfig(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	subject := req.PathValue("subject")

	var body struct {
		Compatibility sr.CompatibilityLevel `json:"compatibility"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidCompatLevel(err.Error()))
		return
	}
	if err := r.SetCompat(subject, body.Compatibility); err != nil {
		r.handleAPIError(w, errInvalidCompatLevel(err.Error()))
		return
	}
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibility": body.Compatibility})
}

// handleDeleteSubjectConfig emulates DELETE /config/{subject} to remove a
// subject-specific compatibility level, reverting it to the global default.
func (r *Registry) handleDeleteSubjectConfig(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	r.mu.Lock()
	defer r.mu.Unlock()

	if subj, ok := r.subjects[subject]; ok {
		subj.compatLevel = nil
	}
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.globalCompat})
}

/* -------------------------------------------------------------------------
   Handler – Mode
   ------------------------------------------------------------------------- */

// handleGetMode emulates GET /mode and returns the global mode.
func (r *Registry) handleGetMode(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": r.globalMode})
}

// handlePutMode emulates PUT /mode and sets the global mode. The ?force query
// parameter is accepted but not enforced: the mock does not restrict setting
// IMPORT mode on a non-empty registry.
func (r *Registry) handlePutMode(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	var body struct {
		Mode sr.Mode `json:"mode"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidMode(err.Error()))
		return
	}
	r.mu.Lock()
	r.globalMode = body.Mode
	r.mu.Unlock()
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": body.Mode})
}

// handleGetSubjectMode emulates GET /mode/{subject}. If the subject has no mode
// override it falls back to the global mode unless defaultToGlobal=false.
func (r *Registry) handleGetSubjectMode(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	useDefault := req.URL.Query().Get("defaultToGlobal") != "false"

	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	switch {
	case ok && subj.mode != nil:
		respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": *subj.mode})
	case useDefault:
		respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": r.globalMode})
	default:
		r.handleAPIError(w, errSubjectNotFound(subject))
	}
}

// handlePutSubjectMode emulates PUT /mode/{subject} and sets a per-subject mode.
// As with the global setter, ?force is accepted but not enforced.
func (r *Registry) handlePutSubjectMode(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	subject := req.PathValue("subject")

	var body struct {
		Mode sr.Mode `json:"mode"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidMode(err.Error()))
		return
	}
	if err := r.SetMode(subject, body.Mode); err != nil {
		r.handleAPIError(w, errInvalidMode(err.Error()))
		return
	}
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": body.Mode})
}

// handleDeleteSubjectMode emulates DELETE /mode/{subject}, removing a
// per-subject mode override and returning the mode that was removed.
func (r *Registry) handleDeleteSubjectMode(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")

	r.mu.Lock()
	defer r.mu.Unlock()

	subj, ok := r.subjects[subject]
	if !ok || subj.mode == nil {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}
	deleted := *subj.mode
	subj.mode = nil
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": deleted})
}

/* -------------------------------------------------------------------------
   Handlers – Contexts
   ------------------------------------------------------------------------- */

// handleGetContexts emulates GET /contexts.
func (r *Registry) handleGetContexts(w http.ResponseWriter, req *http.Request) {
	contextPrefix := req.URL.Query().Get("contextPrefix")
	offsetStr := req.URL.Query().Get("offset")
	limitStr := req.URL.Query().Get("limit")

	r.mu.RLock()
	ctxSet := map[string]bool{".": true}
	for subject, subj := range r.subjects {
		if subj.isDeleted {
			continue
		}
		hasActive := false
		for _, vd := range subj.versions {
			if !vd.isDeleted {
				hasActive = true
				break
			}
		}
		if !hasActive {
			continue
		}
		ctxSet[subjectContext(subject)] = true
	}
	r.mu.RUnlock()

	contexts := make([]string, 0, len(ctxSet))
	for c := range ctxSet {
		if contextPrefix == "" || strings.HasPrefix(c, contextPrefix) {
			contexts = append(contexts, c)
		}
	}
	sort.Strings(contexts)

	offset := 0
	if offsetStr != "" {
		if v, err := strconv.Atoi(offsetStr); err == nil && v > 0 {
			offset = v
		}
	}
	if offset > len(contexts) {
		offset = len(contexts)
	}
	contexts = contexts[offset:]

	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v >= 0 && v < len(contexts) {
			contexts = contexts[:v]
		}
	}

	respondJSON(w, http.StatusOK, contexts)
}

// handleDeleteContext emulates DELETE /contexts/{context}.
func (r *Registry) handleDeleteContext(w http.ResponseWriter, req *http.Request) {
	ctx := req.PathValue("context")

	r.mu.Lock()
	defer r.mu.Unlock()

	for subject, subj := range r.subjects {
		if subj.isDeleted {
			continue
		}
		if subjectContext(subject) == ctx {
			r.handleAPIError(w, errContextNotEmpty(ctx))
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

/* -------------------------------------------------------------------------
   Helpers
   ------------------------------------------------------------------------- */

// handleAPIError is a centralized error handler that takes an error and writes
// the correct HTTP response. It checks if the error is a registryError and uses
// its properties, otherwise returns a generic 500 error.
func (*Registry) handleAPIError(w http.ResponseWriter, err error) {
	var regErr *registryError
	if errors.As(err, &regErr) {
		// This is a known registry error, use its properties.
		writeError(w, regErr.HTTPStatus, regErr.SRCode, regErr.Message)
	} else {
		// This is an unexpected or generic error.
		// Return a generic 500 error.
		writeError(w, http.StatusInternalServerError, sr.ErrStoreError.Code, err.Error())
	}
}
