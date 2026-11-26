package srfake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

// Returns the query parameter name, a subject or subject prefix, and whether
// the request has it. As in Confluent's registry, a context path adds its
// context to a value without one, and gives a request without the parameter
// the context as its value.
func contextParam(req *http.Request, name string) (string, bool) {
	q := req.URL.Query()
	value, ok := q.Get(name), q.Has(name)
	if ctx := requestContext(req); ctx != "" {
		if !hasContext(value) {
			value = qualifiedContext(ctx) + value
		}
		ok = true
	}
	return value, ok
}

// Reports whether subject matches a subjectPrefix, as Confluent's registry
// matches it: ":*:" followed by a name prefix matches in every context, a
// prefix that names a context matches the qualified subject, and any other
// prefix matches subjects of the default context.
func matchesPrefix(subject, prefix string) bool {
	switch {
	case strings.HasPrefix(prefix, ":*:"):
		_, name := splitSubject(subject)
		return strings.HasPrefix(name, prefix[len(":*:"):])
	case strings.HasPrefix(prefix, ":."):
		return strings.HasPrefix(subject, prefix)
	default:
		return subjectContext(subject) == "." && strings.HasPrefix(subject, prefix)
	}
}

// Reports whether subject names its context, or the wildcard context.
func hasContext(subject string) bool {
	return strings.HasPrefix(subject, ":.") || strings.HasPrefix(subject, ":*:")
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

// contextMiddleware serves a /contexts/{context}/... request as the path after
// the context, as Confluent's registry does. The request context carries the
// context's name. A subject without a context after /subjects in the path gets
// the context, and so does the subject of a root /config or /mode. A root
// /config or /mode without a subject gets the context itself as its subject.
// Every subject in a path, with or without a context path, is then normalized
// as rewriteSubjects describes.
func (*Registry) contextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		escaped := req.URL.EscapedPath()
		var ctx string
		if rest, ok := strings.CutPrefix(escaped, "/contexts/"); ok {
			rawName, after, ok := strings.Cut(rest, "/")
			if !ok {
				next.ServeHTTP(w, req)
				return
			}
			name, err := url.PathUnescape(rawName)
			if err != nil {
				writeError(w, http.StatusBadRequest, http.StatusBadRequest, err.Error())
				return
			}
			name, ok = normalizeContext(name)
			if !ok {
				writeError(w, http.StatusBadRequest, http.StatusBadRequest, fmt.Sprintf("Context name '%s' cannot contain a colon", name))
				return
			}
			ctx, escaped = name, "/"+strings.TrimRight(after, "/")
			req = req.WithContext(context.WithValue(req.Context(), ctxKey{}, name))
		}

		rewritten := "/" + strings.Join(rewriteSubjects(strings.Split(escaped[1:], "/"), ctx), "/")
		if ctx == "" && rewritten == escaped {
			next.ServeHTTP(w, req)
			return
		}
		path, err := url.PathUnescape(rewritten)
		if err != nil {
			writeError(w, http.StatusBadRequest, http.StatusBadRequest, err.Error())
			return
		}
		req.URL.Path, req.URL.RawPath = path, rewritten

		next.ServeHTTP(w, req)
	})
}

// Returns the escaped path segments segs with each subject as Confluent's
// registry reads it. A subject is the segment after "subjects", or after a root
// "config" or "mode". When ctx is not empty, a subject without a context gets
// ctx, and a root config or mode without a subject gets ctx as its subject.
// Each subject is then normalized: ":.:foo" is foo, and ":.ctx" is ":.ctx:". A
// root config or mode whose subject is the default context loses its subject,
// which makes it the global route.
func rewriteSubjects(segs []string, ctx string) []string {
	out := make([]string, 0, len(segs)+1)
	var subjectNext, rootConfig bool
	for i, seg := range segs {
		if !subjectNext {
			root := i == 0 && (seg == "config" || seg == "mode")
			subjectNext = seg == "subjects" || root
			rootConfig = rootConfig || root
			out = append(out, seg)
			continue
		}
		subjectNext = false
		orig, err := url.PathUnescape(seg)
		if err != nil {
			out = append(out, seg)
			continue
		}
		s := orig
		if ctx != "" && !hasContext(s) {
			s = qualifiedContext(ctx) + s
		}
		s = normalizeSubject(s)
		switch {
		case s == "" && rootConfig:
		case s == orig || s == "":
			out = append(out, seg)
		default:
			out = append(out, url.PathEscape(s))
		}
	}
	if rootConfig && subjectNext && ctx != "" && qualifiedContext(ctx) != "" {
		out = append(out, url.PathEscape(qualifiedContext(ctx)))
	}
	return out
}

// Returns name as a context, without surrounding colons and with a leading
// dot, as Confluent's registry normalizes it. A colon inside the name makes it
// invalid.
func normalizeContext(name string) (string, bool) {
	name = strings.TrimPrefix(name, ":")
	name = strings.TrimSuffix(name, ":")
	if strings.Contains(name, ":") {
		return name, false
	}
	if !strings.HasPrefix(name, ".") {
		name = "." + name
	}
	return name, true
}

// Returns the prefix that puts a subject into ctx: empty for the default
// context, ":ctx:" otherwise.
func qualifiedContext(ctx string) string {
	if ctx == "." {
		return ""
	}
	return ":" + ctx + ":"
}

/* -------------------------------------------------------------------------
   Handlers – Schemas
   ------------------------------------------------------------------------- */

// idSchema is the body of GET /schemas/ids/{id}. As in Confluent's registry,
// it has the GUID of the schema, and the subject and version of the schema when
// the lookup has a subject.
type idSchema struct {
	Subject string `json:"subject,omitempty"`
	Version int    `json:"version,omitempty"`
	GUID    string `json:"guid,omitempty"`
	sr.Schema
}

// handleGetSchemaByID emulates GET /schemas/ids/{id} to retrieve a full schema
// definition by its ID.
func (r *Registry) handleGetSchemaByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}
	subject, hasSubject := contextParam(req, "subject")
	r.mu.RLock()
	defer r.mu.RUnlock()

	ss, ok := r.schemaByIDLocked(id, subject, hasSubject)
	if !ok {
		r.handleAPIError(w, errSchemaNotFound(strconv.Itoa(id)))
		return
	}
	body := idSchema{GUID: ss.GUID, Schema: ss.Schema}
	if hasSubject {
		body.Subject, body.Version = ss.Subject, ss.Version
	}
	respondJSON(w, http.StatusOK, body)
}

// handleGetRawSchemaByID emulates GET /schemas/ids/{id}/schema to retrieve
// only the schema string by its ID.
func (r *Registry) handleGetRawSchemaByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}
	subject, hasSubject := contextParam(req, "subject")
	r.mu.RLock()
	defer r.mu.RUnlock()

	ss, ok := r.schemaByIDLocked(id, subject, hasSubject)
	if !ok {
		r.handleAPIError(w, errSchemaNotFound(strconv.Itoa(id)))
		return
	}
	respondJSON(w, http.StatusOK, ss.Schema.Schema)
}

// handleGetSchemaVersionsByID emulates GET /schemas/ids/{id}/versions to retrieve
// all subject-version pairs that use the given schema ID.
func (r *Registry) handleGetSchemaVersionsByID(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		r.handleAPIError(w, errInvalidSchema("invalid schema id"))
		return
	}
	subject, hasSubject := contextParam(req, "subject")
	deleted := req.URL.Query().Get("deleted") == "true"

	r.mu.RLock()
	defer r.mu.RUnlock()

	usages, ok := r.idUsagesLocked(id, subject, hasSubject, deleted)
	if !ok {
		r.handleAPIError(w, errSchemaNotFound(""))
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
	subject, hasSubject := contextParam(req, "subject")
	deleted := req.URL.Query().Get("deleted") == "true"

	r.mu.RLock()
	defer r.mu.RUnlock()

	usages, ok := r.idUsagesLocked(id, subject, hasSubject, deleted)
	if !ok {
		r.handleAPIError(w, errSchemaNotFound(""))
		return
	}
	subjects := make([]string, 0, len(usages))
	for _, u := range usages {
		if len(subjects) == 0 || subjects[len(subjects)-1] != u.Subject {
			subjects = append(subjects, u.Subject)
		}
	}
	respondJSON(w, http.StatusOK, subjects)
}

// Returns the versions that carry id in the context that a lookup of id
// resolves to, sorted by subject and then version. As in Confluent's registry,
// the subject query parameter selects the context and does not filter the
// subjects. As in Confluent's registry, soft-deleted versions are listed only
// with ?deleted=true, and ?deletedOnly is not supported. The caller must hold
// the lock.
func (r *Registry) idUsagesLocked(id int, subject string, hasSubject, deleted bool) ([]sr.SubjectVersion, bool) {
	ss, ok := r.schemaByIDLocked(id, subject, hasSubject)
	if !ok {
		return nil, false
	}
	ctx := subjectContext(ss.Subject)
	usages := make([]sr.SubjectVersion, 0)
	for s, subj := range r.subjects {
		if subjectContext(s) != ctx {
			continue
		}
		for v, vd := range subj.versions {
			if vd.schema.ID == id && (deleted || !vd.isDeleted) {
				usages = append(usages, sr.SubjectVersion{Subject: s, Version: v})
			}
		}
	}
	sort.Slice(usages, func(i, j int) bool {
		if usages[i].Subject != usages[j].Subject {
			return usages[i].Subject < usages[j].Subject
		}
		return usages[i].Version < usages[j].Version
	})
	return usages, true
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
		r.handleAPIError(w, errSchemaNotFound(guid))
		return
	}
	respondJSON(w, http.StatusOK, ss)
}

// handleGetSchemaIDsByGUID emulates GET /schemas/guids/{guid}/ids to retrieve the
// (context, schema ID) pairs that the GUID resolves to. A GUID is global, so
// every known context is searched, in sorted order; each context that has the
// GUID contributes its own ID. As in Confluent's registry, that is the ID last
// seeded, registered, or soft-deleted with the GUID, which a permanent delete
// does not remove, and an unknown GUID returns an empty list.
func (r *Registry) handleGetSchemaIDsByGUID(w http.ResponseWriter, req *http.Request) {
	guid := req.PathValue("guid")

	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]sr.ContextID, 0)
	for _, c := range append([]string{"."}, r.sortedContextsLocked()...) {
		if id, ok := r.idsByGUID[contextGUID{c, guid}]; ok {
			ids = append(ids, sr.ContextID{Context: c, ID: id})
		}
	}
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
	latestOnly := req.URL.Query().Get("latestOnly") == "true"
	// As in Confluent's registry, GET /schemas without a prefix lists the
	// default context.
	subjPrefix, _ := contextParam(req, "subjectPrefix")

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
		if !includeDeleted && !subj.active() {
			continue
		}
		if !matchesPrefix(s, subjPrefix) {
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
	// As in Confluent's registry, GET /subjects without a prefix lists every
	// context.
	subjPrefix, ok := contextParam(req, "subjectPrefix")
	if !ok {
		subjPrefix = ":*:"
	}

	r.mu.RLock()
	subjects := make([]string, 0, len(r.subjects))
	for s, subj := range r.subjects {
		if !del.keep(!subj.active()) {
			continue
		}
		if !matchesPrefix(s, subjPrefix) {
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
	if !ok || len(subj.versions) == 0 || (!subj.active() && !del.any()) {
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

	var body struct {
		sr.Schema
		ID      *int `json:"id"`
		Version *int `json:"version"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidSchemaWithCause(err, err.Error()))
		return
	}
	reqID, reqVersion := -1, 0
	if body.ID != nil {
		reqID = *body.ID
	}
	if body.Version != nil {
		reqVersion = *body.Version
	}

	id, _, err := r.register(subject, body.Schema, reqID, reqVersion)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]int{"id": id})
}

// handleGetSubjectSchemaByVersion emulates GET /subjects/{subject}/versions/{version}
// to return a specific schema version for a subject.
func (r *Registry) handleGetSubjectSchemaByVersion(w http.ResponseWriter, req *http.Request) {
	if ss, ok := r.subjectVersion(w, req); ok {
		respondJSON(w, http.StatusOK, ss)
	}
}

// Returns the version that GET /subjects/{subject}/versions/{version} and its
// /schema route ask for, or writes the error and returns false.
func (r *Registry) subjectVersion(w http.ResponseWriter, req *http.Request) (sr.SubjectSchema, bool) {
	subject := req.PathValue("subject")
	del := parseDeleted(req)
	version, err := parseVersion(req.PathValue("version"))
	if err != nil {
		r.handleAPIError(w, err)
		return sr.SubjectSchema{}, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	ss, ok := r.lookupVersionLocked(subject, version, del.any())
	if !ok {
		r.handleAPIError(w, r.errMissingVersionLocked(subject, version, del.any()))
	}
	return ss, ok
}

// handleGetSubjectRawSchemaByVersion emulates GET /subjects/{subject}/versions/{version}/schema
// to return the raw schema string for a specific version.
func (r *Registry) handleGetSubjectRawSchemaByVersion(w http.ResponseWriter, req *http.Request) {
	if ss, ok := r.subjectVersion(w, req); ok {
		respondJSON(w, http.StatusOK, ss.Schema.Schema)
	}
}

// handleGetReferencedBy emulates GET /subjects/{subject}/versions/{version}/referencedby
// to return a list of schema IDs that reference the specified schema.
func (r *Registry) handleGetReferencedBy(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	versionStr := req.PathValue("version")
	del := parseDeleted(req)

	asked, err := parseVersion(versionStr)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Find the target schema. As in Confluent's registry, a numbered version
	// is found even if it is soft-deleted, and latest honors ?deleted.
	includeTarget := del.any() || asked != -1
	target, ok := r.lookupVersionLocked(subject, asked, includeTarget)
	if !ok {
		r.handleAPIError(w, r.errMissingVersionLocked(subject, asked, includeTarget))
		return
	}
	subject, version := target.Subject, target.Version

	// Find all schemas that reference this target schema
	referencingIDs := make([]int, 0) // Initialize as empty slice, not nil
	for referrer, subj := range r.subjects {
		for _, versionData := range subj.versions {
			if !del.keep(versionData.isDeleted) {
				continue
			}

			// Check if this schema references our target
			for _, ref := range versionData.schema.References {
				if refSubject(referrer, ref.Subject) == subject && ref.Version == version {
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

	version, err := parseVersion(verStr)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	version, err = r.deleteVersion(subject, version, permanent)
	if err != nil {
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

	// Look the schema up in subject, and in the same name in every other
	// context, as Confluent's registry does.
	match, ok := r.findUsingContextsLocked(subject, func(s string) (sr.SubjectSchema, bool) {
		subj, ok := r.subjects[s]
		if !ok || (!subj.active() && !del.any()) {
			return sr.SubjectSchema{}, false
		}
		for _, versionData := range subj.versions {
			if del.keep(versionData.isDeleted) && r.schemasEqual(versionData.schema.Schema, body) {
				return versionData.schema, true
			}
		}
		return sr.SubjectSchema{}, false
	})
	if !ok {
		if subj, exists := r.subjects[subject]; !exists || (!subj.active() && !del.any()) {
			r.handleAPIError(w, errSubjectNotFound(subject))
			return
		}
		r.handleAPIError(w, errSchemaNotFound(""))
		return
	}

	resp := map[string]any{
		"subject": match.Subject,
		"version": match.Version,
		"id":      match.ID,
		"guid":    match.GUID,
		"schema":  match.Schema.Schema,
	}
	if match.Type != sr.TypeAvro {
		resp["schemaType"] = match.Type.String()
	}
	if len(match.References) > 0 {
		resp["references"] = match.References
	}
	respondJSON(w, http.StatusOK, resp)
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
func (r *Registry) handleGetGlobalConfig(w http.ResponseWriter, req *http.Request) {
	r.getConfig(w, req, "")
}

// handlePutGlobalConfig emulates PUT /config to update the global
// compatibility level.
func (r *Registry) handlePutGlobalConfig(w http.ResponseWriter, req *http.Request) {
	r.putConfig(w, req, "")
}

// handleGetSubjectConfig emulates GET /config/{subject} to retrieve a
// subject's compatibility level.
func (r *Registry) handleGetSubjectConfig(w http.ResponseWriter, req *http.Request) {
	r.getConfig(w, req, req.PathValue("subject"))
}

// handlePutSubjectConfig emulates PUT /config/{subject} to update a subject's
// compatibility level.
func (r *Registry) handlePutSubjectConfig(w http.ResponseWriter, req *http.Request) {
	r.putConfig(w, req, req.PathValue("subject"))
}

// Serves GET /config and GET /config/{subject}, as Confluent's registry does:
// with ?defaultToGlobal=true, the level that applies to subject, else the level
// set for subject. A subject without a level returns 40408, and the global
// level is the default until it is set.
func (r *Registry) getConfig(w http.ResponseWriter, req *http.Request, subject string) {
	defaultToGlobal := req.URL.Query().Get("defaultToGlobal") == "true"

	r.mu.RLock()
	defer r.mu.RUnlock()

	level, ok := r.compats[subject]
	switch {
	case defaultToGlobal:
		level = r.compatInScopeLocked(subject)
	case !ok && subject == "":
		level = r.defaultCompat
	case !ok:
		r.handleAPIError(w, errCompatNotConfigured(subject))
		return
	}
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.levelOf(level)})
}

// Serves PUT /config and PUT /config/{subject}. As in Confluent's registry, a
// body without a level keeps the level that is set, a null level removes it,
// and a subject in read-only mode refuses the change.
func (r *Registry) putConfig(w http.ResponseWriter, req *http.Request, subject string) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	var body struct {
		Compatibility json.RawMessage `json:"compatibility"`
	}
	var level sr.CompatibilityLevel
	err := decodeJSONRequest(req.Body, &body)
	if err == nil && body.Compatibility != nil && string(body.Compatibility) != "null" {
		if err = json.Unmarshal(body.Compatibility, &level); err == nil && level.String() == "" {
			err = errInvalidCompatLevel()
		}
	}
	if err != nil {
		r.handleAPIError(w, errInvalidCompatLevel())
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.errReadOnlyLocked(subject); err != nil {
		r.handleAPIError(w, err)
		return
	}
	if body.Compatibility == nil {
		level = r.compats[subject]
	}
	r.compats[subject] = level
	resp := make(map[string]sr.CompatibilityLevel)
	if level != 0 {
		resp["compatibility"] = level
	}
	respondJSON(w, http.StatusOK, resp)
}

// handleDeleteGlobalConfig emulates DELETE /config, which removes the global
// compatibility level and returns the level that was in effect, as Confluent's
// registry does.
func (r *Registry) handleDeleteGlobalConfig(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.errReadOnlyLocked(""); err != nil {
		r.handleAPIError(w, err)
		return
	}
	level, ok := r.compats[""]
	if !ok {
		level = r.defaultCompat
	}
	delete(r.compats, "")
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.levelOf(level)})
}

// handleDeleteSubjectConfig emulates DELETE /config/{subject}, which removes the
// compatibility level of a subject and returns it, as Confluent's registry
// does. A subject without a level returns 40401.
func (r *Registry) handleDeleteSubjectConfig(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")

	r.mu.Lock()
	defer r.mu.Unlock()

	level, ok := r.compats[subject]
	if !ok {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}
	if err := r.errReadOnlyLocked(subject); err != nil {
		r.handleAPIError(w, err)
		return
	}
	delete(r.compats, subject)
	respondJSON(w, http.StatusOK, map[string]sr.CompatibilityLevel{"compatibilityLevel": r.levelOf(level)})
}

/* -------------------------------------------------------------------------
   Handler – Mode
   ------------------------------------------------------------------------- */

// handleGetMode emulates GET /mode and returns the global mode.
func (r *Registry) handleGetMode(w http.ResponseWriter, req *http.Request) {
	r.getMode(w, req, "")
}

// handleGetSubjectMode emulates GET /mode/{subject}.
func (r *Registry) handleGetSubjectMode(w http.ResponseWriter, req *http.Request) {
	r.getMode(w, req, req.PathValue("subject"))
}

// Serves GET /mode and GET /mode/{subject}, as Confluent's registry does: with
// ?defaultToGlobal=true, the mode that applies to subject, else the mode set
// for subject. A subject without a mode returns 40409, and the global mode is
// READWRITE until it is set.
func (r *Registry) getMode(w http.ResponseWriter, req *http.Request, subject string) {
	defaultToGlobal := req.URL.Query().Get("defaultToGlobal") == "true"

	r.mu.RLock()
	defer r.mu.RUnlock()

	mode, ok := r.modes[subject]
	switch {
	case defaultToGlobal:
		mode = r.modeInScopeLocked(subject)
	case !ok && subject == "":
		mode = sr.ModeReadWrite
	case !ok:
		r.handleAPIError(w, errModeNotConfigured(subject))
		return
	}
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": mode})
}

// handlePutMode emulates PUT /mode and sets the global mode. The ?force query
// parameter is accepted but not enforced: the mock does not restrict setting
// IMPORT mode on a non-empty registry.
func (r *Registry) handlePutMode(w http.ResponseWriter, req *http.Request) {
	r.putMode(w, req, "")
}

// handlePutSubjectMode emulates PUT /mode/{subject} and sets a per-subject mode.
// As with the global setter, ?force is accepted but not enforced.
func (r *Registry) handlePutSubjectMode(w http.ResponseWriter, req *http.Request) {
	r.putMode(w, req, req.PathValue("subject"))
}

// Serves PUT /mode and PUT /mode/{subject}. As in Confluent's registry, a body
// without a mode removes the mode that is set.
func (r *Registry) putMode(w http.ResponseWriter, req *http.Request, subject string) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<10)
	var body struct {
		Mode *sr.Mode `json:"mode"`
	}
	if err := decodeJSONRequest(req.Body, &body); err != nil {
		r.handleAPIError(w, errInvalidMode(err.Error()))
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if body.Mode == nil {
		delete(r.modes, subject)
		respondJSON(w, http.StatusOK, map[string]sr.Mode{})
		return
	}
	r.modes[subject] = *body.Mode
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": *body.Mode})
}

// handleDeleteMode emulates DELETE /mode, resetting the global mode to the
// default (READWRITE) and returning the mode that was in effect.
func (r *Registry) handleDeleteMode(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	deleted, ok := r.modes[""]
	if !ok {
		deleted = sr.ModeReadWrite
	}
	delete(r.modes, "")
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": deleted})
}

// handleDeleteSubjectMode emulates DELETE /mode/{subject}, removing a
// per-subject mode override and returning the mode that was removed.
func (r *Registry) handleDeleteSubjectMode(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")

	r.mu.Lock()
	defer r.mu.Unlock()

	deleted, ok := r.modes[subject]
	if !ok {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}
	delete(r.modes, subject)
	respondJSON(w, http.StatusOK, map[string]sr.Mode{"mode": deleted})
}

/* -------------------------------------------------------------------------
   Handlers - Contexts
   ------------------------------------------------------------------------- */

// handleGetContexts emulates GET /contexts.
func (r *Registry) handleGetContexts(w http.ResponseWriter, req *http.Request) {
	contextPrefix := req.URL.Query().Get("contextPrefix")
	offsetStr := req.URL.Query().Get("offset")
	limitStr := req.URL.Query().Get("limit")

	r.mu.RLock()
	ctxSet := map[string]bool{".": true}
	for c := range r.contexts {
		ctxSet[c] = true
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
	ctx, ok := normalizeContext(req.PathValue("context"))
	if !ok {
		writeError(w, http.StatusBadRequest, http.StatusBadRequest, fmt.Sprintf("Context name '%s' cannot contain a colon", ctx))
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// As in Confluent's registry, a soft-deleted subject keeps its context.
	for subject, subj := range r.subjects {
		if len(subj.versions) > 0 && subjectContext(subject) == ctx {
			r.handleAPIError(w, errContextNotEmpty(ctx))
			return
		}
	}

	delete(r.contexts, ctx)
	w.WriteHeader(http.StatusNoContent)
}

/* -------------------------------------------------------------------------
   Helpers
   ------------------------------------------------------------------------- */

// handleAPIError is a centralized error handler that takes an error and writes
// the correct HTTP response. It checks if the error is a registryError and uses
// its properties, otherwise returns a generic 500 error.
func (*Registry) handleAPIError(w http.ResponseWriter, err error) {
	if regErr, ok := errors.AsType[*registryError](err); ok {
		// This is a known registry error, use its properties.
		writeError(w, regErr.HTTPStatus, regErr.SRCode, regErr.Message)
	} else {
		// This is an unexpected or generic error.
		// Return a generic 500 error.
		writeError(w, http.StatusInternalServerError, sr.ErrStoreError.Code, err.Error())
	}
}
