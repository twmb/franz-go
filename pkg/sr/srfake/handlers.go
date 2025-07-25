package srfake

import (
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/twmb/franz-go/pkg/sr"
)

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
	sch, ok := r.schemasByID[id]
	r.mu.RUnlock()
	if !ok {
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
	sch, ok := r.schemasByID[id]
	r.mu.RUnlock()
	if !ok {
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
		if subj.isDeleted {
			continue
		}
		for version, versionData := range subj.versions {
			if versionData.schema.ID == id {
				// Skip soft-deleted versions
				if versionData.isDeleted {
					continue
				}
				usages = append(usages, map[string]any{
					"subject": subject,
					"version": version,
				})
			}
		}
	}

	respondJSON(w, http.StatusOK, usages)
}

/* -------------------------------------------------------------------------
   Handlers – Subjects
   ------------------------------------------------------------------------- */

// handleGetSubjects emulates GET /subjects to return a list of all registered
// subjects.
func (r *Registry) handleGetSubjects(w http.ResponseWriter, req *http.Request) {
	includeDeleted := req.URL.Query().Get("deleted") == "true"

	r.mu.RLock()
	subjects := make([]string, 0, len(r.subjects))
	for s, subj := range r.subjects {
		if !includeDeleted && subj.isDeleted {
			continue
		}
		subjects = append(subjects, s)
	}
	r.mu.RUnlock()

	sort.Strings(subjects)
	respondJSON(w, http.StatusOK, subjects)
}

// handleGetSubjectVersions emulates GET /subjects/{subject}/versions to return
// a list of all versions for a given subject.
func (r *Registry) handleGetSubjectVersions(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	includeDeleted := req.URL.Query().Get("deleted") == "true"

	r.mu.RLock()
	defer r.mu.RUnlock()

	subj, ok := r.subjects[subject]
	if !ok || (subj.isDeleted && !includeDeleted) {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}

	var versions []int
	for v, versionData := range subj.versions {
		if !includeDeleted && versionData.isDeleted {
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

	version, err := r.resolveVersion(subject, versionStr)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	sch, ok := r.getSchemaBySubjectVersion(subject, version)
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

	version, err := r.resolveVersion(subject, versionStr)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	sch, ok := r.getSchemaBySubjectVersion(subject, version)
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

	version, err := r.resolveVersion(subject, versionStr)
	if err != nil {
		r.handleAPIError(w, err)
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if the target schema exists
	_, exists := r.getSchemaBySubjectVersionLocked(subject, version)
	if !exists {
		r.handleAPIError(w, errVersionNotFound(subject, version))
		return
	}

	// Find all schemas that reference this target schema
	referencingIDs := make([]int, 0) // Initialize as empty slice, not nil
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

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if subject exists and is not soft-deleted
	subj, ok := r.subjects[subject]
	if !ok || subj.isDeleted {
		r.handleAPIError(w, errSubjectNotFound(subject))
		return
	}

	for v, versionData := range subj.versions {
		if versionData.isDeleted || !r.schemasEqual(versionData.schema.Schema, body) {
			continue
		}
		resp := map[string]any{
			"subject": subject,
			"version": v,
			"id":      versionData.schema.ID,
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

// handleGetMode emulates GET /mode and returns the mock's current operational
// mode.
func (*Registry) handleGetMode(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"mode": "READWRITE"})
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
