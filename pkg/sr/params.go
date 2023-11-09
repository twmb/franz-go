package sr

import (
	"context"
	"fmt"
	"net/http"
)

// Param is a parameter that can be passed to various APIs. Each API documents
// the parameters they accept.
type Param struct {
	normalize       bool
	verbose         bool
	fetchMaxID      bool
	defaultToGlobal bool
	force           bool
	latestOnly      bool
	showDeleted     bool
	deletedOnly     bool
	format          string
	subjectPrefix   string
	subject         string
	page            *int
	limit           int
}

// WithParams adds query parameters to the given context. This is a merge
// operation: any non-zero parameter is kept. The variadic nature of this
// allows for a nicer api:
//
//	sr.WithParams(ctx, sr.Format("default"), sr.FetchMaxID)
func WithParams(ctx context.Context, p ...Param) context.Context {
	return context.WithValue(ctx, &paramsKey, mergeParams(p...))
}

func (cl *Client) applyParams(ctx context.Context, req *http.Request) {
	p := cl.defParams
	user, ok := ctx.Value(&paramsKey).(Param)
	if ok {
		p = mergeParams(p, user)
	}
	p.apply(req)
}

func (p Param) apply(req *http.Request) {
	q := req.URL.Query()
	if p.normalize {
		q.Set("normalize", "true")
	}
	if p.verbose {
		q.Set("verbose", "true")
	}
	if p.fetchMaxID {
		q.Set("fetchMaxId", "true")
	}
	if p.defaultToGlobal {
		q.Set("defaultToGlobal", "true")
	}
	if p.force {
		q.Set("force", "true")
	}
	if p.latestOnly {
		q.Set("latestOnly", "true")
	}
	if p.showDeleted {
		q.Set("deleted", "true")
	}
	if p.deletedOnly {
		q.Set("deletedOnly", "true")
	}
	if p.format != "" {
		q.Set("format", p.format)
	}
	if p.subjectPrefix != "" {
		q.Set("subjectPrefix", p.subjectPrefix)
	}
	if p.subject != "" {
		q.Set("subject", p.subject)
	}
	if p.page != nil && *p.page >= 0 {
		q.Set("page", fmt.Sprintf("%d", *p.page))
	}
	if p.limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", p.limit))
	}
	req.URL.RawQuery = q.Encode()
}

var paramsKey = "params_key"

func mergeParams(p ...Param) Param {
	var merged Param
	for _, p := range p {
		if p.normalize {
			merged.normalize = true
		}
		if p.verbose {
			merged.verbose = true
		}
		if p.fetchMaxID {
			merged.fetchMaxID = true
		}
		if p.defaultToGlobal {
			merged.defaultToGlobal = true
		}
		if p.force {
			merged.force = true
		}
		if p.latestOnly {
			merged.latestOnly = true
		}
		if p.showDeleted {
			merged.showDeleted = true
		}
		if p.deletedOnly {
			merged.deletedOnly = true
		}
		if p.format != "" {
			merged.format = p.format
		}
		if p.subjectPrefix != "" {
			merged.subjectPrefix = p.subjectPrefix
		}
		if p.subject != "" {
			merged.subject = p.subject
		}
		if p.page != nil && *p.page >= 0 {
			merged.page = p.page
		}
		if p.limit > 0 {
			merged.limit = p.limit
		}
	}
	return merged
}

var (
	// Normalize is a Param that configures whether or not to normalize
	// schema's in certain create- or get-schema operations.
	Normalize = Param{normalize: true}

	// Verbose is a Param that configures whether or not to return verbose
	// error messages when checking compatibility.
	Verbose = Param{verbose: true}

	// FetchMaxID is a Param that configures whether or not to fetch the
	// max schema ID in certain get-schema operations.
	FetchMaxID = Param{fetchMaxID: true}

	// DefaultToGlobal is a Param that changes get-compatibility or
	// get-mode to return the global compatibility or mode if the requested
	// subject does not exist.
	DefaultToGlobal = Param{defaultToGlobal: true}

	// Force is a Param that updating the mode if you are setting the mode
	// to IMPORT and schemas currently exist.
	Force = Param{force: true}

	// LatestOnly is a Param that configures whether or not to return only
	// the latest schema in certain get-schema operations.
	LatestOnly = Param{latestOnly: true}

	// ShowDeleted is a Param that configures whether or not to return
	// deleted schemas or subjects in certain get operations.
	ShowDeleted = Param{showDeleted: true}

	// DeletedOnly is a Param that configures whether to return only
	// deleted schemas or subjects in certain get operations.
	DeletedOnly = Param{deletedOnly: true}
)

// Format returns a Param that configures how schema's are returned in certain
// get-schema operations.
//
// For Avro schemas, the Format param supports "default" or "resolved". For
// Protobuf schemas, the Format param supports "default", "ignore_extensions",
// or "serialized".
func Format(f string) Param { return Param{format: f} }

// SubjectPrefix returns a Param that filters subjects by prefix when listing
// schemas.
func SubjectPrefix(pfx string) Param { return Param{subjectPrefix: pfx} }

// Subject returns a Param limiting which subject is returned in certain
// list-schema or list-subject operations.
func Subject(s string) Param { return Param{subject: s} }

/*
TODO once we know the header that is returned for pagination, we can use these.
// Page returns a Param for certain paginating APIs.
func Page(page int) Param { return Param{page: &page} }

// Limit returns a Param for certain paginating APIs.
func Limit(limit int) Param { return Param{limit: limit} }
*/
