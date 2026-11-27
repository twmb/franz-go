// Package sr provides a schema registry client and a helper type to encode
// values and decode data according to the schema registry wire format.
//
// As mentioned on the Serde type, this package does not provide schema
// auto-discovery and type auto-decoding. To aid in strong typing and validated
// encoding/decoding, you must register IDs and values to how to encode or
// decode them. For schema-registry-aware protobuf encoding with automatic
// registration, see github.com/twmb/franz-go/plugin/srproto.
//
// The client does not automatically cache schemas, instead, the Serde type is
// used for the actual caching of IDs to how to encode/decode the IDs. The
// Client type itself simply speaks http to your schema registry and returns
// the results.
//
// To read more about the schema registry, see the following:
//
//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html
package sr

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"time"
)

// ResponseError is the type returned from the schema registry for errors.
type ResponseError struct {
	// Method is the requested http method.
	Method string `json:"-"`
	// URL is the full path that was requested that resulted in this error,
	// with any password in it replaced by "xxxxx".
	URL string `json:"-"`
	// StatusCode is the status code that was returned for this error.
	StatusCode int `json:"-"`
	// Raw contains the raw response body.
	Raw []byte `json:"-"`

	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (e *ResponseError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return string(e.Raw)
}

// SchemaError returns the typed Error corresponding to this response error's error code.
// If the error code is unknown, this returns nil.
func (e *ResponseError) SchemaError() *Error {
	return ErrorForCode(e.ErrorCode)
}

// Client talks to a schema registry and contains helper functions to serialize
// and deserialize objects according to schemas.
type Client struct {
	urls       []string
	httpcl     *http.Client
	dialTLS    *tls.Config
	ua         string
	defParams  Param
	defContext string
	opts       []ClientOpt
	logFn      func(int8, string, ...any)
	logLvlFn   func() int8

	basicAuth *struct {
		user string
		pass string
	}
	bearerToken string
	preReq      func(req *http.Request) error

	sem chan struct{} // a slot per request in flight, see maxConcurrentRequests
}

// NewClient returns a new schema registry client.
func NewClient(opts ...ClientOpt) (*Client, error) {
	cl := &Client{
		urls:     []string{"http://localhost:8081"},
		httpcl:   &http.Client{Timeout: 5 * time.Second},
		ua:       "franz-go",
		opts:     opts,
		logFn:    func(int8, string, ...any) {},
		logLvlFn: func() int8 { return LogLevelInfo },
		sem:      make(chan struct{}, maxConcurrentRequests),
	}

	for _, opt := range opts {
		opt.apply(cl)
	}

	if len(cl.urls) == 0 {
		return nil, errors.New("unable to create client with no URLs")
	}

	if cl.httpcl == nil {
		return nil, errors.New("unable to create client with no http client")
	}

	if cl.dialTLS != nil {
		// Sanity check some invariants: if the user is using DialTLSConfig
		// and a custom http client, the client must be compatible with us
		// setting the one needed field (and no others should be set).
		if cl.httpcl.Transport != nil {
			tr, ok := cl.httpcl.Transport.(*http.Transport)
			if !ok {
				return nil, errors.New("unable to use DialTLSConfig with a custom http.Client that does not use an *http.Transport")
			}
			if tr.Dial != nil || tr.DialContext != nil || tr.DialTLS != nil || tr.DialTLSContext != nil {
				return nil, errors.New("unable to use DialTLSConfig with an http.Client that has a custom dial function")
			}
			tr = tr.Clone()
			tr.TLSClientConfig = cl.dialTLS
			cl.httpcl.Transport = tr
		} else {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			tr.TLSClientConfig = cl.dialTLS
			cl.httpcl.Transport = tr
		}
	}

	return cl, nil
}

func namefn(fn any) string {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return ""
	}
	name := runtime.FuncForPC(v.Pointer()).Name()
	dot := strings.LastIndexByte(name, '.')
	if dot >= 0 {
		return name[dot+1:]
	}
	return name
}

// Opts returns the options that were used to create this client. This can be
// as a base to generate a new client, where you can add override options to
// the end of the original input list.
func (cl *Client) Opts() []ClientOpt {
	return cl.opts
}

// OptValue returns the value for the given configuration option. If the
// given option does not exist, this returns nil. This function takes either
// the option function, such as UserAgent, or its name.
//
// If a configuration option has multiple inputs, this function returns only
// the first input. Variadic option inputs are returned as a single slice.
// Options that are internally stored as a pointer are returned as their
// string input; you can see if the option is internally nil by looking at
// the second value returned from OptValues.
//
//	cl, _ := NewClient(
//		URLs("foo", "bar"),
//		UserAgent("baz"),
//	)
//	urls := cl.OptValue("URLs")   // urls is []string{"http://foo", "http://bar"}; string lookup for the option works
//	ua := cl.OptValue(UserAgent)  // ua is "baz"
//	unk := cl.OptValue("Unknown") // unk is nil
func (cl *Client) OptValue(opt any) any {
	vs := cl.OptValues(opt)
	if len(vs) > 0 {
		return vs[0]
	}
	return nil
}

// OptValues returns all values for options. This method is useful for
// options that have multiple inputs (notably, BasicAuth). This is also useful
// for options that are internally stored as a pointer -- this function will
// return the string value of the option but also whether the option is non-nil.
// Boolean options are returned as a single-element slice with the bool value.
// Variadic inputs are returned as a single slice. If the input option does not
// exist, this returns nil.
//
//	cl, _ := NewClient(
//		URLs("foo", "bar"),
//		UserAgent("baz"),
//		BasicAuth("user", "pass"),
//	)
//	urls := cl.OptValues("URLs")   // urls is []any{[]string{"http://foo", "http://bar"}}
//	ua := cl.OptValues(UserAgent)  // ua is []any{"baz"}
//	ba := cl.OptValues(BasicAuth)  // ba is []any{"user", "pass"}
//	unk := cl.OptValues("Unknown") // unk is nil
func (cl *Client) OptValues(opt any) []any {
	name := namefn(opt)
	if s, ok := opt.(string); ok {
		name = s
	}

	switch name {
	case namefn(HTTPClient):
		return []any{cl.httpcl}
	case namefn(UserAgent):
		return []any{cl.ua}
	case namefn(URLs):
		return []any{cl.urls}
	case namefn(DialTLSConfig):
		return []any{cl.dialTLS}
	case namefn(BasicAuth):
		if cl.basicAuth != nil {
			return []any{cl.basicAuth.user, cl.basicAuth.pass}
		}
		return []any{}
	case namefn(BearerToken):
		return []any{cl.bearerToken}
	case namefn(PreReq):
		return []any{cl.preReq}
	case namefn(DefaultSchemaContext):
		return []any{cl.defContext}
	case namefn(DefaultParams):
		return []any{cl.defParams}
	case namefn(LogFn):
		return []any{cl.logFn}
	case namefn(LogLevelFn):
		return []any{cl.logLvlFn}
	case namefn(LogLevel):
		return []any{cl.logLvlFn()}
	default:
		return nil
	}
}

var schemaContextKey = "context_key"

// InContext scopes requests to be issued to the given schema registry context (namespacing).
// For client wide scoping, use [DefaultSchemaContext]. The following will return the subjects
// from the "foo" context:
//
//	cl.Subjects(sr.InContext(ctx, "foo"))
func InContext(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, &schemaContextKey, name)
}

func (cl *Client) schemaContext(ctx context.Context) string {
	if name, _ := ctx.Value(&schemaContextKey).(string); name != "" {
		return name
	}
	return cl.defContext
}

// Do sends an HTTP request to the schema registry using the given method and
// path, optionally encoding the provided request body `v` as JSON, and decoding
// the response into `into` if non-nil.
//
// This method is a general-purpose extension point for users who need to
// interact with custom or non-standard schema registry endpoints while reusing
// the client's configured authentication, user-agent, retry logic, and error
// handling.
func (cl *Client) Do(ctx context.Context, method, path string, v, into any) error {
	return cl.do(ctx, method, path, v, into)
}

func (cl *Client) get(ctx context.Context, path string, into any) error {
	return cl.doOwn(ctx, http.MethodGet, path, nil, into)
}

func (cl *Client) post(ctx context.Context, path string, v, into any) error {
	return cl.doOwn(ctx, http.MethodPost, path, v, into)
}

func (cl *Client) put(ctx context.Context, path string, v, into any) error {
	return cl.doOwn(ctx, http.MethodPut, path, v, into)
}

func (cl *Client) delete(ctx context.Context, path string, into any) error {
	return cl.doOwn(ctx, http.MethodDelete, path, nil, into)
}

// JoinPath removes empty, "." and ".." segments, and PathEscape keeps them,
// so a subject, GUID, or context that is empty or named either would change
// the endpoint. doOwn checks a path our helpers built from such a name; a
// path given to Do is the caller's.
func (cl *Client) doOwn(ctx context.Context, method, path string, v, into any) error {
	if p := path + "/"; strings.Contains(p, "//") || strings.Contains(p, "/./") || strings.Contains(p, "/../") {
		return fmt.Errorf("invalid path %q: a subject, GUID, or context cannot be empty, %q, or %q", path, ".", "..")
	}
	return cl.do(ctx, method, path, v, into)
}

func (cl *Client) do(ctx context.Context, method, path string, v, into any) error {
	// "." is the registry's default context, which has no path prefix.
	ctxName := cl.schemaContext(ctx)
	if ctxName == ".." {
		return fmt.Errorf("invalid schema context %q", ctxName)
	}
	if ctxName != "" && ctxName != "." && path != "/contexts" && !strings.HasPrefix(path, "/contexts/") {
		path = "/contexts/" + url.PathEscape(ctxName) + path
	}
	// URL.JoinPath returns the base URL alone for a path with an invalid
	// escape, which would send the request to the registry's root.
	if _, err := url.PathUnescape(path); err != nil {
		return fmt.Errorf("invalid path %q: %w", path, err)
	}

	// At most maxConcurrentRequests requests are in flight per client.
	select {
	case cl.sem <- struct{}{}:
		defer func() { <-cl.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	urls := cl.urls

start:
	base, err := url.Parse(urls[0])
	if err != nil {
		// A url.Error quotes the whole URL, password included.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return fmt.Errorf("unable to parse URL to join with %q: %w", path, err)
	}
	u := base.JoinPath(path)
	// Logs and errors use logURL, which replaces a password in the URL with
	// "xxxxx".
	logURL := u.Redacted()

	urls = urls[1:]

	var reqBody io.Reader
	if v != nil {
		marshaled, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("unable to encode body for %s %q: %w", method, logURL, err)
		}
		reqBody = bytes.NewReader(marshaled)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
	if err != nil {
		return fmt.Errorf("unable to create request for %s %q: %v", method, logURL, err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("User-Agent", cl.ua)
	if cl.basicAuth != nil {
		req.SetBasicAuth(cl.basicAuth.user, cl.basicAuth.pass)
	}
	if cl.bearerToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", cl.bearerToken))
	}
	cl.applyParams(ctx, req)

	if cl.preReq != nil {
		if err := cl.preReq(req); err != nil {
			return fmt.Errorf("pre-request hook failed for %s %q: %w", method, logURL, err)
		}
	}
	if cl.logLvlFn() >= LogLevelDebug {
		cl.logFn(LogLevelDebug, "sending request", "method", method, "URL", req.URL.Redacted(), "has_bearer", cl.bearerToken != "", "has_basic_auth", cl.basicAuth != nil)
	} else {
		cl.logFn(LogLevelInfo, "sending request", "method", method, "URL", logURL)
	}
	resp, err := cl.httpcl.Do(req)
	if err != nil {
		if len(urls) == 0 {
			return fmt.Errorf("unable to %s %q: %w", method, logURL, err)
		}
		cl.logFn(LogLevelDebug, "retrying request", "method", method, "URL", logURL, "error", err)
		goto start
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("unable to read response body from %s %q: %w", method, logURL, err)
	}

	if resp.StatusCode >= 300 {
		e := &ResponseError{
			Method:     method,
			URL:        logURL,
			StatusCode: resp.StatusCode,
			Raw:        bytes.TrimSpace(body),
		}
		if len(e.Raw) == 0 {
			e.Message = "no response"
		}
		_ = json.Unmarshal(body, e) // best effort
		return e
	}

	if into != nil {
		switch into := into.(type) {
		case *[]byte:
			*into = body // return raw body to caller
		default:
			if err := json.Unmarshal(body, into); err != nil {
				return fmt.Errorf("unable to decode ok response body from %s %q: %w", method, logURL, err)
			}
		}
	}
	return nil
}
