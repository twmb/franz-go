// Package sr provides a schema registry client and a helper type to encode
// values and decode data according to the schema registry wire format.
//
// As mentioned on the Serde type, this package does not provide schema
// auto-discovery and type auto-decoding. To aid in strong typing and validated
// encoding/decoding, you must register IDs and values to how to encode or
// decode them.
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
	// URL is the full path that was requested that resulted in this error.
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

// Client talks to a schema registry and contains helper functions to serialize
// and deserialize objects according to schemas.
type Client struct {
	urls      []string
	httpcl    *http.Client
	dialTLS   *tls.Config
	ua        string
	defParams Param
	opts      []ClientOpt
	logFn     func(int8, string, ...any)
	logLvlFn  func() int8

	basicAuth *struct {
		user string
		pass string
	}
	bearerToken string
	preReq      func(req *http.Request) error
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
	if v.Type().Kind() != reflect.Func {
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
// given option does not exist, this returns nil. This function takes either a
// raw ClientOpt, or an Opt function name.
//
// If a configuration option has multiple inputs, this function returns only
// the first input. Variadic option inputs are returned as a single slice.
// Options that are internally stored as a pointer are returned as their
// string input; you can see if the option is internally nil by looking at
// the second value returned from OptValues.
//
//		var (
//	 		cl, _ := NewClient(
//	 			URLs("foo", "bar"),
//				UserAgent("baz"),
//	 		)
//	 		urls = cl.OptValue("URLs")     // urls is []string{"foo", "bar"}; string lookup for the option works
//	 		ua   = cl.OptValue(UserAgent)  // ua is "baz"
//	 		unk  = cl.OptValue("Unknown"), // unk is nil
//		)
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
// Variadic inputs are returned as a signle slice. If the input option does not
// exist, this returns nil.
//
//	     var (
//		 		cl, _ := NewClient(
//		 			URLs("foo", "bar"),
//					UserAgent("baz"),
//		 		)
//		 		urls = cl.OptValues("URLs")     // urls is []any{[]string{"foo", "bar"}}
//		 		ua   = cl.OptValues(UserAgent)  // ua is []any{"baz"}
//		 		ba   = cl.OptValues(BasicAuth)  // ba is []any{"user", "pass"}
//		 		unk  = cl.OptValues("Unknown"), // unk is nil
//	     )
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

func (cl *Client) get(ctx context.Context, path string, into any) error {
	return cl.do(ctx, http.MethodGet, path, nil, into)
}

func (cl *Client) post(ctx context.Context, path string, v, into any) error {
	return cl.do(ctx, http.MethodPost, path, v, into)
}

func (cl *Client) put(ctx context.Context, path string, v, into any) error {
	return cl.do(ctx, http.MethodPut, path, v, into)
}

func (cl *Client) delete(ctx context.Context, path string, into any) error {
	return cl.do(ctx, http.MethodDelete, path, nil, into)
}

func (cl *Client) do(ctx context.Context, method, path string, v, into any) error {
	urls := cl.urls

start:
	reqURL, err := url.JoinPath(urls[0], path)
	if err != nil {
		return fmt.Errorf("unable to join path for %q and %q: %w", urls[0], path, err)
	}

	urls = urls[1:]

	var reqBody io.Reader
	if v != nil {
		marshaled, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("unable to encode body for %s %q: %w", method, reqURL, err)
		}
		reqBody = bytes.NewReader(marshaled)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reqBody)
	if err != nil {
		return fmt.Errorf("unable to create request for %s %q: %v", method, reqURL, err)
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
			return fmt.Errorf("pre-request hook failed for %s %q: %w", method, reqURL, err)
		}
	}
	if cl.logLvlFn() >= LogLevelDebug {
		cl.logFn(LogLevelDebug, "sending request", "method", method, "URL", reqURL, "has_bearer", cl.bearerToken != "", "has_basic_auth", cl.basicAuth != nil)
	} else {
		cl.logFn(LogLevelInfo, "sending request", "method", method, "URL", reqURL)
	}
	resp, err := cl.httpcl.Do(req)
	if err != nil {
		if len(urls) == 0 {
			return fmt.Errorf("unable to %s %q: %w", method, reqURL, err)
		}
		cl.logFn(LogLevelDebug, "retrying request", "method", method, "URL", reqURL, "error", err)
		goto start
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("unable to read response body from %s %q: %w", method, reqURL, err)
	}

	if resp.StatusCode >= 300 {
		e := &ResponseError{
			Method:     method,
			URL:        reqURL,
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
				return fmt.Errorf("unable to decode ok response body from %s %q: %w", method, reqURL, err)
			}
		}
	}
	return nil
}
