// The _test package suffix indicates this is a black-box test. It can only
// access the exported APIs of the 'srfake' package, just like a real consumer.
package srfake_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

var (
	userSchema = sr.Schema{
		Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}`,
		Type:   sr.TypeAvro,
	}
	userSchemaV2 = sr.Schema{
		Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"email","type":"string","default":""}]}`,
		Type:   sr.TypeAvro,
	}
	productSchema = sr.Schema{
		Schema: `{"type":"record","name":"Product","fields":[{"name":"sku","type":"string"}]}`,
		Type:   sr.TypeAvro,
	}
)

func TestSubjectHandlers(t *testing.T) {
	testCases := []struct {
		name       string
		method     string
		path       string
		body       string
		setup      func(*srfake.Registry)
		wantStatus int
		wantBody   string // Use jsonEqual for comparison
	}{
		// List Subjects
		{
			name:   "GET /subjects - no subjects",
			method: "GET",
			path:   "/subjects",
			setup: func(r *srfake.Registry) {
				// no setup needed
			},
			wantStatus: http.StatusOK,
			wantBody:   `[]`,
		},
		{
			name:   "GET /subjects - with subjects",
			method: "GET",
			path:   "/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("product-topic-value", 1, 2, productSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `["product-topic-value", "user-topic-value"]`,
		},
		{
			name:   "GET /subjects - with soft-deleted subject",
			method: "GET",
			path:   "/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("product-topic-value", 1, 2, productSchema)

				// Perform a soft-delete via the public HTTP API
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/user-topic-value", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("setup failed: could not delete subject: %v", err)
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusOK,
			wantBody:   `["product-topic-value"]`,
		},
		{
			name:   "GET /subjects - including soft-deleted",
			method: "GET",
			path:   "/subjects?deleted=true",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("product-topic-value", 1, 2, productSchema)
				// Perform a soft-delete via the public HTTP API
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/user-topic-value", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("setup failed: could not delete subject: %v", err)
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusOK,
			wantBody:   `["product-topic-value", "user-topic-value"]`,
		},

		// POST /subjects/{subject}/versions
		{
			name:       "POST /subjects/{s}/versions - register new schema",
			method:     "POST",
			path:       "/subjects/user-topic-value/versions",
			body:       `{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`,
			wantStatus: http.StatusOK,
			wantBody:   `{"id": 1}`,
		},
		{
			name:   "POST /subjects/{s}/versions - register existing schema same subject",
			method: "POST",
			path:   "/subjects/user-topic-value/versions",
			body:   `{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`,
			setup: func(r *srfake.Registry) {
				r.RegisterSchema("user-topic-value", sr.Schema{Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`})
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"id": 1}`,
		},
		{
			name:   "POST /subjects/{s}/versions - register existing schema new subject",
			method: "POST",
			path:   "/subjects/user-topic-key/versions",
			body:   `{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`,
			setup: func(r *srfake.Registry) {
				// Schema exists under a different subject, should reuse the ID
				r.RegisterSchema("user-topic-value", sr.Schema{Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`})
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"id": 1}`,
		},
		{
			name:       "POST /subjects/{s}/versions - bad request body",
			method:     "POST",
			path:       "/subjects/user-topic-value/versions",
			body:       `{"schema": "not json"}`,
			wantStatus: http.StatusUnprocessableEntity,
			wantBody:   `{"error_code": 42201, "message": "invalid AVRO schema: invalid character 'o' in literal null (expecting 'u')"}`,
		},
		{
			name:   "POST /subjects/{s}/versions - soft-deleted subject",
			method: "POST",
			path:   "/subjects/user-topic-value/versions",
			body:   `{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`,
			setup: func(r *srfake.Registry) {
				// First register a schema, then soft-delete the subject
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				// Perform a soft-delete via the public HTTP API
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/user-topic-value", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					panic("setup failed: could not delete subject")
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40401, "message": "subject \"user-topic-value\" not found"}`,
		},

		// GET /subjects/{subject}/versions
		{
			name:       "GET /subjects/{s}/versions - subject not found",
			method:     "GET",
			path:       "/subjects/no-such-subject/versions",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40401, "message": "subject \"no-such-subject\" not found"}`,
		},
		{
			name:   "GET /subjects/{s}/versions - list versions",
			method: "GET",
			path:   "/subjects/user-topic-value/versions",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("user-topic-value", 2, 2, userSchemaV2)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[1, 2]`,
		},

		// GET /subjects/{subject}/versions/{version}
		{
			name:   "GET /subjects/{s}/versions/{v} - by number",
			method: "GET",
			path:   "/subjects/user-topic-value/versions/2",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("user-topic-value", 2, 2, userSchemaV2)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"subject":"user-topic-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}"}`,
		},
		{
			name:   "GET /subjects/{s}/versions/{v} - by 'latest'",
			method: "GET",
			path:   "/subjects/user-topic-value/versions/latest",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("user-topic-value", 2, 2, userSchemaV2)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"subject":"user-topic-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\",\"default\":\"\"}]}"}`,
		},
		{
			name:   "GET /subjects/{s}/versions/{v} - latest with no active versions",
			method: "GET",
			path:   "/subjects/user-topic-value/versions/latest",
			setup: func(r *srfake.Registry) {
				// Create a subject with a schema, then hard delete the version
				// to simulate a subject with no active versions
				r.SeedSchema("user-topic-value", 1, 1, userSchema)

				// Hard delete the version to make latestVersion = 0
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/user-topic-value/versions/1?permanent=true", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("setup failed: could not delete version: %v", err)
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40401, "message": "subject \"user-topic-value\" not found"}`,
		},
		{
			name:   "GET /subjects/{s}/versions/{v} - latest on non-existent subject",
			method: "GET",
			path:   "/subjects/non-existent-subject/versions/latest",
			setup: func(r *srfake.Registry) {
				// No setup needed - subject doesn't exist
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40401, "message": "subject \"non-existent-subject\" not found"}`,
		},
		{
			name:   "GET /subjects/{s}/versions/{v} - version not found",
			method: "GET",
			path:   "/subjects/user-topic-value/versions/99",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40402, "message": "version 99 not found for \"user-topic-value\""}`,
		},

		// DELETE /subjects/{subject}
		{
			name:   "DELETE /subjects/{s} - soft delete",
			method: "DELETE",
			path:   "/subjects/user-topic-value",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				r.SeedSchema("user-topic-value", 2, 2, userSchemaV2)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[1, 2]`,
		},
		{
			name:   "DELETE /subjects/{s} - permanent delete",
			method: "DELETE",
			path:   "/subjects/user-topic-value?permanent=true",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[1]`,
		},

		// POST /subjects/{subject} (check if schema registered)
		{
			name:   "POST /subjects/{s} - schema found",
			method: "POST",
			path:   "/subjects/user-topic-value",
			body:   `{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"subject":"user-topic-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
		},
		{
			name:   "POST /subjects/{s} - schema not found",
			method: "POST",
			path:   "/subjects/user-topic-value",
			body:   `{"schema":"{\"type\":\"record\",\"name\":\"DoesNotExist\"}"}`,
			setup: func(r *srfake.Registry) {
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40403, "message": "schema not found"}`,
		},
		{
			name:   "POST /subjects/{s} - soft-deleted subject",
			method: "POST",
			path:   "/subjects/user-topic-value",
			body:   `{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
			setup: func(r *srfake.Registry) {
				// First register a schema, then soft-delete the subject
				r.SeedSchema("user-topic-value", 1, 1, userSchema)
				// Perform a soft-delete via the public HTTP API
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/user-topic-value", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					panic("setup failed: could not delete subject")
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40401, "message": "subject \"user-topic-value\" not found"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := srfake.New()
			t.Cleanup(reg.Close)
			client := &http.Client{}

			if tc.setup != nil {
				tc.setup(reg)
			}

			req, err := http.NewRequest(tc.method, reg.URL()+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Failed to execute request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("got status %d, want %d\nbody: %s", resp.StatusCode, tc.wantStatus, string(bodyBytes))
			}

			bodyBytes, _ := io.ReadAll(resp.Body)
			bodyStr := string(bodyBytes)

			if !jsonEqual(bodyStr, tc.wantBody) {
				t.Errorf("body mismatch:\ngot:  %s\nwant: %s", bodyStr, tc.wantBody)
			}
		})
	}
}

func TestSchemaHandlers(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	client := &http.Client{}
	reg.SeedSchema("user-topic", 1, 123, userSchema)

	testCases := []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "GET /schemas/ids/{id} - found",
			path:       "/schemas/ids/123",
			wantStatus: http.StatusOK,
			wantBody:   `{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
		},
		{
			name:       "GET /schemas/ids/{id} - not found",
			path:       "/schemas/ids/999",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40403, "message": "schema not found"}`,
		},
		{
			name:       "GET /schemas/ids/{id}/schema - found",
			path:       "/schemas/ids/123/schema",
			wantStatus: http.StatusOK,
			wantBody:   `"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"`,
		},
		{
			name:       "GET /schemas/ids/{id}/schema - not found",
			path:       "/schemas/ids/999/schema",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error_code": 40403, "message": "schema not found"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, reg.URL()+tc.path, http.NoBody)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("HTTP request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("got status %d, want %d", resp.StatusCode, tc.wantStatus)
			}

			bodyBytes, _ := io.ReadAll(resp.Body)
			if !jsonEqual(string(bodyBytes), tc.wantBody) {
				t.Errorf("body mismatch:\ngot:  %s\nwant: %s", string(bodyBytes), tc.wantBody)
			}
		})
	}
}

func TestConfigHandlers(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	client := &http.Client{}

	t.Run("Global config", func(t *testing.T) {
		// 1. Get initial global config
		req, _ := http.NewRequest("GET", reg.URL()+"/config", http.NoBody)
		resp, _ := client.Do(req)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !jsonEqual(string(body), `{"compatibilityLevel": "BACKWARD"}`) {
			t.Errorf("unexpected initial global config: %s", string(body))
		}

		// 2. Update global config
		putReq, _ := http.NewRequest("PUT", reg.URL()+"/config", strings.NewReader(`{"compatibility": "FORWARD"}`))
		putReq.Header.Set("Content-Type", "application/json")
		putResp, _ := client.Do(putReq)
		putResp.Body.Close()
		if putResp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status on PUT /config: %d", putResp.StatusCode)
		}

		// 3. Get updated global config
		req2, _ := http.NewRequest("GET", reg.URL()+"/config", http.NoBody)
		resp2, _ := client.Do(req2)
		body2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if !jsonEqual(string(body2), `{"compatibilityLevel": "FORWARD"}`) {
			t.Errorf("unexpected updated global config: %s", string(body2))
		}
	})

	t.Run("Subject-level config", func(t *testing.T) {
		reg.Reset()
		subject := "my-topic"

		// 1. Get subject config, should fallback to global
		req, _ := http.NewRequest("GET", reg.URL()+"/config/"+subject, http.NoBody)
		resp, _ := client.Do(req)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !jsonEqual(string(body), `{"compatibilityLevel": "BACKWARD"}`) {
			t.Errorf("unexpected initial subject config: %s", string(body))
		}

		// 2. Set subject config
		putReq, _ := http.NewRequest("PUT", reg.URL()+"/config/"+subject, strings.NewReader(`{"compatibility": "FULL"}`))
		putResp, _ := client.Do(putReq)
		putResp.Body.Close()
		if putResp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status on PUT /config/{subject}: %d", putResp.StatusCode)
		}

		// 3. Get subject config, should show specific level
		req2, _ := http.NewRequest("GET", reg.URL()+"/config/"+subject, http.NoBody)
		resp2, _ := client.Do(req2)
		body2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if !jsonEqual(string(body2), `{"compatibilityLevel": "FULL"}`) {
			t.Errorf("unexpected specific subject config: %s", string(body2))
		}

		// 4. Delete subject config
		delReq, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/config/"+subject, http.NoBody)
		delResp, _ := client.Do(delReq)
		delResp.Body.Close()
		if delResp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status on DELETE /config/{subject}: %d", delResp.StatusCode)
		}

		// 5. Get subject config again, should fall back to global
		req3, _ := http.NewRequest("GET", reg.URL()+"/config/"+subject, http.NoBody)
		resp3, _ := client.Do(req3)
		body3, _ := io.ReadAll(resp3.Body)
		resp3.Body.Close()
		if !jsonEqual(string(body3), `{"compatibilityLevel": "BACKWARD"}`) {
			t.Errorf("unexpected reverted subject config: %s", string(body3))
		}
	})
}

func TestAuthMiddleware(t *testing.T) {
	const authToken = "Bearer my-secret-token"
	reg := srfake.New(srfake.WithAuth(authToken))
	t.Cleanup(reg.Close)
	client := &http.Client{}

	testCases := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "no auth header",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong auth header",
			authHeader: "Bearer wrong-token",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "correct auth header",
			authHeader: authToken,
			wantStatus: http.StatusOK, // for GET /subjects
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", reg.URL()+"/subjects", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("HTTP request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("got status %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}

// TestClientIntegration validates that the mock registry works correctly when
// used with the actual sr.Client, covering the most common user workflows.
func TestClientIntegration(t *testing.T) {
	const subject = "client-test-topic-value"

	// This struct is used for the body of PUT requests to the config endpoint.
	type compatBody struct {
		Compatibility sr.CompatibilityLevel `json:"compatibility"`
	}

	testCases := []struct {
		name       string
		setup      func(t *testing.T, r *srfake.Registry)
		act        func(t *testing.T, cl *sr.Client) (any, error)
		wantErr    any
		wantResult any
	}{
		// --- Schema Creation ---
		{
			name: "CreateSchema - new schema",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), subject, userSchema)
			},
			wantResult: sr.SubjectSchema{
				Subject: subject,
				Version: 1,
				ID:      1,
				Schema:  userSchema,
			},
		},
		{
			name: "CreateSchema - existing schema same subject",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), subject, userSchema)
			},
			wantResult: sr.SubjectSchema{
				Subject: subject,
				Version: 1,
				ID:      1,
				Schema:  userSchema,
			},
		},
		{
			name: "CreateSchema - new version for subject",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), subject, userSchemaV2)
			},
			wantResult: sr.SubjectSchema{
				Subject: subject,
				Version: 2,
				ID:      2,
				Schema:  userSchemaV2,
			},
		},
		{
			name: "CreateSchema - existing schema different subject should reuse ID",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("another-subject", 1, 99, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), "new-subject-for-product", productSchema)
			},
			wantResult: sr.SubjectSchema{
				Subject: "new-subject-for-product",
				Version: 1,
				ID:      99,
				Schema:  productSchema,
			},
		},

		// --- Schema Retrieval ---
		{
			name: "SchemaByID - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 123, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaByID(context.Background(), 123)
			},
			wantResult: userSchema,
		},
		{
			name: "SchemaByVersion - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaByVersion(context.Background(), subject, 2)
			},
			wantResult: sr.SubjectSchema{
				Subject: subject,
				Version: 2,
				ID:      2,
				Schema:  userSchemaV2,
			},
		},
		{
			name: "LatestSchema - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaByVersion(context.Background(), subject, -1)
			},
			wantResult: sr.SubjectSchema{
				Subject: subject,
				Version: 2,
				ID:      2,
				Schema:  userSchemaV2,
			},
		},

		// --- Subject & Version Management ---
		{
			name: "Subjects - list all",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("subj-a", 1, 1, userSchema)
				r.SeedSchema("subj-b", 1, 2, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.Subjects(context.Background())
			},
			wantResult: []string{"subj-a", "subj-b"},
		},
		{
			name: "SchemaVersions - list all for subject",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SubjectVersions(context.Background(), subject)
			},
			wantResult: []int{1, 2},
		},
		{
			name: "DeleteSubject - permanent",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.DeleteSubject(context.Background(), subject, true)
			},
			wantResult: []int{1, 2}, // returns deleted versions
		},
		{
			name: "DeleteSchemaVersion - then verify not found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// Action: Delete version 2
				err := cl.DeleteSchema(context.Background(), subject, 2, sr.HardDelete)
				if err != nil {
					return nil, errors.New("delete call failed unexpectedly")
				}
				// Verify: Try to fetch the deleted version. This should fail.
				return cl.SchemaByVersion(context.Background(), subject, 2)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusNotFound},
		},
		{
			name: "DeleteSchemaVersion - latest version recalculation",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, and 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema) // Different schema for version 3
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Verify latest is version 3
				latest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest before delete: %v", err)
				}
				if latest.Version != 3 {
					return nil, fmt.Errorf("expected latest version 3, got %d", latest.Version)
				}

				// 2. Hard delete version 3 (the latest)
				err = cl.DeleteSchema(context.Background(), subject, 3, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete version 3: %v", err)
				}

				// 3. Get latest again - should now be version 2
				newLatest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest after delete: %v", err)
				}

				return newLatest.Version, nil
			},
			wantResult: 2, // Latest should now be version 2
		},
		{
			name: "DeleteSchemaVersion - delete all versions",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up a single version
				r.SeedSchema(subject, 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Hard delete the only version
				err := cl.DeleteSchema(context.Background(), subject, 1, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete version 1: %v", err)
				}

				// 2. Try to get latest - should fail since no versions exist
				return cl.SchemaByVersion(context.Background(), subject, -1)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusNotFound},
		},
		{
			name: "DeleteSchemaVersion - delete non-latest version",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, and 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Hard delete version 2 (not the latest)
				err := cl.DeleteSchema(context.Background(), subject, 2, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete version 2: %v", err)
				}

				// 2. Get latest - should still be version 3
				latest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest after delete: %v", err)
				}

				return latest.Version, nil
			},
			wantResult: 3, // Latest should still be version 3
		},
		{
			name: "DeleteSchemaVersion - soft delete latest version recalculation",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, and 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Verify latest is version 3
				latest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest before delete: %v", err)
				}
				if latest.Version != 3 {
					return nil, fmt.Errorf("expected latest version 3, got %d", latest.Version)
				}

				// 2. Soft delete version 3 (the latest)
				err = cl.DeleteSchema(context.Background(), subject, 3, sr.SoftDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to soft delete version 3: %v", err)
				}

				// 3. Get latest again - should now be version 2
				newLatest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest after soft delete: %v", err)
				}

				return newLatest.Version, nil
			},
			wantResult: 2, // Latest should now be version 2
		},
		{
			name: "DeleteSchemaVersion - soft delete all versions",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up a single version
				r.SeedSchema(subject, 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Soft delete the only version
				err := cl.DeleteSchema(context.Background(), subject, 1, sr.SoftDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to soft delete version 1: %v", err)
				}

				// 2. Try to get latest - should fail since no non-soft-deleted versions exist
				return cl.SchemaByVersion(context.Background(), subject, -1)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusNotFound},
		},
		{
			name: "DeleteSchemaVersion - soft delete non-latest version",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, and 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Soft delete version 2 (not the latest)
				err := cl.DeleteSchema(context.Background(), subject, 2, sr.SoftDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to soft delete version 2: %v", err)
				}

				// 2. Get latest - should still be version 3
				latest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest after soft delete: %v", err)
				}

				return latest.Version, nil
			},
			wantResult: 3, // Latest should still be version 3
		},

		// --- Configuration Management ---
		{
			name: "GlobalCompatibility - get and set",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Get initial global config
				results := cl.Compatibility(context.Background())
				if len(results) != 1 || results[0].Err != nil {
					return nil, errors.New("failed to get initial global compat")
				}
				if results[0].Level != sr.CompatBackward {
					return nil, errors.New("initial global compat was not BACKWARD")
				}

				// 2. Set global config to FULL
				body := compatBody{Compatibility: sr.CompatFull}
				if err := cl.Do(context.Background(), http.MethodPut, "/config", body, nil); err != nil {
					return nil, err
				}

				// 3. Get updated global config
				finalResults := cl.Compatibility(context.Background())
				if len(finalResults) != 1 || finalResults[0].Err != nil {
					return nil, errors.New("failed to get final global compat")
				}
				return finalResults[0].Level, nil
			},
			wantResult: sr.CompatFull,
		},
		{
			name: "SubjectCompatibility - get, set, and delete",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Get initial subject compat (falls back to global BACKWARD)
				results := cl.Compatibility(context.Background(), subject)
				if len(results) != 1 || results[0].Err != nil || results[0].Level != sr.CompatBackward {
					return nil, errors.New("initial subject compat did not fall back to BACKWARD")
				}

				// 2. Set subject-specific level
				body := compatBody{Compatibility: sr.CompatNone}
				if err := cl.Do(context.Background(), http.MethodPut, "/config/"+subject, body, nil); err != nil {
					return nil, errors.New("failed to set subject compatibility")
				}

				// 3. Get specific level
				results = cl.Compatibility(context.Background(), subject)
				if len(results) != 1 || results[0].Err != nil || results[0].Level != sr.CompatNone {
					return nil, errors.New("could not get specific subject compat")
				}

				// 4. Delete subject level
				if err := cl.Do(context.Background(), http.MethodDelete, "/config/"+subject, nil, nil); err != nil {
					return nil, errors.New("failed to delete subject compatibility")
				}

				// 5. Get again, should fall back to global
				finalResults := cl.Compatibility(context.Background(), subject)
				if len(finalResults) != 1 || finalResults[0].Err != nil {
					return nil, errors.New("failed to get final subject compat")
				}
				return finalResults[0].Level, nil
			},
			wantResult: sr.CompatBackward,
		},
		{
			name: "CreateSchema - direct circular reference",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// Create a schema that references itself
				selfRefSchema := sr.Schema{
					Schema: `{"type":"record","name":"SelfRef","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
					References: []sr.SchemaReference{
						{Name: "self", Subject: "self-ref", Version: 1},
					},
				}
				return cl.CreateSchema(context.Background(), "self-ref", selfRefSchema)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusUnprocessableEntity},
		},
		{
			name: "CreateSchema - indirect circular reference",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up schema A
				schemaA := sr.Schema{
					Schema: `{"type":"record","name":"SchemaA","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				r.SeedSchema("schema-a", 1, 1, schemaA)

				// Set up schema B that references schema A
				schemaB := sr.Schema{
					Schema: `{"type":"record","name":"SchemaB","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
					References: []sr.SchemaReference{
						{Name: "refA", Subject: "schema-a", Version: 1},
					},
				}
				r.SeedSchema("schema-b", 1, 2, schemaB)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// Now try to create a new version of schema A that references schema B
				// This creates: A:1 (no refs) <- B:1 (refs A:1) <- A:2 (refs B:1) = cycle
				schemaAWithCycle := sr.Schema{
					Schema: `{"type":"record","name":"SchemaA","fields":[{"name":"id","type":"string"},{"name":"b_ref","type":"string"}]}`,
					Type:   sr.TypeAvro,
					References: []sr.SchemaReference{
						{Name: "refB", Subject: "schema-b", Version: 1},
					},
				}
				return cl.CreateSchema(context.Background(), "schema-a", schemaAWithCycle)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusUnprocessableEntity},
		},
		{
			name: "CreateSchema - valid references without cycles",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up a base schema with no references
				baseSchema := sr.Schema{
					Schema: `{"type":"record","name":"Base","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				r.SeedSchema("base-schema", 1, 1, baseSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// Create a schema that references the base schema (valid, no cycle)
				derivedSchema := sr.Schema{
					Schema: `{"type":"record","name":"Derived","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
					References: []sr.SchemaReference{
						{Name: "base", Subject: "base-schema", Version: 1},
					},
				}
				return cl.CreateSchema(context.Background(), "derived-schema", derivedSchema)
			},
			wantResult: sr.SubjectSchema{
				Subject: "derived-schema",
				Version: 1,
				ID:      2,
				Schema: sr.Schema{
					Schema: `{"type":"record","name":"Derived","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
					References: []sr.SchemaReference{
						{Name: "base", Subject: "base-schema", Version: 1},
					},
				},
			},
		},
		{
			name: "CreateSchema - soft-deleted subject should fail",
			setup: func(t *testing.T, r *srfake.Registry) {
				// First register a schema, then soft-delete the subject
				r.SeedSchema(subject, 1, 1, userSchema)
				// Soft-delete the subject
				resp, err := http.DefaultClient.Do(func() *http.Request {
					req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/"+subject, http.NoBody)
					return req
				}())
				if err != nil {
					t.Fatalf("setup failed: could not delete subject: %v", err)
				}
				resp.Body.Close()
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), subject, userSchemaV2)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusNotFound},
		},
		{
			name: "CreateSchema - empty subject name should fail",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), "", userSchema)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusUnprocessableEntity},
		},
		{
			name: "CreateSchema - subject name with null byte should fail",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.CreateSchema(context.Background(), "test\x00subject", userSchema)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusUnprocessableEntity},
		},
		{
			name: "Version numbering - no reuse after latest version deletion",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Verify latest is version 3
				latest, err := cl.SchemaByVersion(context.Background(), subject, -1)
				if err != nil {
					return nil, fmt.Errorf("failed to get latest before delete: %v", err)
				}
				if latest.Version != 3 {
					return nil, fmt.Errorf("expected latest version 3, got %d", latest.Version)
				}

				// 2. Hard delete version 3 (the latest)
				err = cl.DeleteSchema(context.Background(), subject, 3, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete version 3: %v", err)
				}

				// 3. Register a new schema - should get version 4, NOT version 3
				newSchema := sr.Schema{
					Schema: `{"type":"record","name":"NewSchema","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				result, err := cl.CreateSchema(context.Background(), subject, newSchema)
				if err != nil {
					return nil, fmt.Errorf("failed to register new schema: %v", err)
				}

				return result.Version, nil
			},
			wantResult: 4, // Should skip version 3 and use version 4
		},
		{
			name: "Version numbering - no reuse after non-latest version deletion",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, 3, 4, 5 with version 5 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
				r.SeedSchema(subject, 4, 4, userSchema)   // Reuse existing schema
				r.SeedSchema(subject, 5, 5, userSchemaV2) // Reuse existing schema
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Hard delete version 3 (not the latest)
				err := cl.DeleteSchema(context.Background(), subject, 3, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete version 3: %v", err)
				}

				// 2. Register a new schema - should get version 6, NOT version 3
				newSchema := sr.Schema{
					Schema: `{"type":"record","name":"NewSchema","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				result, err := cl.CreateSchema(context.Background(), subject, newSchema)
				if err != nil {
					return nil, fmt.Errorf("failed to register new schema: %v", err)
				}

				return result.Version, nil
			},
			wantResult: 6, // Should skip version 3 and use version 6
		},
		{
			name: "Version numbering - no reuse after soft delete",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up versions 1, 2, 3 with version 3 being latest
				r.SeedSchema(subject, 1, 1, userSchema)
				r.SeedSchema(subject, 2, 2, userSchemaV2)
				r.SeedSchema(subject, 3, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Soft delete version 3 (the latest)
				err := cl.DeleteSchema(context.Background(), subject, 3, sr.SoftDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to soft delete version 3: %v", err)
				}

				// 2. Register a new schema - should get version 4, NOT version 3
				newSchema := sr.Schema{
					Schema: `{"type":"record","name":"NewSchema","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				result, err := cl.CreateSchema(context.Background(), subject, newSchema)
				if err != nil {
					return nil, fmt.Errorf("failed to register new schema: %v", err)
				}

				return result.Version, nil
			},
			wantResult: 4, // Should skip version 3 and use version 4
		},
		{
			name: "Schema ID allocation - no reuse after hard delete",
			setup: func(t *testing.T, r *srfake.Registry) {
				// Set up schemas with IDs 1, 2, 3
				r.SeedSchema("subject-1", 1, 1, userSchema)
				r.SeedSchema("subject-2", 1, 2, userSchemaV2)
				r.SeedSchema("subject-3", 1, 3, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				// 1. Hard delete schema with ID 2
				err := cl.DeleteSchema(context.Background(), "subject-2", 1, sr.HardDelete)
				if err != nil {
					return nil, fmt.Errorf("failed to delete schema with ID 2: %v", err)
				}

				// 2. Register a new schema - should get ID 4, NOT ID 2
				newSchema := sr.Schema{
					Schema: `{"type":"record","name":"NewSchema","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				}
				result, err := cl.CreateSchema(context.Background(), "new-subject", newSchema)
				if err != nil {
					return nil, fmt.Errorf("failed to register new schema: %v", err)
				}

				return result.ID, nil
			},
			wantResult: 4, // Should skip ID 2 and use ID 4
		},

		// --- Context Methods ---
		{
			name: "Contexts - list all",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 1, 2, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.Contexts(context.Background())
			},
			wantResult: []string{".", ".myctx"},
		},
		{
			name: "Contexts - with prefix filter",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 1, 2, productSchema)
				r.SeedSchema(":.other:topic-value", 1, 3, sr.Schema{
					Schema: `{"type":"record","name":"Other","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				})
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.Contexts(sr.WithParams(context.Background(), sr.ContextPrefix(".my")))
			},
			wantResult: []string{".myctx"},
		},
		{
			name: "DeleteContext - empty context",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return nil, cl.DeleteContext(context.Background(), ".myctx")
			},
		},
		{
			name: "DeleteContext - non-empty context",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return nil, cl.DeleteContext(context.Background(), ".myctx")
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusConflict},
		},
		{
			name: "Context Subjects - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
				r.SeedSchema("plain-topic", 1, 2, productSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.Subjects(sr.WithSchemaContext(context.Background(), ".myctx"))
			},
			wantResult: []string{":.myctx:topic-value"},
		},
		{
			name: "Context Subjects - empty context",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.Subjects(sr.WithSchemaContext(context.Background(), ".myctx"))
			},
			wantResult: []string{},
		},
		{
			name: "Context SchemaByID - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaByID(sr.WithSchemaContext(context.Background(), ".myctx"), 1)
			},
			wantResult: userSchema,
		},
		{
			name: "Context SchemaByID - not in context",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaByID(sr.WithSchemaContext(context.Background(), ".myctx"), 1)
			},
			wantErr: &sr.ResponseError{StatusCode: http.StatusNotFound},
		},
		{
			name: "Context SubjectsByID - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SubjectsByID(sr.WithSchemaContext(context.Background(), ".myctx"), 1)
			},
			wantResult: []string{":.myctx:topic-value"},
		},
		{
			name: "Context SchemaVersionsByID - found",
			setup: func(t *testing.T, r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				return cl.SchemaVersionsByID(sr.WithSchemaContext(context.Background(), ".myctx"), 1)
			},
			wantResult: []sr.SubjectVersion{{Subject: ":.myctx:topic-value", Version: 1}},
		},
		{
			name: "CreateSchema with context - end-to-end",
			act: func(t *testing.T, cl *sr.Client) (any, error) {
				ctx := sr.WithSchemaContext(context.Background(), ".myctx")
				return cl.CreateSchema(ctx, ":.myctx:topic-value", userSchema)
			},
			wantResult: sr.SubjectSchema{
				Subject: ":.myctx:topic-value",
				Version: 1,
				ID:      1,
				Schema:  userSchema,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := srfake.New()
			t.Cleanup(reg.Close)
			cl, err := sr.NewClient(sr.URLs(reg.URL()))
			if err != nil {
				t.Fatalf("Failed to create sr.Client: %v", err)
			}

			if tc.setup != nil {
				tc.setup(t, reg)
			}

			gotResult, gotErr := tc.act(t, cl)

			if tc.wantErr != nil {
				if gotErr == nil {
					t.Fatalf("got nil error, want %v", tc.wantErr)
				}
			} else if gotErr != nil {
				t.Fatalf("act() returned unexpected error: %v", gotErr)
			}

			// Only compare results if no error was expected
			if tc.wantErr == nil {
				if !reflect.DeepEqual(gotResult, tc.wantResult) {
					t.Errorf("act() returned unexpected result:\ngot:  %#v\nwant: %#v", gotResult, tc.wantResult)
				}
			}
		})
	}
}

// TestRaceConditionRegisterSchema tests that RegisterSchema is safe from race conditions
// where referenced schemas could be deleted between validation and registration.
func TestRaceConditionRegisterSchema(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)

	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatalf("Failed to create sr.Client: %v", err)
	}

	// Set up a referenced schema
	refSchema := sr.Schema{
		Schema: `{"type":"record","name":"RefSchema","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
	}
	reg.SeedSchema("ref-subject", 1, 1, refSchema)

	// Create a schema that references the above schema
	referencingSchema := sr.Schema{
		Schema: `{"type":"record","name":"MainSchema","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{Name: "ref", Subject: "ref-subject", Version: 1},
		},
	}

	// Test case 1: Normal registration should succeed
	id1, version1, err := reg.RegisterSchema("main-subject-1", referencingSchema)
	if err != nil {
		t.Fatalf("Normal registration failed: %v", err)
	}
	if id1 == 0 || version1 == 0 {
		t.Fatalf("Invalid registration result: id=%d, version=%d", id1, version1)
	}

	// Test case 2: Cannot delete referenced schema (should fail)
	// Try to delete the referenced schema - should fail because it's still referenced
	req, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/ref-subject/versions/1?permanent=true", http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Delete request failed: %v", err)
	}
	resp.Body.Close()

	// Delete should have failed with 409 Conflict
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("Expected delete to fail with 409 Conflict, got status %d", resp.StatusCode)
	}

	// Delete the referencing schema first
	req2, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/main-subject-1/versions/1?permanent=true", http.NoBody)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("Delete referencing schema request failed: %v", err)
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("Expected deletion of referencing schema to succeed, got status %d", resp2.StatusCode)
	}

	// Now try to delete the referenced schema - should succeed
	req3, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/ref-subject/versions/1?permanent=true", http.NoBody)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("Delete referenced schema request failed: %v", err)
	}
	resp3.Body.Close()

	if resp3.StatusCode != http.StatusOK {
		t.Errorf("Expected deletion of referenced schema to succeed after references removed, got status %d", resp3.StatusCode)
	}

	// Now try to register another schema with the same reference - should fail
	_, err = cl.CreateSchema(t.Context(), "main-subject-2", referencingSchema)
	if err == nil {
		t.Fatalf("Registration should have failed after referenced schema was deleted")
	}
	var respErr *sr.ResponseError
	if !errors.As(err, &respErr) {
		t.Fatalf("expected ResponseError, got %T", err)
	}
	if respErr.ErrorCode != 42201 || respErr.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("expected reference not found error (422, 42201), got (%d, %d)", respErr.StatusCode, respErr.ErrorCode)
	}

	// Test case 3: Verify the referenced schema was actually deleted
	_, exists := reg.GetSchema("ref-subject", 1)
	if exists {
		t.Error("Referenced schema should have been deleted")
	}

	// Verify the referencing schema was also deleted
	_, exists = reg.GetSchema("main-subject-1", 1)
	if exists {
		t.Error("Referencing schema should have been deleted")
	}

	t.Logf("Race condition test passed: reference validation prevents deletion of referenced schemas")
}

// TestReferencedByEndpoint tests the /referencedby endpoint functionality
func TestReferencedByEndpoint(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)

	// Set up a base schema that will be referenced
	baseSchema := sr.Schema{
		Schema: `{"type":"record","name":"BaseSchema","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
	}
	reg.SeedSchema("base-schema", 1, 1, baseSchema)

	// Set up schemas that reference the base schema
	refSchema1 := sr.Schema{
		Schema: `{"type":"record","name":"RefSchema1","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{Name: "base", Subject: "base-schema", Version: 1},
		},
	}
	reg.SeedSchema("ref-schema-1", 1, 2, refSchema1)

	refSchema2 := sr.Schema{
		Schema: `{"type":"record","name":"RefSchema2","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
		References: []sr.SchemaReference{
			{Name: "base", Subject: "base-schema", Version: 1},
		},
	}
	reg.SeedSchema("ref-schema-2", 1, 3, refSchema2)

	// Set up a schema that doesn't reference anything
	independentSchema := sr.Schema{
		Schema: `{"type":"record","name":"IndependentSchema","fields":[{"name":"id","type":"string"}]}`,
		Type:   sr.TypeAvro,
	}
	reg.SeedSchema("independent-schema", 1, 4, independentSchema)

	testCases := []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "Get referencedby for base schema - should return referencing schema IDs",
			path:       "/subjects/base-schema/versions/1/referencedby",
			wantStatus: http.StatusOK,
			wantBody:   `[2,3]`, // IDs of schemas that reference base-schema:1
		},
		{
			name:       "Get referencedby for schema with no references - should return empty array",
			path:       "/subjects/independent-schema/versions/1/referencedby",
			wantStatus: http.StatusOK,
			wantBody:   `[]`,
		},
		{
			name:       "Get referencedby for non-existent subject - should return 404",
			path:       "/subjects/non-existent/versions/1/referencedby",
			wantStatus: http.StatusNotFound,
			wantBody:   "",
		},
		{
			name:       "Get referencedby for non-existent version - should return 404",
			path:       "/subjects/base-schema/versions/999/referencedby",
			wantStatus: http.StatusNotFound,
			wantBody:   "",
		},
		{
			name:       "Get referencedby using 'latest' version",
			path:       "/subjects/base-schema/versions/latest/referencedby",
			wantStatus: http.StatusOK,
			wantBody:   `[2,3]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, reg.URL()+tc.path, http.NoBody)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("got status %d, want %d", resp.StatusCode, tc.wantStatus)
			}

			if tc.wantBody != "" {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("failed to read response body: %v", err)
				}

				if !jsonEqual(string(body), tc.wantBody) {
					t.Errorf("got body %s, want %s", string(body), tc.wantBody)
				}
			}
		})
	}
}

// TestDeletionWithReferences tests schema deletion behavior when schemas are referenced
func TestDeletionWithReferences(t *testing.T) {
	t.Run("Cannot delete schema that is still referenced", func(t *testing.T) {
		reg := srfake.New()
		t.Cleanup(reg.Close)

		// Set up a base schema that will be referenced
		baseSchema := sr.Schema{
			Schema: `{"type":"record","name":"BaseSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
		}
		reg.SeedSchema("base-schema", 1, 1, baseSchema)

		// Set up a schema that references the base schema
		refSchema := sr.Schema{
			Schema: `{"type":"record","name":"RefSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
			References: []sr.SchemaReference{
				{Name: "base", Subject: "base-schema", Version: 1},
			},
		}
		reg.SeedSchema("ref-schema", 1, 2, refSchema)

		// Should NOT be able to delete the referenced schema (Confluent behavior)
		req, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/base-schema/versions/1?permanent=true", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Delete request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected deletion to fail with 409 Conflict, got status %d", resp.StatusCode)
		}

		// Verify the referenced schema was NOT deleted
		_, exists := reg.GetSchema("base-schema", 1)
		if !exists {
			t.Error("Referenced schema should NOT have been deleted")
		}

		// Should be able to delete the referencing schema first
		req2, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/ref-schema/versions/1?permanent=true", http.NoBody)
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatalf("Delete referencing schema request failed: %v", err)
		}
		resp2.Body.Close()

		if resp2.StatusCode != http.StatusOK {
			t.Errorf("Expected deletion of referencing schema to succeed, got status %d", resp2.StatusCode)
		}

		// Now should be able to delete the referenced schema
		req3, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/base-schema/versions/1?permanent=true", http.NoBody)
		resp3, err := http.DefaultClient.Do(req3)
		if err != nil {
			t.Fatalf("Delete referenced schema request failed: %v", err)
		}
		resp3.Body.Close()

		if resp3.StatusCode != http.StatusOK {
			t.Errorf("Expected deletion of referenced schema to succeed after references removed, got status %d", resp3.StatusCode)
		}
	})

	t.Run("Cannot delete entire subject when it has referenced schemas", func(t *testing.T) {
		reg := srfake.New()
		t.Cleanup(reg.Close)

		// Set up schemas
		baseSchema := sr.Schema{
			Schema: `{"type":"record","name":"BaseSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
		}
		reg.SeedSchema("base-schema", 1, 1, baseSchema)

		refSchema := sr.Schema{
			Schema: `{"type":"record","name":"RefSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
			References: []sr.SchemaReference{
				{Name: "base", Subject: "base-schema", Version: 1},
			},
		}
		reg.SeedSchema("ref-schema", 1, 2, refSchema)

		// Should NOT be able to delete entire subject when it's referenced
		req, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/base-schema?permanent=true", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Delete subject request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected subject deletion to fail with 409 Conflict, got status %d", resp.StatusCode)
		}
	})

	t.Run("Cannot soft delete referenced schema", func(t *testing.T) {
		reg := srfake.New()
		t.Cleanup(reg.Close)

		// Set up schemas
		baseSchema := sr.Schema{
			Schema: `{"type":"record","name":"BaseSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
		}
		reg.SeedSchema("base-schema", 1, 1, baseSchema)

		refSchema := sr.Schema{
			Schema: `{"type":"record","name":"RefSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
			References: []sr.SchemaReference{
				{Name: "base", Subject: "base-schema", Version: 1},
			},
		}
		reg.SeedSchema("ref-schema", 1, 2, refSchema)

		// Should NOT be able to soft delete referenced schema either
		req, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/base-schema/versions/1", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Soft delete request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusConflict {
			t.Errorf("Expected soft deletion to fail with 409 Conflict, got status %d", resp.StatusCode)
		}
	})

	t.Run("Can delete schema with no references", func(t *testing.T) {
		reg := srfake.New()
		t.Cleanup(reg.Close)

		// Set up a schema with no references
		independentSchema := sr.Schema{
			Schema: `{"type":"record","name":"IndependentSchema","fields":[{"name":"id","type":"string"}]}`,
			Type:   sr.TypeAvro,
		}
		reg.SeedSchema("independent-schema", 1, 1, independentSchema)

		// Should be able to delete schema with no references
		req, _ := http.NewRequest(http.MethodDelete, reg.URL()+"/subjects/independent-schema/versions/1?permanent=true", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Delete request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected deletion to succeed, got status %d", resp.StatusCode)
		}

		// Verify the schema was deleted
		_, exists := reg.GetSchema("independent-schema", 1)
		if exists {
			t.Error("Schema should have been deleted")
		}
	})
}

func TestContextHandlers(t *testing.T) {
	testCases := []struct {
		name       string
		method     string
		path       string
		body       string
		setup      func(*srfake.Registry)
		wantStatus int
		wantBody   string
	}{
		// GET /contexts
		{
			name:   "GET /contexts - no subjects returns default context only",
			method: "GET",
			path:   "/contexts",
			setup: func(r *srfake.Registry) {
				// no setup needed
			},
			wantStatus: http.StatusOK,
			wantBody:   `["."]`,
		},
		{
			name:   "GET /contexts - plain + contexted subjects",
			method: "GET",
			path:   "/contexts",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 1, 2, productSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[".", ".myctx"]`,
		},
		{
			name:   "GET /contexts - soft-deleted contexted subject excluded",
			method: "GET",
			path:   "/contexts",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 1, 2, productSchema)
				// Soft-delete the contexted subject
				req, _ := http.NewRequest(http.MethodDelete, r.URL()+"/subjects/:.myctx:topic-value", http.NoBody)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("setup failed: could not delete subject: %v", err)
				}
				resp.Body.Close()
			},
			wantStatus: http.StatusOK,
			wantBody:   `["."]`,
		},
		{
			name:   "GET /contexts - contextPrefix filter",
			method: "GET",
			path:   "/contexts?contextPrefix=.my",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 1, 2, productSchema)
				r.SeedSchema(":.other:topic-value", 1, 3, sr.Schema{
					Schema: `{"type":"record","name":"Other","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				})
			},
			wantStatus: http.StatusOK,
			wantBody:   `[".myctx"]`,
		},
		{
			name:   "GET /contexts - offset and limit",
			method: "GET",
			path:   "/contexts?offset=1&limit=1",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
				r.SeedSchema(":.a:topic", 1, 2, productSchema)
				r.SeedSchema(":.b:topic", 1, 3, sr.Schema{
					Schema: `{"type":"record","name":"B","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				})
			},
			wantStatus: http.StatusOK,
			wantBody:   `[".a"]`, // sorted: [".", ".a", ".b"], offset=1 limit=1 → [".a"]
		},

		// DELETE /contexts/{context}
		{
			name:   "DELETE /contexts/.myctx - empty context returns 204",
			method: "DELETE",
			path:   "/contexts/.myctx",
			setup: func(r *srfake.Registry) {
				// No subjects in .myctx context
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "DELETE /contexts/.myctx - non-empty context returns error",
			method: "DELETE",
			path:   "/contexts/.myctx",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusConflict,
		},

		// GET /contexts/{context}/schemas/ids/{id}
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id} - schema in context",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
		},
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id} - schema not in context returns 404",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1",
			setup: func(r *srfake.Registry) {
				// Schema is in default context, not .myctx
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			wantStatus: http.StatusNotFound,
		},

		// GET /contexts/{context}/schemas/ids/{id}/versions
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id}/versions - versions in context",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1/versions",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[{"subject":":.myctx:topic-value","version":1}]`,
		},
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id}/versions - no versions in context returns 404",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1/versions",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			wantStatus: http.StatusNotFound,
		},

		// GET /contexts/{context}/schemas/ids/{id}/subjects
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id}/subjects - subjects in context",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[":.myctx:topic-value"]`,
		},
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id}/subjects - no subjects in context returns 404",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			wantStatus: http.StatusNotFound,
		},

		// GET /contexts/{context}/subjects
		{
			name:   "GET /contexts/.myctx/subjects - subjects in context",
			method: "GET",
			path:   "/contexts/.myctx/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
				r.SeedSchema(":.myctx:other-topic", 1, 2, productSchema)
				r.SeedSchema("plain-topic", 1, 3, sr.Schema{
					Schema: `{"type":"record","name":"Plain","fields":[{"name":"id","type":"string"}]}`,
					Type:   sr.TypeAvro,
				})
			},
			wantStatus: http.StatusOK,
			wantBody:   `[":.myctx:other-topic", ":.myctx:topic-value"]`,
		},
		{
			name:   "GET /contexts/.myctx/subjects - no subjects in context returns empty array",
			method: "GET",
			path:   "/contexts/.myctx/subjects",
			setup: func(r *srfake.Registry) {
				r.SeedSchema("plain-topic", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[]`,
		},

		// Context-prefixed operations (via middleware stripping)
		{
			name:   "POST /contexts/.myctx/subjects/{s}/versions - register schema in context",
			method: "POST",
			path:   "/contexts/.myctx/subjects/:.myctx:topic-value/versions",
			body:   `{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
			setup:  func(r *srfake.Registry) {},
			wantStatus: http.StatusOK,
			wantBody:   `{"id": 1}`,
		},
		{
			name:   "GET /contexts/.myctx/subjects/{s}/versions - list versions in context",
			method: "GET",
			path:   "/contexts/.myctx/subjects/:.myctx:topic-value/versions",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
				r.SeedSchema(":.myctx:topic-value", 2, 2, userSchemaV2)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[1, 2]`,
		},
		{
			name:   "GET /contexts/.myctx/subjects/{s}/versions/{v} - get version in context",
			method: "GET",
			path:   "/contexts/.myctx/subjects/:.myctx:topic-value/versions/1",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"subject":":.myctx:topic-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}`,
		},
		{
			name:   "DELETE /contexts/.myctx/subjects/{s} - delete subject in context",
			method: "DELETE",
			path:   "/contexts/.myctx/subjects/:.myctx:topic-value",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `[1]`,
		},
		{
			name:   "GET /contexts/.myctx/config - get global config in context",
			method: "GET",
			path:   "/contexts/.myctx/config",
			setup:  func(r *srfake.Registry) {},
			wantStatus: http.StatusOK,
			wantBody:   `{"compatibilityLevel": "BACKWARD"}`,
		},
		{
			name:   "GET /contexts/.myctx/schemas/ids/{id}/schema - get raw schema by ID in context",
			method: "GET",
			path:   "/contexts/.myctx/schemas/ids/1/schema",
			setup: func(r *srfake.Registry) {
				r.SeedSchema(":.myctx:topic-value", 1, 1, userSchema)
			},
			wantStatus: http.StatusOK,
			wantBody:   `"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := srfake.New()
			t.Cleanup(reg.Close)
			client := &http.Client{}

			if tc.setup != nil {
				tc.setup(reg)
			}

			var reqBody io.Reader = http.NoBody
			if tc.body != "" {
				reqBody = strings.NewReader(tc.body)
			}
			req, err := http.NewRequest(tc.method, reg.URL()+tc.path, reqBody)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Failed to execute request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("got status %d, want %d\nbody: %s", resp.StatusCode, tc.wantStatus, string(bodyBytes))
				return
			}

			if tc.wantBody != "" {
				bodyBytes, _ := io.ReadAll(resp.Body)
				if !jsonEqual(string(bodyBytes), tc.wantBody) {
					t.Errorf("body mismatch:\ngot:  %s\nwant: %s", string(bodyBytes), tc.wantBody)
				}
			}
		})
	}
}

// jsonEqual compares two JSON strings for semantic equality, ignoring
// whitespace and key order.
func jsonEqual(a, b string) bool {
	var j1, j2 any
	if err := json.Unmarshal([]byte(a), &j1); err != nil {
		var j2Err error
		// If a is not json, check if b is also not json.
		if j2Err = json.Unmarshal([]byte(b), &j2); j2Err != nil {
			// Both are not json, compare as plain text.
			return strings.TrimSpace(a) == strings.TrimSpace(b)
		}
		// a is not json but b is, they are not equal.
		return false
	}
	// a is json, b must also be json.
	if err := json.Unmarshal([]byte(b), &j2); err != nil {
		return false
	}
	return reflect.DeepEqual(j1, j2)
}

// TestErrorHandling verifies that the mock registry returns proper sr.ResponseError objects
// that clients can use for error checking.
func TestErrorHandling(t *testing.T) {
	registry := srfake.New()
	t.Cleanup(registry.Close)

	cl, err := sr.NewClient(sr.URLs(registry.URL()))
	if err != nil {
		t.Fatalf("Failed to create sr.Client: %v", err)
	}

	// Test case 1: Subject not found error
	t.Run("subject not found error", func(t *testing.T) {
		registry.Reset()

		// Create and soft-delete a subject
		registry.SeedSchema("test", 1, 1, sr.Schema{
			Schema: `{"type": "string"}`,
			Type:   sr.TypeAvro,
		})

		// Soft-delete the subject by making an HTTP request
		req, _ := http.NewRequest("DELETE", registry.URL()+"/subjects/test", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to delete subject: %v", err)
		}
		resp.Body.Close()

		// Now try to register to the soft-deleted subject using the client
		_, err = cl.CreateSchema(t.Context(), "test", sr.Schema{
			Schema: `{"type": "number"}`,
			Type:   sr.TypeAvro,
		})

		if err == nil {
			t.Fatalf("expected error, got nil")
		}

		// Test errors.As with ResponseError (the client-facing error type)
		var respErr *sr.ResponseError
		if !errors.As(err, &respErr) {
			t.Errorf("errors.As failed: expected ResponseError, got %T", err)
		} else {
			if respErr.StatusCode != http.StatusNotFound {
				t.Errorf("expected HTTP status %d, got %d", http.StatusNotFound, respErr.StatusCode)
			}
			if respErr.ErrorCode != 40401 {
				t.Errorf("expected SR error code %d, got %d", 40401, respErr.ErrorCode)
			}
		}

		// Test that the error message is meaningful
		if err.Error() == "" {
			t.Error("error message should not be empty")
		}
	})

	// Test case 2: Invalid schema error
	t.Run("invalid schema error", func(t *testing.T) {
		registry.Reset()

		// Try to register an invalid schema using the client
		_, err := cl.CreateSchema(t.Context(), "test", sr.Schema{
			Schema: `invalid json`,
			Type:   sr.TypeAvro,
		})

		if err == nil {
			t.Fatalf("expected error, got nil")
		}

		// Test errors.As with ResponseError (the client-facing error type)
		var respErr *sr.ResponseError
		if !errors.As(err, &respErr) {
			t.Errorf("errors.As failed: expected ResponseError, got %T", err)
		} else {
			if respErr.StatusCode != http.StatusUnprocessableEntity {
				t.Errorf("expected HTTP status %d, got %d", http.StatusUnprocessableEntity, respErr.StatusCode)
			}
			if respErr.ErrorCode != 42201 {
				t.Errorf("expected SR error code %d, got %d", 42201, respErr.ErrorCode)
			}
		}

		// Test that the error message is meaningful
		if err.Error() == "" {
			t.Error("error message should not be empty")
		}
	})
}
