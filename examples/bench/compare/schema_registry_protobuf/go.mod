module srprotobench

go 1.27.0

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.15.1
	github.com/twmb/franz-go/pkg/sr v1.8.0
	github.com/twmb/franz-go/pkg/sr/serde/protobuf v0.0.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.22.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.14.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.7.2 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jhump/protoreflect v1.17.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto v0.0.0-20240325203815-454cdb8f5daa // indirect
)

// This example is never published, so it builds the two packages under review
// straight from the tree rather than from a tag.
replace (
	github.com/twmb/franz-go/pkg/sr => ../../../../pkg/sr
	github.com/twmb/franz-go/pkg/sr/serde/protobuf => ../../../../pkg/sr/serde/protobuf
)
