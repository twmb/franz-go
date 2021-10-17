module manual_committing

go 1.16

require (
	github.com/twmb/franz-go v1.1.4
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20211016003631-fbf9239e2698
)

replace github.com/twmb/franz-go => ../..

replace github.com/twmb/franz-go/pkg/kadm => ../../pkg/kadm
