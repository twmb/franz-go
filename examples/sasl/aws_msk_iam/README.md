Connecting via `AWS_MSK_IAM`
===

This example shows connecting via `AWS_MSK_IAM` and printing metadata
information about your MSK cluster.

By default, this will use a credential file at either
`AWS_SHARED_CREDENTIALS_FILE`, if the environment variable exists, or at
`$HOME/.aws/credentials`.

As well, this will default to the profile specified by the `AWS_PROFILE`
environment variable if it exists, otherwise to `default`.

Seed brokers are required; to find them, navigate to your
[MSK cluster](https://console.aws.amazon.com/msk/home),
click "View client information", and then copy & paste the IAM bootstrap
servers for the `-brokers` flag (note that this will have a trailing newline,
which is fine if you run this with `go run . -brokers <paste>`.

## Flags

`-brokers` specifies the seed brokers of your MSK cluster

`-creds-file` overrides the default credentials file

`-creds-profile` overrides the default credentials profile
