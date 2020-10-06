# Connecting to a Kafka cluster

In this directory you'll find some examples demonstrating how to create a client for the various supported Kafka authentication options.

## SASL Support

Supported SASL mechanisms are:

- Kerberos / GSSAPI
- OAuth
- Plain
- SCRAM (SHA256 + SHA512)

## TLS Support

This client does not provide any TLS on its own, however it does provide a Dialer option to set how connections should be dialed to brokers. You can use the dialer to dial TLS as necessary, with any specific custom TLS configuration you desire.
