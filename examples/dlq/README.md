Handling DLQ (Dead Letter Queue)
===

This contains an example of handling DLQ to make safe and secure message delivering.

A DLQ is a special type of topic that stores messages that a system cannot process
due to excessive errors. Each timed out or reached max retry record
need to be reviewed by team or by automatic services to eliminate system errors.

Run `go run .` in this directory to see the consumer output!
