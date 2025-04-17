Handling AutoCommitMarks with MarkCommitOffsets method
===

This contains an example of handling auto commit of marked offsets.

When using AutoCommitMarks option, it's required to mark offsets that going to be commited.
It can be configured also with AutoCommitInterval option to indicate the interval.
Some projects use MarkCommitRecords while lib has also MarkCommitOffsets method.
It does the same logic, but stores only offsets.

Run `go run .` in this directory to see the consumer output!
