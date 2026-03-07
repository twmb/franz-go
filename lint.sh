#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
failed=0

modules=(
	.
	pkg/kadm
	pkg/kfake
	pkg/kmsg
	pkg/sr
	pkg/sasl/kerberos
)

for mod in "${modules[@]}"; do
	echo "==> linting $mod"
	if ! (cd "$root/$mod" && golangci-lint run --timeout=5m "$@"); then
		failed=1
	fi
	echo
done

if [ "$failed" -ne 0 ]; then
	echo "FAIL: lint errors found"
	exit 1
fi
echo "OK: all modules passed"
