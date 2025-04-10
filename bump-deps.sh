#!/bin/bash

# call go mod tidy on all the examples
# you should call it from the same directory as this script

set -euo pipefail

minlang="1.21"

echo "Fetching latest stable Go version..."
latest_go_version_full=$(curl -s https://go.dev/VERSION?m=text)
if [[ -z "$latest_go_version_full" ]]; then
    echo "Error: Could not fetch the latest Go version from go.dev. Exiting."
    exit 1
fi
maxlang="${latest_go_version_full#go}"
if [[ -z "$maxlang" ]]; then
    echo "Error: Could not parse the Go version string '$latest_go_version_full'. Exiting."
    exit 1
fi
echo "Latest stable Go version detected: $maxlang"

for modfile in $(find . -name 'go.mod' -print0 | xargs -0)
do
    moddir=$(dirname "$modfile")
    cd "$moddir"
    echo "$moddir"
    filelang="$(grep "^go " go.mod | tr -d 'go \n')"
    lang=" -go=$minlang"
    if [[ "$filelang" > "$minlang" ]]; then
        lang=" -go=$filelang"
    fi
    if [[ $(pwd) == *"/franz-go/examples/"* ]]; then
        lang=" -go=$maxlang"
    fi
    go get -u ./...; go mod tidy "${lang}"
    cd - >/dev/null
done
