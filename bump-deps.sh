#!/bin/bash

# call go mod tidy on all the examples
# you should call it from the same directory as this script

set -euo pipefail

minlang="1.24.0"
maxlang="1.25.0"
for modfile in $(find . -name 'go.mod' -print0 | xargs -0)
do
    moddir=$(dirname "$modfile")
    pushd "$moddir" > /dev/null
    echo "$moddir"
    filelang="$(go mod edit -json | jq -r .Go)"
    lang="$minlang"
    if [[ "$filelang" > "$minlang" ]]; then
        lang="$filelang"
    fi
    if [[ $(pwd) == *"/franz-go/examples/"* ]]; then
        lang="$maxlang"
    fi
    go get -u ./...; go mod edit -go=$lang; go mod tidy
    popd > /dev/null
done
