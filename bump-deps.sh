#!/bin/bash

# call go mod tidy on all the examples
# you should call it from the same directory as this script

set -euo pipefail

minlang="1.23.8"
maxlang="1.24.2"
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
    go get -u ./...; go mod tidy $lang
    cd - >/dev/null
done
