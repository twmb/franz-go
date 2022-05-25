#!/bin/bash

# call go mod tidy on all the examples
# you should call it from the same directory as this script

set -euo pipefail

for sumfile in $(find . -name 'go.sum' -print0 | xargs -0)
do
    sumdir=$(dirname "$sumfile")
    cd "$sumdir"
    echo "$sumdir"
    go get -u ./...; go mod tidy -go=1.18
    cd -    
done
