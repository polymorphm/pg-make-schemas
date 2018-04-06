#!/bin/bash

set -e

cd -- "${0%/*}"

git --version >/dev/null

rev="$(git rev-parse --verify HEAD 2>/dev/null || true)"

if [ "x$rev" == "x" ]
then
    echo "{no-git-rev}"
    
    exit
fi

tag="$(git tag --points-at HEAD 2>/dev/null || true)"
dirty="$(git status -s 2>/dev/null || true)"

echo "${tag:+$tag+}$rev${dirty:+"{dirty}"}"
