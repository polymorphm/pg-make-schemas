#!/bin/bash

set -e

cd -- "${0%/*}"

git describe --dirty --long --always 2>/dev/null ||
    cat git-describe.txt 2>/dev/null ||
    echo "{no-git-rev}"

# vi:ts=2:sw=2:et
