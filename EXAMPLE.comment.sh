#!/bin/bash

set -e

cd -- "${0%/*}"

git describe --long --always --dirt 2>/dev/null ||
    cat git-descibe.txt 2>/dev/null ||
    echo "{no-git-rev}"

# vi:ts=2:sw=2:et
