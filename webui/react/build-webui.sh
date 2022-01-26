#!/bin/sh

set -e

rm -rf build
npm run build
# tar the build directory so it is cacheable
tar -cf build.tar build
