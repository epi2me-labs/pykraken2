#!/bin/bash

# Copyright 2013-2021, Derrick Wood <dwood@cs.jhu.edu>
#
# This file is part of the Kraken 2 taxonomic sequence classification system.
#
#
# Modified by cwright for use in pykraken2. This version always uses the CMakeLists
# to build the project since the included Make files assume various things about
# the build environment.

set -e

VERSION="2.1.2"

if [ -z "$1" ] || [ -n "$2" ]
then
  echo "Usage: $(basename $0) KRAKEN2_DIR"
  exit 64
fi

if [ "$1" = "KRAKEN2_DIR" ]
then
  echo "Please replace \"KRAKEN2_DIR\" with the name of the directory"
  echo "that you want to install Kraken 2 in."
  exit 1
fi

# Perl cmd used to canonicalize dirname - "readlink -f" doesn't work
# on OS X.
export KRAKEN2_DIR=$(perl -MCwd=abs_path -le 'print abs_path(shift)' "$1")

mkdir -p "$KRAKEN2_DIR"

rm -rf build
mkdir build 
cd build 
cmake ..
make
cd ..
for file in build_db classify dump_table estimate_capacity lookup_accession_numbers; do
    cp build/src/$file $KRAKEN2_DIR
done;

for file in scripts/*
do
  perl -pl -e 'BEGIN { while (@ARGV) { $_ = shift; ($k,$v) = split /=/, $_, 2; $H{$k} = $v } }'\
           -e 's/#####=(\w+)=#####/$H{$1}/g' \
           "KRAKEN2_DIR=$KRAKEN2_DIR" "VERSION=$VERSION" \
           < "$file" > "$KRAKEN2_DIR/$(basename $file)"
  if [ -x "$file" ]
  then
    chmod +x "$KRAKEN2_DIR/$(basename $file)"
  fi
done
