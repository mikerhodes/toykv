#!/bin/bash
echo "Running memtable size estimation tests..."
cargo test memtable::tests --lib
echo "Tests completed!"