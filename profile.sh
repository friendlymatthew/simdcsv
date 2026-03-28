#!/usr/bin/env bash

set -euo pipefail

cargo b --release --bin parse_hits_100 && samply record ./target/release/parse_hits_100
