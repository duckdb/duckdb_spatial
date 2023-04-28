#!/bin/bash

# Usage: ./extension-upload.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest>
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <s3_bucket>           : S3 bucket to upload to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")

set -e

ext="build/release/extension/$1/$1.duckdb_extension"

# compress extension binary
gzip < $ext > "$1.duckdb_extension.gz"

# upload compressed extension binary to S3
aws s3 cp $1.duckdb_extension.gz s3://$5/$1/$2/$3/$4/$1.duckdb_extension.gz --acl public-read

if [ $6 = 'true']
then
  aws s3 cp $1.duckdb_extension.gz s3://$5/$1/latest/$3/$4/$1.duckdb_extension.gz --acl public-read
fi
# also uplo