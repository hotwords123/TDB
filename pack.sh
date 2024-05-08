#!/bin/bash

branch_name=$(git rev-parse --abbrev-ref HEAD)
base_commit=$(git merge-base tdb/master HEAD)

echo "Branch name: $branch_name"
echo "Base commit: $base_commit"

# Make a tarball of the files that have changed since the base commit
git diff --name-only "$base_commit" HEAD | xargs tar -cf "$branch_name".tar

# Make a diff file of the changes
git diff "$base_commit" HEAD > "$branch_name".diff

# Write the base commit to a file
echo "$base_commit" > BASE_COMMIT

# Zip the tarball and patch file
zip -m "$branch_name".zip "$branch_name".tar "$branch_name".diff BASE_COMMIT
