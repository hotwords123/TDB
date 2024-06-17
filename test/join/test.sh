#!/bin/bash

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

data_dir=./db.out
out_dir=./sql.out
exe=$(realpath ../../build/bin/server)

function title {
    echo -e "\033[1;34m============ $1 ============\033[0m"
}

title "Cleaning database"
rm -rf "$data_dir"
mkdir -p "$data_dir" "$out_dir"

for file in ./*.sql; do
    title "Testing $file"
    (
        cat "$file"
        echo "bye"
    ) | (
        cd "$data_dir"
        "$exe" -P cli
    ) > "$out_dir/${file%.sql}.out"
done
