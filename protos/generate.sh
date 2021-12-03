#!/bin/bash
for lang in go python; do
  docker run --rm -v "${PWD}/protos":/defs namely/protoc-all -f task.proto -l "${lang}" -o .
done