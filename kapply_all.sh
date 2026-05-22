#!/bin/bash

# deploys all pods in all directories using kubectl apply -f services.yaml

echo " Current directory: $(pwd)"
echo ""

# loop through each directory (non-recursive)
for dir in */ ; do
    [ -d "$dir" ] || continue

    echo " Entering: $dir"

    if [ -f "${dir}/services.yaml" ]; then
        echo " Found services.yaml → running..."
        (cd "$dir" && kubectl apply -f services.yaml)
    else
        echo " services.yaml not found in $dir"
    fi

    echo "-----------------------------"
done

echo " Done processing all directories"