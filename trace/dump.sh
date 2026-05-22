
#!/bin/bash

OUTPUT_DIR="redis_dump"
POD="redis-7c46f4654d-zvqb9"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo " Scanning and dumping keys inside the pod..."

# Execute the loop inside the pod to eliminate kubectl API overhead,
# then stream a tarball of the results directly back to the host.
kubectl exec "$POD" -- sh -c '
    WORK_DIR=$(mktemp -d)
    cd "$WORK_DIR"

    redis-cli --scan --pattern "job:*-f?-*" | while read key; do
        safe_key=$(echo "$key" | tr ":" "_")
        
        # Write directly to file to handle multi-line JSON safely
        redis-cli GET "$key" > "${safe_key}.json"

        # Delete the file if it is empty
        if [ ! -s "${safe_key}.json" ]; then
            rm "${safe_key}.json"
        fi
    done

    # Archive the files and stream to stdout
    tar -cf - . 2>/dev/null
    
    # Clean up container filesystem
    cd /
    rm -rf "$WORK_DIR"
' | tar -xf - -C "$OUTPUT_DIR"

echo " Dump complete in /$OUTPUT_DIR folder"

#flush the keys after dumping
kubectl exec -it "$POD" -- redis-cli FLUSHALL
kubectl exec -it redis-7c46f4654d-zvqb9 -- redis-cli FLUSHALL
echo " Redis keys flushed after dumping"
