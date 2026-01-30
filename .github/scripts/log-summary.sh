#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="$1"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file not found: $LOG_FILE"
    exit 1
fi

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

INSIDE_ERROR_BLOCK=0
CURRENT_TEST=""
BLOCK=""

while IFS= read -r line; do
    if [[ $INSIDE_ERROR_BLOCK -eq 0 ]]; then

        if [[ "$line" =~ ^===[[:space:]]+RUN[[:space:]]+([^[:space:]]+) ]]; then
            CURRENT_TEST="${BASH_REMATCH[1]}"
        fi

        if [[ "$line" == *"Error Trace:"* ]] || \
           [[ "$line" == *"Error:"* ]] || \
           [[ "$line" == *"FAIL:"* ]] || \
           [[ "$line" == *"--- FAIL:"* ]]; then
            INSIDE_ERROR_BLOCK=1
            BLOCK=""
            [ -n "$CURRENT_TEST" ] && BLOCK="Test: $CURRENT_TEST"$'\n'
            BLOCK+="$line"$'\n'
        fi

    elif [[ $INSIDE_ERROR_BLOCK -eq 1 ]]; then
        if [[ -z "$line" ]] || [[ "$line" =~ ^=== ]]; then
            echo "===================" >> "$TMPFILE"
            echo "$BLOCK" >> "$TMPFILE"
            echo "===================" >> "$TMPFILE"
            echo "" >> "$TMPFILE"
            INSIDE_ERROR_BLOCK=0
            CURRENT_TEST=""
            BLOCK=""
        else
            BLOCK+="$line"$'\n'
        fi
    fi
done < "$LOG_FILE"

# Save current error block if log file ended.
if [[ $INSIDE_ERROR_BLOCK -eq 1 && -n "$BLOCK" ]]; then
    echo "=== FAILED TEST ===" >> "$TMPFILE"
    echo "$BLOCK" >> "$TMPFILE"
    echo "===================" >> "$TMPFILE"
fi

if [ ! -s "$TMPFILE" ]; then
    echo "No failed tests found."
    exit 0
fi

cat "$TMPFILE"
exit 1