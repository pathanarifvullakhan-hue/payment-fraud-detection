#!/usr/bin/env bash
# Acceptance #6: search logs and the database for anything that looks like a full card number.
# Usage: ./scripts/check-no-card-numbers.sh /path/to/AnypointStudio/.../logs
LOGDIR="${1:-.}"
echo "Searching logs in $LOGDIR for 13+ digit numbers (should print nothing):"
grep -rE "[0-9]{13,}" "$LOGDIR" --include="*.log" || echo "  none found - good"
echo
echo "Masked cards stored in DB (should all look like XXXXXXXXXXXX4417):"
docker exec mysql mysql -uroot -proot -e "SELECT DISTINCT masked_card FROM frauddb.fraud_events;" 2>/dev/null
