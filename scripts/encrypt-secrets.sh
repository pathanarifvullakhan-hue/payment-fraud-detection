#!/usr/bin/env bash
# Encrypts the 3 secrets and writes src/main/resources/secure-dev.yaml for you.
# Usage (from project root):  ./scripts/encrypt-secrets.sh ~/Downloads/secure-properties-tool.jar
# The key below MUST match the VM argument you give Studio:  -M-Dsecure.key=mySecretKey
set -e
cd "$(dirname "$0")/.."
JAR="${1:-$HOME/Downloads/secure-properties-tool.jar}"
KEY="${SECURE_KEY:-mySecretKey}"
DB_PASSWORD='ChangeMe!123'   # same password that schema.sql gives the fraud_app user

if [ ! -f "$JAR" ]; then
  echo "Cannot find $JAR"
  echo "Download secure-properties-tool.jar (see README step 5) and pass its path as the first argument."
  exit 1
fi
command -v java >/dev/null || { echo "Java not found. Install JDK 17 first (README step 1)."; exit 1; }

enc() {
  java -cp "$JAR" com.mulesoft.tools.SecurePropertiesTool string encrypt Blowfish CBC "$KEY" "$1" | tail -n 1 | tr -d '\r\n'
}

DB_ENC="$(enc "$DB_PASSWORD")"
USER_ENC="$(enc 'x')"
PASS_ENC="$(enc 'x')"
[ -n "$DB_ENC" ] && [ -n "$USER_ENC" ] && [ -n "$PASS_ENC" ] || { echo "Encryption produced empty output - check the jar path and Java version."; exit 1; }

cat > src/main/resources/secure-dev.yaml <<YAML
db:
  password: "![${DB_ENC}]"
smtp:
  user: "![${USER_ENC}]"
  password: "![${PASS_ENC}]"
YAML
echo "Wrote src/main/resources/secure-dev.yaml"
echo "Now add this in Studio (Run Configurations -> Arguments -> VM arguments):  -M-Dsecure.key=${KEY}"
