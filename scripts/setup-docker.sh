#!/usr/bin/env bash
# Starts MySQL + MailHog in Docker and creates the database. Safe to run again.
# Usage (from project root):  ./scripts/setup-docker.sh
set -e
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running. Open the Docker Desktop app, wait for the whale icon to stop animating, then run this again."
  exit 1
fi

# MySQL
if docker ps -a --format '{{.Names}}' | grep -qx mysql; then
  docker start mysql >/dev/null
else
  docker run -d --name mysql -p 3307:3306 -e MYSQL_ROOT_PASSWORD=root mysql:8 >/dev/null
fi

# MailHog (fake mail server). --platform is needed on Apple Silicon (M1/M2/M3/M4); harmless on Intel.
if docker ps -a --format '{{.Names}}' | grep -qx mailhog; then
  docker start mailhog >/dev/null
else
  docker run -d --name mailhog --platform linux/amd64 -p 1025:1025 -p 8025:8025 mailhog/mailhog >/dev/null
fi

echo "Waiting for MySQL to be ready (first start can take 30-60 seconds)..."
for i in $(seq 1 60); do
  if docker exec mysql mysqladmin ping -uroot -proot --silent >/dev/null 2>&1; then break; fi
  sleep 2
done

docker exec -i mysql mysql -uroot -proot < src/main/resources/sql/schema.sql
echo
echo "Tables in frauddb:"
docker exec mysql mysql -uroot -proot -e "USE frauddb; SHOW TABLES;" 2>/dev/null
echo
echo "Done. MailHog inbox: http://localhost:8025"
