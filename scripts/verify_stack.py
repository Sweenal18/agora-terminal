#!/usr/bin/env python3
"""
Verify all local infrastructure services are up and reachable.
Run after: docker compose -f infra/docker/docker-compose.yml up -d
"""

import socket
import sys
import urllib.request
import urllib.error

CHECKS = [
    {"name": "Kafka",               "type": "tcp",  "host": "localhost", "port": 9092},
    {"name": "MinIO (S3 API)",      "type": "http", "url": "http://localhost:9000/minio/health/live"},
    {"name": "MinIO (Console)",     "type": "http", "url": "http://localhost:9001"},
    {"name": "QuestDB (REST)",      "type": "http", "url": "http://localhost:9003/"},
    {"name": "QuestDB (PG wire)",   "type": "tcp",  "host": "localhost", "port": 8812},
    {"name": "PostgreSQL",          "type": "tcp",  "host": "localhost", "port": 5432},
    {"name": "Schema Registry",     "type": "http", "url": "http://localhost:8081/subjects"},
]

GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"
BOLD  = "\033[1m"

def check_tcp(host, port, timeout=3.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def check_http(url, timeout=5.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status < 500
    except urllib.error.HTTPError as e:
        return e.code < 500
    except Exception:
        return False

def run_checks():
    print(f"\n{BOLD}Agora Terminal — Stack Verification{RESET}")
    print("-" * 40)
    all_ok = True
    for c in CHECKS:
        ok = check_tcp(c["host"], c["port"]) if c["type"] == "tcp" else check_http(c["url"])
        status = f"{GREEN}OK  {RESET}" if ok else f"{RED}FAIL{RESET}"
        print(f"  [{status}]  {c['name']}")
        if not ok:
            all_ok = False
    print("-" * 40)
    if all_ok:
        print(f"\n{GREEN}{BOLD}All services healthy. You are ready to build.{RESET}\n")
    else:
        print(f"\n{RED}{BOLD}Some services are not reachable.{RESET}")
        print("Run: docker compose -f infra/docker/docker-compose.yml ps\n")
    return all_ok

if __name__ == "__main__":
    sys.exit(0 if run_checks() else 1)
