"""Backup / restore for the project database (Docker).
Run from the repo root:
  python db/scripts/db_backup.py backup
  python db/scripts/db_backup.py restore db/backups/<file>.dump [target_db]
"""
import os
import pathlib
import subprocess
import sys
from datetime import datetime
from urllib.parse import urlparse

ROOT = pathlib.Path(__file__).resolve().parents[2]
BACKUP_DIR = ROOT / "db" / "backups"
SERVICE = "postgres"


def read_db_url():
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        key = line.split("=", 1)[0].strip()
        if key in ("DATABASE_URL", "URL_DATABASE"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("DATABASE_URL not found in .env")


_url = urlparse(read_db_url())
DB_USER = _url.username
DB_NAME = _url.path.lstrip("/")


def dc(*args, check=True):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=check)


def backup():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    for ext, extra in (("dump", ["-Fc"]), ("sql", ["--no-owner", "--no-privileges"])):
        tmp = f"/tmp/backup.{ext}"
        dc("exec", "-T", SERVICE, "pg_dump", "-U", DB_USER, "-d", DB_NAME, *extra, "-f", tmp)
        dc("cp", f"{SERVICE}:{tmp}", f"db/backups/{DB_NAME}_{stamp}.{ext}")
    print("Backup done -> db/backups/")


def restore(dump_file, target_db=None):
    target_db = target_db or DB_NAME
    rel = os.path.relpath(dump_file, ROOT).replace("\\", "/")
    dc("exec", "-T", SERVICE, "psql", "-U", DB_USER, "-d", "postgres",
       "-c", f'CREATE DATABASE "{target_db}";', check=False)
    dc("cp", rel, f"{SERVICE}:/tmp/restore.dump")
    dc("exec", "-T", SERVICE, "pg_restore", "-U", DB_USER, "-d", target_db,
       "--clean", "--if-exists", "--no-owner", "/tmp/restore.dump")
    print("Restored into", target_db)


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["backup"]:
        backup()
    elif args[:1] == ["restore"] and len(args) >= 2:
        restore(args[1], args[2] if len(args) > 2 else None)
    else:
        print(__doc__)
        
        ##Athar