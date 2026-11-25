# Backup, Recovery & Deployment

Database: `food_waste_db` (schema `food_waste`), running in the `postgres` service of `docker-compose.yaml`.

## Requirements
- Docker Desktop (running), Git, Python 3
- `python -m pip install sqlalchemy psycopg2-binary python-dotenv`
- A `.env` file created from `.env.example` (never commit `.env`)

## Path A: Rebuild from the source files (normal setup)
```
git pull origin main
docker compose up -d
docker compose ps
python db/config/setup_database.py
```
`postgres` must be `healthy` before running the script. Run it on a fresh database only.

If you get `No module named 'psycopg'`, change `DATABASE_URL` in your local `.env` to start with `postgresql+psycopg2://`.

## Path B: Restore from a backup
```
python db/scripts/db_backup.py restore db/backups/<file>.dump
```
To test without touching the real database, add a target name:
```
python db/scripts/db_backup.py restore db/backups/<file>.dump food_waste_test
```
  ##Athar 
Always restore from the `.dump` file. The `.sql` file is for reading only.

## Create a new backup
```
python db/scripts/db_backup.py backup
```
Creates a `.dump` (for restore) and a `.sql` (readable) file in `db/backups/`.

## Verify
```
docker compose exec -T postgres psql -U airflow -d food_waste_db -c "SELECT 'activity' t, count(*) FROM food_waste.activity UNION ALL SELECT 'commodity', count(*) FROM food_waste.commodity UNION ALL SELECT 'country', count(*) FROM food_waste.country UNION ALL SELECT 'method', count(*) FROM food_waste.method UNION ALL SELECT 'observation', count(*) FROM food_waste.observation UNION ALL SELECT 'observation_activity', count(*) FROM food_waste.observation_activity UNION ALL SELECT 'source', count(*) FROM food_waste.source;"
```

| Table | Rows |
|---|---|
| activity | 52 |
| commodity | 196 |
| country | 142 |
| method | 11 |
| observation | 31164 |
| observation_activity | 40436 |
| source | 1216 |

   ##Athar
## Test results
- Backup created with `db_backup.py backup` (`.dump` + `.sql`).
- The `.dump` was restored into a separate database, `food_waste_test`.
- Row counts matched the original `food_waste_db` in all 7 tables (see the table above).
- Foreign key checks after `setup_database.py`: all 0.

## Notes
- Back up `food_waste_db` only. Do not touch the Airflow database.
- Use `food_waste.<table>` in queries (the tables are in the `food_waste` schema, not `public`).
- Port 5432 busy? Stop the other PostgreSQL service first.

   ##Athar