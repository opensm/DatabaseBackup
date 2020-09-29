from lib.PostgresBackup import PostgresDumps

p = PostgresDumps()
p.postgres_dump(db_config="nccc", params="--verbose --clean --create")
