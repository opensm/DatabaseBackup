from lib.PostgresBackup import PostgresDumps

p = PostgresDumps()
p.postgres_dump(db_config="nccc", params="--verbose --clean --create")
p.pg_basedump(db_config='nccc', params="-Ft  -z -R -P ")
