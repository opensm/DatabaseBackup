from lib.PostgresBackup import PostgresDumps

p = PostgresDumps()
p.get_database_list(db_config="nccc")
