from lib.PostgresBackup import PostgresDumps
import sys
import getopt

p = PostgresDumps()
p.postgres_dump(db_config="nccc", params="--verbose --clean --create")
p.pg_basedump(db_config='nccc', params="-Ft  -z -R -P ")


def useage():
    print("%s -h \t#帮助文档" % sys.argv[0])
    print("%s -b #相关的数据库配置 \t#导入hive端启动" % sys.argv[0])
    print("%s -r #相关同步配置 \t#kafka消费端启动" % sys.argv[0])
