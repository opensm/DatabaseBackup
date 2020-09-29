from lib.PostgresBackup import PostgresDumps
import sys
import getopt
from collections import Counter
from lib.setting import BACKUP_PARAMS, BASE_BACKUP_PARAMS


def useage():
    print("%s -h \t#帮助文档" % sys.argv[0])
    print("%s -b #相关的数据库配置 \t#导入hive端启动" % sys.argv[0])
    print("%s -r #相关同步配置 \t#kafka消费端启动" % sys.argv[0])


def main():
    if len(sys.argv) == 1:
        useage()
        sys.exit()
    try:
        options, args = getopt.getopt(
            sys.argv[1:],
            "b:r:p:"
        )
    except getopt.GetoptError:
        print("%s -h" % sys.argv[0])
        sys.exit(1)
    command_dict = dict(options)
    # 帮助
    if command_dict.keys() == ['-h']:
        useage()
        sys.exit()
    # 获取监控项数据
    elif command_dict.keys() == ['-b']:
        p = PostgresDumps()
        config_key = command_dict.get("-b")
        p.postgres_dump(db_config=config_key, params=BACKUP_PARAMS)
        p.pg_basedump(db_config=config_key, params=BASE_BACKUP_PARAMS)
    elif Counter(command_dict.keys()) == ['-r', '-p']:


    else:
        useage()
        sys.exit(1)
