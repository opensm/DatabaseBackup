# -*- coding: utf-8 -*-
from lib.PostgresBackup import PostgresDumps
import sys
import getopt
from collections import Counter
from lib.setting import BACKUP_PARAMS, BASE_BACKUP_PARAMS


def useage():
    print("%s -h \t#帮助文档" % sys.argv[0])
    print("%s -b #备份postgres的配置 \t#备份postgres数据" % sys.argv[0])
    print("%s -r [相关同步配置] -p [推送匹配字符，绝对路径] \t#手动推送文件" % sys.argv[0])


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
    p = PostgresDumps()
    if command_dict.keys() == ['-h']:
        useage()
        sys.exit()
    # 获取监控项数据
    elif command_dict.keys() == ['-b']:
        config_key = command_dict.get("-b")
        p.postgres_dump(db_config=config_key, params=BACKUP_PARAMS)
        p.pg_basedump(db_config=config_key, params=BASE_BACKUP_PARAMS)
    elif Counter(command_dict.keys()) == ['-r', '-p']:
        rsync_cfg = command_dict.get("-r")
        compare = command_dict.get("-p")
        p.rsync_file(rsync_conf=rsync_cfg, compare=compare)
    else:
        useage()
        sys.exit(1)


if __name__ == "__main__":
    main()
