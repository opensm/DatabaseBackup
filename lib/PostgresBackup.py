import sys
from lib.Log import RecodeLog
import os
from lib.setting import EXEC_BIN, DB_CONFIG_DICT, RSYNC_CONFIG_DICT
import psycopg2


class PostgresDumps:

    @staticmethod
    def exec_command(command):
        """
        :param command:
        :return:
        """
        try:
            if sys.version_info < (3, 0):
                import commands
                (status, output) = commands.getstatusoutput(cmd=command)
            else:
                import subprocess
                (status, output) = subprocess.getstatusoutput(cmd=command)
            if status != 0:
                raise Exception(output)
            RecodeLog.info(msg="执行命令成功：{0}".format(command))
            return True
        except Exception as error:
            RecodeLog.error("执行命令异常：{0},原因:{1}".format(command, error))
            return False

    @staticmethod
    def get_database_list(db_config):
        """
        :param db_config:
        :return:
        """
        if not db_config in DB_CONFIG_DICT.keys():
            raise Exception("{0}:相关配置不存在，数据库配置列表中！")
        config_dict = DB_CONFIG_DICT[db_config]
        conn = psycopg2.connect(**config_dict)
        cursor = conn.cursor()
        cursor.execute("show database;")
        rows = cursor.fetchall()
        return rows

    def postgres_dump(self, params, db_config):
        """
        :param params:
        :param db_config:
        :return:
        """
        psql = os.path.join(EXEC_BIN, 'psql')
        pg_dump = os.path.join(EXEC_BIN, 'pg_dump')
        if not os.path.exists(psql) or not os.path.exists(pg_dump):
            raise EnvironmentError("可执行命令不存在: {0},{1}".format(psql, pg_dump))

        dblist = self.get_database_list(db_config=db_config)

        if len(dblist) == 0 or not dblist:
            raise Exception("没有获取到数据库列表:{0}".format(db_config))

        for db in dblist:
            print(db)
            dump_str = "{0} {1}".format(pg_dump, params)
            print(dump_str)
