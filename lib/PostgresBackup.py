# -*- coding: utf-8 -*-
import sys
from lib.Log import RecodeLog
import os
from lib.setting import EXEC_BIN, DB_CONFIG_DICT, RSYNC_CONFIG_DICT
import psycopg2
from psycopg2.extras import RealDictCursor
import copy


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
        if db_config not in DB_CONFIG_DICT.keys():
            raise Exception("{0}:相关配置不存在，数据库配置列表中！")
        config_dict = DB_CONFIG_DICT[db_config]
        conn = psycopg2.connect(**config_dict)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("select pg_database.datname AS size from pg_database;")
        rows = cursor.fetchall()
        print(rows)
        return rows

    def postgres_dump(self, params, db_config):
        """
        :param params:
        :param db_config:
        :return:
        """
        if not isinstance(db_config, str):
            raise Exception("输入数据库类型错误！{0}", db_config)
        psql = os.path.join(EXEC_BIN, 'psql')
        pg_dump = os.path.join(EXEC_BIN, 'pg_dump')
        if not os.path.exists(psql) or not os.path.exists(pg_dump):
            raise EnvironmentError("可执行命令不存在: {0},{1}".format(psql, pg_dump))

        dblist = self.get_database_list(db_config=db_config)

        if len(dblist) == 0 or not dblist:
            raise Exception("没有获取到数据库列表:{0}".format(db_config))

        pg_params = copy.deepcopy(DB_CONFIG_DICT[db_config])
        print(pg_params)
        pg_password = pg_params.pop('password')
        pg_database = pg_params.pop('database')
        dump_params = "export PGPASSWORD={0} && {1} {2}".format(pg_password, pg_dump, copy.deepcopy(params))
        dump_str = ""

        for key, value in pg_params.items():
            dump_params = "{0} --{1}={2}".format(dump_params, key, value)
        for db in dblist:
            print(db)
            dump_str = "{0} {1}".format(dump_params, db)
        print(dump_str)
