# -*- coding: utf-8 -*-
import sys
from lib.Log import RecodeLog
import os
from lib.setting import EXEC_BIN, DB_CONFIG_DICT, RSYNC_CONFIG_DICT, BACKUP_DIR, GET_ADDRESS_CMD
import psycopg2
import copy
import socket
import datetime


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
            RecodeLog.info(msg="执行命令成功：{0},{1}".format(command, output))
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
        # cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute("select pg_database.datname AS dbname from pg_database;")
        rows = cursor.fetchall()
        return rows

    @staticmethod
    def get_address():
        """
        :return:
        """
        if sys.version_info < (3, 0):
            import commands
            (status, output) = commands.getstatusoutput(cmd=GET_ADDRESS_CMD)
        else:
            import subprocess
            (status, output) = subprocess.getstatusoutput(cmd=GET_ADDRESS_CMD)
        if status != 0:
            RecodeLog.error("执行命令异常：{0},原因:{1}".format(GET_ADDRESS_CMD, output))
            raise Exception(output)
        return output

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
        ipaddress = self.get_address()
        pg_password = pg_params.pop('password')
        pg_database = pg_params.pop('database')
        dump_params = "export PGPASSWORD={0} && {1} {2}".format(pg_password, pg_dump, copy.deepcopy(params))
        rsync_params = copy.deepcopy(RSYNC_CONFIG_DICT[db_config])

        for key, value in pg_params.items():
            dump_params = "{0} --{1}={2}".format(dump_params, key, value)
        for db in dblist:
            achieve = os.path.join(BACKUP_DIR, "{0}_{1}_{2}.gz".format(
                ipaddress, db[0], datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
            ))
            dump_str = "{0} {1}| gzip > {2} && md5sum {2} > {2}.md5".format(
                dump_params, db[0], os.path.join(BACKUP_DIR, "{0}_{1}_{2}.gz".format(
                    ipaddress, db[0], datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
                ))
            )
            rsync_params['achieve'] = "{0}.*".format(os.path.splitext(achieve)[0])
            if not self.exec_command(command=dump_str):
                RecodeLog.error(msg="备份数据库失败：{0}".format(dump_str))
            else:
                RecodeLog.info(msg="备份数据库成功：{0}".format(dump_str))
            self.rsync_dump(**rsync_params)

    def rsync_dump(self, passwd, achieve, user, host, mode, port=873, timeout=60):
        """
        :param passwd:
        :param timeout:
        :param achieve:
        :param user:
        :param host:
        :param mode:
        :param port:
        :return:
        """
        rsync_cmd_str = '''export RSYNC_PASSWORD="%s"''' \
                        ''' && /usr/bin/rsync ''' \
                        '''-vzrtopgPc ''' \
                        '''--progress ''' \
                        '''--timeout=%d ''' \
                        '''--port=%d ''' \
                        '''--chmod=o+r %s %s@%s::%s''' % (
                            passwd, int(timeout), port, achieve, user, host, mode
                        )
        if not self.exec_command(command=rsync_cmd_str):
            RecodeLog.error(msg="推送文件失败！{0}".format(rsync_cmd_str))
            return False
        else:
            RecodeLog.info(msg="推送文件成功！{0}".format(rsync_cmd_str))
            return True

    def pg_basedump(self, db_config, params):
        """
        :param db_config:
        :param params:
        :return:
        """
        if not isinstance(db_config, str):
            raise Exception("输入数据库类型错误！{0}", db_config)
        pg_basebackup = os.path.join(EXEC_BIN, 'pg_basebackup')
        if not os.path.exists(pg_basebackup):
            raise EnvironmentError("可执行命令不存在: {0}".format(pg_basebackup))
        pg_params = copy.deepcopy(DB_CONFIG_DICT[db_config])
        ipaddress = self.get_address()
        pg_password = pg_params.pop('password')
        pg_database = pg_params.pop('database')
        dump_params = "export PGPASSWORD={0} && {1} {2}".format(pg_password, pg_basebackup, copy.deepcopy(params))
        rsync_params = copy.deepcopy(RSYNC_CONFIG_DICT[db_config])

        for key, value in pg_params.items():
            dump_params = "{0} --{1}={2}".format(dump_params, key, value)

        achieve = os.path.join(BACKUP_DIR, "{0}_all-dump_{1}".format(
            ipaddress, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
        ))
        if not os.path.exists(achieve):
            os.makedirs(achieve)
        pg_basedump_str = "{0} -D {1} ".format(dump_params, achieve)
        rsync_params['achieve'] = achieve
        if not self.exec_command(command=pg_basedump_str):
            RecodeLog.error(msg="全量备份异常！{0}".format(pg_basedump_str))
            return False
        else:
            RecodeLog.info(msg="全量备份成功！{0}".format(pg_basedump_str))
            self.rsync_dump(**rsync_params)
            return True
