# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

import pymysql
from sshtunnel import SSHTunnelForwarder

class mysqlPipeline(object):

    # count=260

    def __init__(self):

        # self.server=SSHTunnelForwarder(
        #     ssh_address_or_host=('101.200.196.248',7002),
        #     ssh_username='rsadmin',
        #     ssh_password='rskj@chinark',
        #     remote_bind_address=('127.0.0.1', 3306)
        # )
        # self.server.start()

        # 建立数据库连接
        # self.connection = pymysql.connect(user='cupid', password='mysql@chinark', db='yy_data',host='127.0.0.1',port=self.server.local_bind_address,
        #                                   charset='utf8')

        self.connection = pymysql.connect(user='root', password='root', db='yy_data1', host='127.0.0.1',
                                          port=3303,
                                          charset='utf8')

        # 创建操作游标
        self.cursor = self.connection.cursor()


    def process_item(self, item, spider):
        #获取当前时间
        dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #定义sql语句
        self.cursor.execute(
            """insert into yy_data1.zdata_baidubaike_all_病证症空(info_mc,info_mcjs,info_bm,info_ywmc,info_fk,info_dfrq,info_fbbw,info_xybm,info_bybj,info_lcbx,info_jbzd,info_bzsz,info_fj,info_zjlf,info_yfbj,info_yslf,info_tnlf,info_wfwz,info_hl,info_yh,info_qt,origin_url,web_authority,create_time,update_time)
            value(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s)""",
            (
                item['mc'],
                item['mcjs'],
                item['bm'],
                item['ywmc'],
                item['fk'],
                item['dfrq'],
                item['fbbw'],
                item['xybm'],
                item['bybj'],
                item['lcbx'],
                item['jbzd'],
                item['bzsz'],
                item['fj'],
                item['zjlf'],
                item['yfbj'],
                item['yslf'],
                item['tnlf'],
                item['wfwz'],
                item['hl'],
                item['yh'],
                item['qt'],
                item['url'],
                item['auth'],
                dt,
                dt,
            )
        )
        # self.count=self.count+1
        self.connection.commit()
        return item

    def __del__(self):
        self.cursor.close()
        self.connection.close()
