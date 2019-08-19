# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ExampleDataItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # 共22列，取列名的拼音首字母，对应关系如下：

    # mc:"名词"           mcjs:"名词解释"     bm:"别名"           ywmc="英文名称"
    # fk:"分科"           dfrq:"多发人群"     fbbw:"发病部位"     x/z ybm:"西/中 医病名"
    # bybj:"病因病机"     lcbx:"临床表现"     jbzd:"鉴别诊断"     bzsz:"辨证施治"
    # fj:"方剂"           zjlf:"针灸疗法"     yfbj:"预防保健"     yslf:"饮食疗法"
    # tnlf:"推拿疗法"     wfwz:"外敷/外治"     hl:"护理"          yh:"预后"
    # qt:"其他"           url:保存爬取路径

    mc = scrapy.Field()
    mcjs = scrapy.Field()
    bm = scrapy.Field()
    ywmc = scrapy.Field()
    fk = scrapy.Field()
    dfrq = scrapy.Field()
    fbbw = scrapy.Field()
    xybm = scrapy.Field()
    bybj = scrapy.Field()
    lcbx = scrapy.Field()
    jbzd = scrapy.Field()
    bzsz = scrapy.Field()
    fj = scrapy.Field()
    zjlf = scrapy.Field()
    yfbj = scrapy.Field()
    yslf = scrapy.Field()
    tnlf = scrapy.Field()
    wfwz = scrapy.Field()
    hl = scrapy.Field()
    yh = scrapy.Field()
    qt = scrapy.Field()
    url = scrapy.Field()
    auth=scrapy.Field()

