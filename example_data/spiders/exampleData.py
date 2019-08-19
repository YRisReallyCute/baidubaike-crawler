# -*- coding: utf-8 -*-
import scrapy
import xlrd
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from example_data.items import ExampleDataItem

import csv
import re


class ExampledataSpider(scrapy.spiders.CrawlSpider):
    name = 'exampleData'
    start_urls = ['https://baike.baidu.com']
    base_site='https://baike.baidu.com/item/'

# ---------------------------初始化部分--------------------------------
    #包括：读取疾病名称以待爬取；读取一级标题到列的转换关系并形成字典；初始化cache；初始化特殊一级标题数组

    # 读取疾病信息到数组 diseaseArry 中
    file = '病证症空汇总_能爬到的.csv'
    diseaseArry=[]
    with open(file) as f2:
        reader = csv.DictReader(f2)
        for row in reader:
            diseaseArry.append(row['name'])

    # 读取存放转换关系的文件Etitle2col.csv，并以字典对形式存储，
    # key值对应标题名，key_value对应列名，可以直接通过列名访问item
    filename = 'Etitle2col.csv'
    title2num = {}
    with open(filename) as f1:
        reader = csv.DictReader(f1)
        for row in reader:
            title = row['name']
            num = row['num']
            title2num[title] = num

    #未给出的标题cache
    l1_cache=[]
    text_cache=[]
    l2_cache=[]
    l2_text_cache=[]

    l1_name={}
    l2_name={}

    wb = xlrd.open_workbook('证症病空-未出现的标题汇总.xlsx')
    t1 = wb.sheet_by_index(0)
    l = []
    n = []
    for i in range(173):
        l += t1.row_values(i, 0, 1)
        n += t1.row_values(i, 1, 2)

    for i in range(l.__len__()):
        l1_name[l[i]] = n[i]

    t_2 = wb.sheet_by_index(1)
    l_2 = []
    n_2 = []
    for i in range(695):
        l_2 += t_2.row_values(i, 0, 1)
        n_2 += t_2.row_values(i, 1, 2)

    for i in range(l.__len__()):
        l2_name[l_2[i]] = n_2[i]

    # 特殊一级标题，这些标题下的二级标题可能会被存在其他列下
    special_l1=['辨证施治','辨证论治','辨证要点','预防保健','预防','其他疗法','其他治疗']

    # 需要存储两个位置的标题，这些标题需要存在多个列下
    multi_pos_l1={'预防调护':['yfbj','hl']}

# ----------------------------函数find_level--------------------------------
    # 根据字典的key值（一级标题）返回对应的列名
    def find_level(self, level):
        if self.title2num.get(level) != None:
            return self.title2num[level]
        if re.search(r'.*发病.*', level):
            return 'fbbw'
        if re.search(r'.*西医病名.*', level):
            return 'xybm'
        if re.search(r'.*常见病.*', level):
            return 'jbzd'
        return -1

# ----------------------------函数is_special--------------------------------
    #判断是不是特殊的二级标题，如果是特殊的二级标题，返回应当放到的列名
    def is_special(self,level1,level2):
        itemName=''
        if re.search(r".*辨证施治.*", level1):
            # 根据二级标题分类
            if re.search(r".*辨证要点.*", level2) :
                itemName= 'jbzd'
            elif re.search(r".*基本方药.*", level2) :
                itemName='fj'
            elif re.search(r".*针灸.*", level2):
                itemName='zjlf'
            else:
                itemName = ''

        elif re.search( '辨证论治',level1):
            if re.search(r".*辨证要点.*", level2):
                itemName='jbzd'

        elif re.search( '其他疗法',level1) or re.search( '其他治疗',level1):
            if re.search(r".*针灸.*", level2):
                itemName='zjlf'
            elif re.search(r".*饮食.*", level2):
                itemName='yslf'
            elif re.search(r".*外治.*", level2) or re.search(r".*外敷.*", level2):
                itemName='wfwz'
            elif re.search(r".*推拿.*", level2):
                itemName='tnlf'

        elif re.search( '辨证要点',level1):
            if re.search(r".*治疗原则.*", level2):  # 10
                itemName='bzsz'

        elif re.search( '预防保健',level1):
            if re.search(r".*中草药.*", level2):  # 10
                itemName='fj'

        elif re.search( '预防',level1):
            if re.search(r".*中.*药.*", level2):
                itemName='fj'
            elif re.search(r".*食疗.*", level2):
                itemName='yslf'
            elif re.search(r".*汤品.*", level2):
                itemName='yslf'

        else:
            itemName = ''

        return itemName

# ----------------------------函数replace_special--------------------------------

    def replace_special(self,str):
        nstr=str.strip().replace('\t','').replace('\xa0','').replace('\u3000','').replace('辩', '辨').replace('：', '').replace(' ', '')
        return nstr

# ----------------------------函数replace_special--------------------------------
    # 去除二级标题的标号，不保留其顺序
    def replace_l2(self,str):
        # 如果有其他形式可以再添加
        nstr = self.replace_special(str)
        nstr=nstr.replace('一、','').replace('二、','').replace('三、','').replace('四、', '').replace('五、', '')
        return nstr


# -----------------------------函数parse--------------------------------------
    # 将疾病名称与base_site拼接形成URL，循环爬取，调用getInfo
    def parse(self, response):
        for i in range(len(self.diseaseArry)):
            url = self.base_site + self.diseaseArry[i]
            yield  scrapy.Request(url,callback=self.getInfo)
        # url='https://baike.baidu.com/item/心阴虚证'
        # yield scrapy.Request(url, callback=self.getInfo)

# ---------------------------函数getInfo--------------------------------------
    # 爬取页面具体信息
    def getInfo(self,response):
        item = ExampleDataItem()

        # 将每一个item初始化为''
        itemArry=['mc','mcjs','bm','ywmc','fk','dfrq','fbbw','xybm','bybj','lcbx','jbzd','bzsz',
                    'fj','zjlf','yfbj','yslf','tnlf','wfwz','hl','yh','qt','url','auth']
        for i in range(len(itemArry)):
            item[itemArry[i]]=''

        #权威认证
        # authority1=response.xpath('//div[@class="page-background"]').extract()
        # authority2=response.xpath('//a[@class="page-background"]').extract()
        # authority=authority1+authority2
        authority=response.xpath('//*[@class="page-background"]').extract()
        if(len(authority)>0):
            item['auth']=1
        else:
            item['auth']=0

        # # 锁定
        # locked = response.xpath('//a[@class="lock-lemma" and contains(@style,"display")]//text()').extract()
        # notlock=response.xpath('//a[@class="lock-lemma"]').css('::attr(style)').extract()
        # # if(len(notlock)>0):
        # #     item['locked'] = 0
        # # else:
        # if (len(locked) > 0):
        #     item['locked'] = 1
        # else:
        #     item['locked'] = 0


        # URL的值
        item['url'] = response.request.url

        #名词
        item['mc']= response.xpath('//dd[@class="lemmaWgt-lemmaTitle-title"]/h1/text()').extract()[0]

        #名词解释
        mcjs_text=''
        mcjs_para= response.xpath('//div[@class="lemma-summary"]/div[@class="para"]/text()').extract()
        for each_t1 in mcjs_para:
            mcjs_text=mcjs_text+each_t1+'<br/>'
        item['mcjs']=mcjs_text

        # ------------------------------------基本信息部分----------------------------------------

        # 所有能爬取到的一级标题及其对应内容
        level1 = []
        level1_text = []

        base_div_arry=response.xpath('.//div')
        for each_base_div in base_div_arry:
            # 根据class属性判断是否是基本信息部分
            kk=each_base_div.css('::attr(class)').extract()
            while type(kk)is list and len(kk)>0:
                kk=kk[0]
            if len(kk)==0:continue

            if re.search(r'.*[b B]ase.*[i I]nfo.*',kk) :
                for each_dl in each_base_div.xpath('.//dl'):
                    dt = each_dl.xpath('./dt/text()').extract()
                    dd = each_dl.xpath('./dd/text()').extract()
                    for j in range(len(dt)):
                        level1.append(self.replace_special(dt[j]))
                        level1_text.append(self.replace_special(dd[j]))

        for i in range(len(level1)):
            tmp_ind=self.find_level(level1[i])
            if tmp_ind!=-1:
                tmp_l='<h1>'+level1[i]+'</h1>'+'<br/>'
                item[tmp_ind]+= tmp_l+level1_text[i]
            else:
                if(level1[i] not in self.l1_name.keys()):
                    self.l1_name[level1[i]]=item['mc']
                self.l1_cache.append(item['mc']+'\t'+level1[i])
                self.text_cache.append(level1_text[i])

        # -------------------------------------正文部分-------------------------------------------

        tmp_l1=''           #存放一级标题
        tmp_l1_text=''      #存放一级标题及其下内容
        tmp_l3=''           #存放二级标题
        tmp_l3_text=''      #存放二级标题及其下内容
        tmp_l3_num=0        #存放当前二级标题下是否有内容
        tmp_col = ''        #存放二级标题内容应该放到哪列下面，列名

        tmp_l1=response.xpath('//div[@class="para-title level-2"]/h2/text()').extract_first()
        if(tmp_l1==None):
            tmp_l1=""
        else:
            tmp_l1=self.replace_special(tmp_l1)
        tmp_l1_text=tmp_l1_text+'<h1>'+tmp_l1+'</h1>'+'<br/>'
        level1.append(tmp_l1)

        divArry=response.xpath('//div[@class="para-title level-2"]/following-sibling::div')     #选出同级的所有div

        #逐个div进行提取
        for each_div in divArry:
            css=each_div.css('::attr(class)').extract()[0]

            # -------------------------------- 一级标题 -----------------------------------------
            if css == 'para-title level-2':
                #如果有还未写入的l3，将其写入
                if tmp_l3_num != 0:
                    item[tmp_col] += '<h1>' + tmp_l1 + '</h1>' + '<br/>' + tmp_l3_text
                else:
                    tmp_l1_text+=tmp_l3_text
                tmp_l3 = ''  # 更新l2
                tmp_l3_text = ''
                tmp_col=''
                tmp_l3_num = 0

                #更新tmp_l1的值，并将tmp_l1_text写进对应的item中
                if self.multi_pos_l1.get(tmp_l1)!=None:     #是否需要写入多个列中
                    col_arry=self.multi_pos_l1[tmp_l1]
                    for each_col in col_arry:
                        item[each_col]+=tmp_l1_text
                else:
                    number=self.find_level(tmp_l1)
                    if(number==""):
                        continue
                    if number==-1:
                        #没找到，写进cache里
                        if(tmp_l1 not in self.l1_name.keys()):
                            self.l1_name[tmp_l1]=item['mc']
                        self.l1_cache.append(item['mc']+'\t'+tmp_l1)
                        self.text_cache.append(tmp_l1_text)
                    else:
                        #找到了，写进对应的item里
                        item[number]+=tmp_l1_text
                #重置tmp值
                tmp_l1=self.replace_special(each_div.xpath('./h2/text()').extract()[0])
                tmp_l1_text=''
                tmp_l1_text+='<h1>'+tmp_l1+'</h1>'+'<br/>'


            # -------------------------------- 二级标题 -----------------------------------------
            if css == 'para-title level-3':
                # 先将缓存的特殊二级标题内容写入，特殊标题写入特殊列
                if tmp_l3_num != 0:
                    item[tmp_col] += '<h1>' + tmp_l1 + '</h1>' + '<br/>' + tmp_l3_text
                else:   #非特殊标题写入tmp_l1_text
                    tmp_l1_text+=tmp_l3_text
                 #清空缓存
                tmp_col = ''
                tmp_l3_num = 0

                level2 = each_div.xpath('./h3/text()').extract()[0]
                level2=self.replace_l2(level2)
                tmp_l3=level2
                # 写进cache里
                if(level2 not in self.l2_name.keys()):
                    self.l2_name[level2]=item['mc']
                tmp_l3_text = '<h2>' + tmp_l3 + '</h2>' + '<br/>'
                if self.is_special(tmp_l1, tmp_l3) != '':
                    tmp_l3_num = 1
                    tmp_col = self.is_special(tmp_l1, tmp_l3)

            # -------------------------------- 正文内容 -----------------------------------------

            if css == 'para':
                tmp_str = ''
                para_text = each_div.xpath('.//text()').extract()
                b_text = each_div.xpath('./b/text()').extract()
                #判断此时的一级标题是否是特殊标题
                tmp_l1 = self.replace_special(tmp_l1)
                tmp_l3 = self.replace_special(tmp_l3)

                if len(para_text) < 2 and len(para_text) > 0:
                    if len(b_text)>0:#二级标题
                        if tmp_l3_num != 0:
                            item[tmp_col] += '<h1>' + tmp_l1 + '</h1>' + '<br/>' + tmp_l3_text
                        else:  # 非特殊标题写入tmp_l1_text
                            tmp_l1_text += tmp_l3_text
                        # 清空缓存
                        tmp_col = ''
                        tmp_l3_num = 0
                        #更新tmp_l3的值
                        tmp_l3=b_text[0]
                        #写进cache里
                        if (tmp_l3 not in self.l2_name.keys()):
                            self.l2_name[tmp_l3] = item['mc']
                        tmp_l3_text='<h2>' + tmp_l3 + '</h2>' + '<br/>'
                        if self.is_special(tmp_l1, tmp_l3) != '':
                            tmp_l3_num = 1
                            tmp_col = self.is_special(tmp_l1, tmp_l3)
                    else:#一段话
                        if tmp_l3!='':
                            tmp_l3_text+=para_text[0]
                        else:
                            tmp_l1_text+=para_text[0]
                else:#很多段话
                    for each_para_text in para_text:
                        tmp_str+= self.replace_special(each_para_text)
                    if tmp_l3 != '':
                        tmp_l3_text += tmp_str
                    else:
                        tmp_l1_text += tmp_str

        # 如果有还未写入的l3，将其写入
        if tmp_l3_num != 0:
            item[tmp_col] += '<h1>' + tmp_l1 + '</h1>' + '<br/>' + tmp_l3_text
        else:
            tmp_l1_text += tmp_l3_text

        #是否需要写入多个列中
        if self.multi_pos_l1.get(tmp_l1) != None:
            col_arry = self.multi_pos_l1[tmp_l1]
            for each_col in col_arry:
                item[each_col] += tmp_l1_text
        else:
            number = self.find_level(tmp_l1)
            if number!="":
                if number == -1:
                    # 没找到，写进cache里
                    self.l1_cache.append(item['mc']+'\t'+tmp_l1)
                else:
                    # 找到了，写进对应的item里
                    item[number] += tmp_l1_text

        for each_key in self.l1_name.keys():
            print(each_key+'\t'+self.l1_name[each_key])

        print("/r/n----------二级标题------------/r/n")

        for each_key in self.l2_name.keys():
            print(each_key+'\t'+self.l2_name[each_key])

        yield item




        #基本信息部分
        # baseArry = response.xpath('//div[@class="lemma-summary"]/following-sibling::div')
        # for i in range(len(baseArry)):
        #     if i==5:
        #         break
        #     for each_dl in baseArry[i].xpath('.//dl'):
        #         dt = each_dl.xpath('./dt/text()').extract()
        #         dd = each_dl.xpath('./dd/text()').extract()
        #         for j in range(len(dt)):
        #             level1.append(self.replace_special(dt[j]))
        #             level1_text.append(self.replace_special(dd[j]))


                #para部分
                # if tmp_l1 in self.special_l1:
                #
                #     # 情况一：一级标题和二级标题都是特殊标题，此时需要将tmp_l3_text写入不同列下
                #     if self.is_special(tmp_l1, tmp_l3) != '':
                #         tmp_l3_num = 1
                #         tmp_col = self.is_special(tmp_l1, tmp_l3)
                #         if len(para_text) < 2 and len(para_text) > 0:  # 一段话或者标题
                #             if len(b_text) > 0:  # 标题2
                #                 b_text[0]=self.replace_l2(b_text[0])
                #                 tmp_str += '<h2>' + b_text[0] + '</h2>'
                #                 item[tmp_col] += '<h1>' + tmp_l1 + '</h1>' + '<br/>' + tmp_l3_text
                #                 tmp_l3 = b_text[0]  # 更新l2
                #                 tmp_l3_text = ''
                #                 tmp_l3_num = 0
                #                 tmp_col=''
                #             else:  # 一段话
                #                 tmp_str += para_text[0]
                #         else:  # 将所有提取到的文字组合起来
                #             for each_para_text in para_text:
                #                 tmp_str += self.replace_special(each_para_text)
                #         tmp_l3_text += tmp_str + '<br/>'
                #
                #     # 情况二：只有一级标题是特殊标题，此时需要更新tmp_l3的值
                #     else:
                #         if len(para_text) < 2 and len(para_text) > 0:  # 一段话或者标题
                #             if len(b_text) > 0:  # 标题2
                #                 b_text[0] = self.replace_l2(b_text[0])
                #                 tmp_str = tmp_str + '<h2>' + b_text[0] + '</h2>'
                #                 tmp_l3 = b_text[0]  # 更新l2
                #                 tmp_l3_text = tmp_str + '<br/>'
                #             else:  # 一段话
                #                 tmp_str += para_text[0]
                #         else:  # 将所有提取到的文字组合起来
                #             for each_para_text in para_text:
                #                 tmp_str += self.replace_special(each_para_text)
                #         if self.is_special(tmp_l1, tmp_l3) == '':
                #             tmp_l1_text += tmp_str + '<br/>'
                #
                # # 情况三：都不是特殊标题，不需要更新tmp_l3的值，也不需要写入
                # else:
                #     if len(para_text) < 2 and len(para_text) > 0:  # 一段话或者标题
                #         if len(b_text) > 0:  # 标题2
                #             b_text[0] = self.replace_l2(b_text[0])
                #             tmp_str = tmp_str + '<h2>' + b_text[0] + '</h2>'
                #         else:  # 一段话
                #             tmp_str = tmp_str + para_text[0]
                #     else:  # 将所有提取到的文字组合起来
                #         for each_para_text in para_text:
                #             tmp_str += self.replace_special(each_para_text)
                #     tmp_l1_text+= tmp_str + '<br/>'