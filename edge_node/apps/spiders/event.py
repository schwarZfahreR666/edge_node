import requests
from bs4 import BeautifulSoup as bs
import json
import time
import pandas as pd
import re
import csv
from datetime import datetime
import pymysql

myheaders = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Host": "httpbin.org",
    "Referer": "https://blog.csdn.net/XnCSD/article/details/88615791",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
    "X-Amzn-Trace-Id": "Root=1-5f4e0ada-a90c5fb53819815b478964e7"
}

dic = {'date': None, 'source': None, 'content': None}

def get_event_bendibao():
    result = []
    tags = []
    for i in range(1, 2):
        start_url = 'http://bj.bendibao.com/news/list_17_175_'+str(i)+'.htm'
        res = requests.get(url=start_url, headers=myheaders)
        res.encoding = 'utf8'
        res_text = bs(res.text, 'html.parser')
        tags += res_text.select(".J-share-a")

    urls = []
    for tag in tags:
        tag_info = {'url':None,'info':None}
        pattern = re.compile(r'href\=\"(.*?)\"')
        url = pattern.search(str(tag)).group(1)
        pattern = re.compile(r'\>(.*?)\<\/')
        info = pattern.search(str(tag)).group(1)

        tag_info['url'] = url
        tag_info['info'] = info
        urls.append(tag_info)


    for url in urls:
        dic = {'date':None,'source':None,'content':None}
        con = {'content':None}
        address = url['url']
        res = requests.get(url = address,headers = myheaders)
        res.encoding = 'utf8'
        res_text=bs(res.text,'html.parser')
        time = res_text.select(".time")
        pattern = re.compile(r'发布时间：(.*?)\<')
        time = pattern.search(str(time)).group(1)
        text = res_text.select(".content")
        text=bs(str(text),'html.parser')
        text = bs(str(text.select("p")),'html.parser')
        ans = ''
        for child in text.contents:
            child_bs = bs(str(child),'html.parser')
            inner = child_bs.select('p')
            if inner:
                ans += str(inner[0].text)
        ans = ans.replace('\u3000', '').replace('\n', '')
        dic['date'] = time
        dic['source'] = address
        dic['content'] = ans
        con['content'] = ans
        result.append(con)
    print(result)
    return result


def get_event_jiaoguanju():
    result = []
    base_url = 'http://jtgl.beijing.gov.cn/'
    dic = {'date':None,'source':None,'content':None}
    con = {'content':None}

    for i in range(1,2):

        start_url = 'http://jtgl.beijing.gov.cn/jgj/jgxx/gsgg/jttg/df7fa898-'+ str(i) +'.html'
        res = requests.get(url = start_url,headers = myheaders)
        res.encoding = 'utf8'
        res_text=bs(res.text,'html.parser')
        lis = res_text.select(".mod_page_content_li")
        for li in lis:
            con = {'content':None}
            time = li.select(".mod_page_content_time")[0].text
            title = li.a.text
            url = li.a.get("href")
            src = base_url + url
            res = requests.get(url = src,headers = myheaders)
            res.encoding = 'utf8'
            res_text=bs(res.text,'html.parser')
            content = res_text.select('.content001')[0].text.replace('\u3000','').replace('\n','')
        #         dic['date'] = time
        #         dic['source'] = src
            con['content'] = content
            result.append(con)

    print(result)
    return result


def get_event_bus():
    result = []
    dic = {'date':None,'source':None,'content':None}
    con = {'content':None}

    start_url = 'http://www.bjbus.com/home/ajax_news_list.php'
    formdata ={'txtPage': 1,
           'txtDisplayRows': 10,
           'txtType': 1,
           'txtCode': None,
           'txtContainer': 'content',
           'txtStyle': 2}
    res = requests.post(url = start_url,headers = myheaders,data =formdata)
    res.encoding = 'utf8'

    res_text=bs(res.text,'html.parser')
    tags = res_text.select("a[href][target]")

    urls = []
    for tag in tags:
        tag_info = {'url':None,'info':None}
        pattern = re.compile(r'href\=\"(.*?)\"')
        url = pattern.search(str(tag)).group(1)
        pattern = re.compile(r'\>(.*?)\<\/')
        info = pattern.search(str(tag)).group(1)
        if info!='[详情]':
            tag_info['url'] = url
            tag_info['info'] = info
            urls.append(tag_info)

    for url in urls:
        dic = {'date':None,'source':None,'content':None}
        con = {'content':None}
        address = 'http://www.bjbus.com/home/'+url['url']
        res = requests.get(url = address,headers = myheaders)
        res.encoding = 'utf8'
        res_text=bs(res.text,'html.parser')
        time = res_text.select("span")
        pattern = re.compile(r'发布时间：(.*?)\<')
        time = pattern.search(str(time)).group(1)
        ans = ''
        for text in res_text.select("p"):
            if '上一篇' in text.text:
                break
            ans = ans + text.text
        year = time.split(' ')[1]
        time = time.split(' ')[0]
        mon = time.split('/')[0]
        day = time.split('/')[1]
        time = year + '-' + mon + '-' + day
        ans = ans.replace('\xa0', '')

        # dic['date'] = time
        # dic['source'] = address
        # dic['content'] = ans
        con['content'] = ans
        # 入库
        # 打开数据库连接
        # db = pymysql.connect(host='39.99.192.63',
        #                  database='DEMODB',
        #                  port=3306,
        #                  user='devops',
        #                  password='devops',
        #                  charset="utf8",
        #                  use_unicode=True)
        # # 使用 cursor() 方法创建一个游标对象 cursor
        # cursor = db.cursor()
        # # SQL 插入语句
        # sql = "INSERT INTO EVENT (date,source,content) \
        #             VALUES (%s, %s,  %s);"
        #
        # try:
        #     # 执行sql语句
        #     cursor.execute("set names utf8;")
        #     cursor.execute(sql,(time,address,ans))
        #     # 提交到数据库执行
        #     db.commit()
        # except pymysql.Error as e:
        #     print(e.args[0], e.args[1])
        #     print(time,address,ans)
        #     # 如果发生错误则回滚
        #     db.rollback()
        # # 关闭数据库连接
        #     db.close()
        result.append(con)
    print(result)
    return result


def get_event_yingjiju():
    result = []
    start_url = 'http://yjglj.beijing.gov.cn/col/col2472/index.html'
    res = requests.get(url = start_url,headers = myheaders)
    pattern = re.compile(r'urls\[i\] = \'(.*?)\'')
    tags = pattern.findall(res.text)
    urls = []
    base = 'http://yjglj.beijing.gov.cn'
    count = 0
    tag_num = 10
    for tag in tags:
        url = base + tag
        res = requests.get(url = url,headers = myheaders)
        res_text=bs(res.content,'html.parser')
        #     print(res_text)
        date = res_text.select('meta[name="PubDate"]')[0].get('content')
        info = res_text.select('meta[name="description"]')[0].get('content')
        dic = {'date':None,'source':None,'content':None}
        con = {'content':None}
        # dic['date'] = date
        # dic['source'] = url
        con['content'] = info
        result.append(con)
        count += 1
        if count >= tag_num:
            break

    print(result)
    return result


if __name__ == '__main__':
    get_event_yingjiju()
