import os
import requests
import json
import time
import pandas as pd
import re
import csv
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import pymysql


aks=['DmmkH3X8gC1c0nZDeUKlr6WOiSndU8LK',
    'mIbPuvn8Z6ogd291fEZVaF1z4769yvFa',
    'ifN2jpMZsDWvshKZrAowdaQjwUkwr8vv',
    '46I8r8Yf7aBNHXP7Paow0nW1N1AvlGNl'
]
gd_ak='1c1398b59b98b8db138d04ea20fa517a'
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


# roads = ['松山大桥',
#         '西羊坊隧道',
#         '京银路',
#         '康张路',
#         '京礼高速',
#         '百康路',
#         '西丁路',
#         '百泉街',
#         '湖南西路',
#         '妫水南街',
#         '京藏高速',
#         '京新高速',
#         '京承高速',
#         '万泉河路',
#         '莲花池西路',
#         '莲花池东路',
#         '大兴机场高速',
#         '崇文门东大街',

#     ]
_change_status_root = os.path.join('edge_node/apps', 'spiders')
print(_change_status_root)
road_net_path = os.path.join(_change_status_root, 'road_net.json')

road_loc_path = os.path.join(_change_status_root, 'road_loc.json')

road_dirc_path = os.path.join(_change_status_root, 'road_dirc.json')
with open(road_net_path, 'r', encoding='utf-8') as f:
    road_net = json.load(f)
with open(road_loc_path, 'r', encoding='utf-8') as f:
    road_loc = json.load(f)
with open(road_dirc_path, 'r', encoding='utf-8') as f:
    road_dirc = json.load(f)

roads_yq = [
                    '团结路', 
                    '京银路', 
                    '康张路',
                    '西顺城街',
                    '京原路',
                    '京平高速',
                    '京藏高速', 
                    '京新高速', 
                    '京承高速',
                    '京礼高速',
                ]

roads_st = [
                    '北三环', 
                    '学院南路', 
                    '紫竹院路',
                    '西直门外大街',
                    '北二环',
                    '车公庄西路',
                    '车公庄大街', 
                    '阜成路', 
                    '阜成门外大街',
                    '蓝靛厂南路',
                    '西三环',
                    '首体南路',
                    '三里河路',
                    '展览馆路',
                    '西二环',
                    '新街口外大街',
                    '长春桥路',
                    '魏公村路',
                    '新康路',
                    '万寿寺路',
                    '玲珑路',
                    '中关村南大街',
                    '灶君庙街',
                    '大柳树路',
                    '隔音屏隧道',
                    '西土城路',
                    '西直门外大街',
                    '德胜门外大街',
                ]


roads_wks = [
                    '紫竹院路',
                    '阜成路',
                    '玉渊潭南路',
                    '复兴路',
                    '莲石东路',
                    '莲花池西路',
                    '西五环',
                    '西四环',
                    '西翠路',
                    '西三环',
                    '西二环',
                    '杏石口路',
                    '西直门外大街',
                    '阜石路',
                    '阜成门外大街',
                    '石景山路',
                    '复兴门外大街',
                    
                ]


roads_wks_section = [
                    '紫竹院路',
                    '阜成路',
                    '玉渊潭南路',
                    '复兴路',
                    '莲石东路',
                    '莲花池西路',
                    '西五环',
                    '西四环',
                    '西翠路',
                    '西三环',
                    '西二环',
                ]

roads_st_section = [
                    '北三环', 
                    '学院南路', 
                    '紫竹院路',
                    '西直门外大街',
                    '北二环',
                    '车公庄西路',
                    '车公庄大街', 
                    '阜成路', 
                    '阜成门外大街',
                    '蓝靛厂南路',
                    '西三环',
                    '首体南路',
                    '三里河路',
                    '展览馆路',
                    '西二环',
                    '新街口外大街',
                ]
roads = [
                    '团结路', 
                    '京银路', 
                    '康张路',
                    '西顺城街',
                    '京原路',
                    '京平高速',
                    '京藏高速', 
                    '京新高速', 
                    '京承高速',
                    '京礼高速',
                ]


def getSectionNum(text,road_name,direction):
    location = text.split(',')[1]
    found_label = 0
    if '到' in location:
        loc1 = location.split('到')[0]
        loc2 = location.split('到')[1]
        pattern = re.compile(r'从(.*)')
        loc1 = pattern.match(loc1)[1]
        if direction == 0:#南向北
            loc = loc2
        elif direction == 1:#北向南
            loc = loc1
        url = 'http://api.map.baidu.com/geocoding/v3/?address='+ loc +'&output=json&city=北京市&ak=' + aks[0]
        res = requests.get(url).content
        
        
        res = json.loads(res)
        if res['status']==0:
            lng = float(res['result']['location']['lng'])
            lat = float(res['result']['location']['lat'])
    
            road_list = road_net[road_name]
            s_num = 0
            for road_num in road_list:
                s_name = road_list[road_num]['s']
                e_name = road_list[road_num]['e']
                s_lng = float(road_loc[road_name][s_name][0])
                s_lat = float(road_loc[road_name][s_name][1])
                e_lng = float(road_loc[road_name][e_name][0])
                e_lat = float(road_loc[road_name][e_name][1])
        
                if s_lat>=lat and lat>e_lat:
                    s_num = road_num
                    found_label = 1
                    break
                    
        else:
            s_num = 4
    elif '附近' in location:
        pattern = re.compile(r'(.*)附近')
        loc = pattern.match(location)[1]
        
        url = 'http://api.map.baidu.com/geocoding/v3/?address='+ loc +'&output=json&city=北京市&ak=' + aks[0]
        res = requests.get(url).content
        res = json.loads(res)
        if res['status']==0:
            lng = float(res['result']['location']['lng'])
            lat = float(res['result']['location']['lat'])
    
            road_list = road_net[road_name]
            s_num = 0
            for road_num in road_list:
                s_name = road_list[road_num]['s']
                e_name = road_list[road_num]['e']
                s_lng = float(road_loc[road_name][s_name][0])
                s_lat = float(road_loc[road_name][s_name][1])
                e_lng = float(road_loc[road_name][e_name][0])
                e_lat = float(road_loc[road_name][e_name][1])
        
                if s_lat>=lat and lat>e_lat:
                    s_num = road_num
                    found_label = 1
                    break
                    
        else:
            s_num = 4
    else:
        s_num = -1
    if found_label == 0:
        return -1
    return s_num


def getSectionNum2(text,road_name,direction):
    location = text.split(',')[1]
    found_label = 0
    dirc = road_dirc[road_name]
    if '到' in location:
        loc1 = location.split('到')[0]
        loc2 = location.split('到')[1]
        pattern = re.compile(r'从(.*)')
        loc1 = pattern.match(loc1)[1]
        
        if direction == 0:#南向北  东向西
            loc = loc2
        elif direction == 1:#北向南  西向东
            loc = loc1
        url = 'http://api.map.baidu.com/geocoding/v3/?address='+ loc +'&output=json&city=北京市&ak=' + aks[0]
        res = requests.get(url).content
        res = json.loads(res)
        if res['status']==0:
            lng = float(res['result']['location']['lng'])
            lat = float(res['result']['location']['lat'])
    
            road_list = road_net[road_name]
            s_num = -1
            for road_num in road_list:
                s_name = road_list[road_num]['s']
                e_name = road_list[road_num]['e']
                s_lng = float(road_loc[road_name][s_name][0])
                s_lat = float(road_loc[road_name][s_name][1])
                e_lng = float(road_loc[road_name][e_name][0])
                e_lat = float(road_loc[road_name][e_name][1])
                if dirc == "NS":
                    if s_lat>=lat and lat>e_lat:
                        s_num = road_num
                        found_label = 1
                        break
                elif dirc == "WE":
                    if s_lng<=lng and lng<e_lng:
                        s_num = road_num
                        found_label = 1
                        break
                    
        else:
            s_num = -1
    elif '附近' in location:
        pattern = re.compile(r'(.*)附近')
        loc = pattern.match(location)[1]
        
        url = 'http://api.map.baidu.com/geocoding/v3/?address='+ loc +'&output=json&city=北京市&ak=' + aks[0]
        res = requests.get(url).content
        res = json.loads(res)
        if res['status']==0:
            lng = float(res['result']['location']['lng'])
            lat = float(res['result']['location']['lat'])
    
            road_list = road_net[road_name]
            s_num = -1
            for road_num in road_list:
                s_name = road_list[road_num]['s']
                e_name = road_list[road_num]['e']
                s_lng = float(road_loc[road_name][s_name][0])
                s_lat = float(road_loc[road_name][s_name][1])
                e_lng = float(road_loc[road_name][e_name][0])
                e_lat = float(road_loc[road_name][e_name][1])
        
                if s_lat>=lat and lat>e_lat:
                    s_num = road_num
                    found_label = 1
                    break
                elif s_lng<=lng and lng<e_lng:
                    s_num = road_num
                    found_label = 1
                    break
        else:
            s_num = -1
    else:
        s_num = -1
    if found_label == 0:
        return -1
    return s_num


def getWeather(ak,currentTime,path):
    city_ids = ['110101',
            '110102',
            '110105',
            '110106',
            '110107',
            '110108',
            '110109',
            '110111',
            '110112',
            '110113',
            '110114',
            '110115',
            '110116',
            '110117',
            '110118',
            '110119',
           ]
    datas = []
    for city_id in city_ids:
        url = 'http://api.map.baidu.com/weather/v1/?district_id='+city_id+'&data_type=all&ak='+ak
        data = {'name':None,'time':None,'text':None,'temp':None,'rh':None,'wind_class':None,'wind_dir':None}
        res = requests.get(url).content
        res = json.loads(res)
        res = res['result']
        data['name'] = res['location']['name']
        data['time'] = res['now']['uptime']
        data['text'] = res['now']['text']
        data['temp'] = res['now']['temp']
        data['rh'] = res['now']['rh']
        data['wind_class'] = res['now']['wind_class']
        data['wind_dir'] = res['now']['wind_dir']
        datas.append(data)
    columns = ['name','time','text','temp','rh','wind_class','wind_dir']
    # feature = pd.DataFrame(datas)
    # feature.to_csv(path+'/'+currentTime+'_weather.csv',columns=columns)

def getRoad_yq(road_name,ak,currentTime,saveTime):
    TrafficStatusUrl = 'http://api.map.baidu.com/traffic/v1/road?road_name='+road_name+'&city=北京市&ak='+ak
    res = requests.get(url=TrafficStatusUrl).content
    total_json = json.loads(res)
    currentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    direction = 0
    s_num = 0


    if('description' in total_json):

        description = total_json['description']
        road_traffic = total_json['road_traffic'][0]
        ro_len = len(road_traffic)
        
        if(ro_len>1):
            congestion_sections = road_traffic['congestion_sections']
            con_len = len(congestion_sections)
            for i in range(0,con_len):
                congestion_section = congestion_sections[i]
                speed = congestion_section['speed']
                congestion_distance = congestion_section['congestion_distance']
                congestion_trend = congestion_section['congestion_trend']
                section_desc = congestion_section['section_desc']
                print(section_desc)
                document = section_desc
                
                
                if '向' in document:
                    pattern = re.compile(r'(.?)向(.?)')
                    direction1 = pattern.match(document).group(1)
                    direction2 = pattern.match(document).group(2)
                    if direction1 == '南' or direction1 == '东':
                        direction = 0
                    elif direction1 == '北' or direction1 == '西':
                        direction = 1

                if road_name =='京藏高速' or road_name == '京新高速':
                    s_num = getSectionNum(document,road_name,direction)
                    if s_num == -1:
                        return -1
                res = {'time':currentTime,'road_name':road_name,'description':description,'speed':speed,'congestion_distance':congestion_distance,'congestion_trend':congestion_trend,'section_desc':section_desc,'direction':direction,'section_id':s_num}
                return res
        else:
            res = {'time':currentTime,'road_name':road_name,'description':description,'speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
            return res

    else:
        res = {'time':currentTime,'road_name':road_name,'description':'路名有误','speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
        return res


def getRoad_st(road_name,ak,currentTime,saveTime):
    TrafficStatusUrl = 'http://api.map.baidu.com/traffic/v1/road?road_name='+road_name+'&city=北京市&ak='+ak
    res = requests.get(url=TrafficStatusUrl).content
    total_json = json.loads(res)
    currentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    direction = 0
    s_num = 1


    if('description' in total_json):

        description = total_json['description']
        road_traffic = total_json['road_traffic'][0]
        ro_len = len(road_traffic)
        
        if(ro_len>1):
            congestion_sections = road_traffic['congestion_sections']
            con_len = len(congestion_sections)
            for i in range(0,con_len):
                congestion_section = congestion_sections[i]
                speed = congestion_section['speed']
                congestion_distance = congestion_section['congestion_distance']
                congestion_trend = congestion_section['congestion_trend']
                section_desc = congestion_section['section_desc']
                print(section_desc)
                document = section_desc
                
                
                if '向' in document:
                    pattern = re.compile(r'(.?)向(.?)')
                    direction1 = pattern.match(document).group(1)
                    direction2 = pattern.match(document).group(2)
                    if direction1 == '南' or direction1 == '东':
                        direction = 0
                    elif direction1 == '北' or direction1 == '西':
                        direction = 1

                if road_name in roads_st_section:
                    s_num = getSectionNum2(document,road_name,direction)
                    if s_num == -1:
                        return -1
                
                res = {'time':currentTime,'road_name':road_name,'description':description,'speed':speed,'congestion_distance':congestion_distance,'congestion_trend':congestion_trend,'section_desc':section_desc,'direction':direction,'section_id':s_num}
                return res
        else:
            res = {'time':currentTime,'road_name':road_name,'description':description,'speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
            return res

    else:
        res = {'time':currentTime,'road_name':road_name,'description':'路名有误','speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
        return res


def getRoad_wks(road_name,ak,currentTime,saveTime):
    TrafficStatusUrl = 'http://api.map.baidu.com/traffic/v1/road?road_name='+road_name+'&city=北京市&ak='+ak
    res = requests.get(url=TrafficStatusUrl).content
    total_json = json.loads(res)
    currentTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    direction = 0
    s_num = 1


    if('description' in total_json):

        description = total_json['description']
        road_traffic = total_json['road_traffic'][0]
        ro_len = len(road_traffic)
        
        if(ro_len>1):
            congestion_sections = road_traffic['congestion_sections']
            con_len = len(congestion_sections)
            for i in range(0,con_len):
                congestion_section = congestion_sections[i]
                speed = congestion_section['speed']
                congestion_distance = congestion_section['congestion_distance']
                congestion_trend = congestion_section['congestion_trend']
                section_desc = congestion_section['section_desc']
                print(section_desc)
                document = section_desc
                
                
                if '向' in document:
                    pattern = re.compile(r'(.?)向(.?)')
                    direction1 = pattern.match(document).group(1)
                    direction2 = pattern.match(document).group(2)
                    if direction1 == '南' or direction1 == '东':
                        direction = 0
                    elif direction1 == '北' or direction1 == '西':
                        direction = 1

                if road_name in roads_wks_section:
                    s_num = getSectionNum2(document,road_name+'_',direction)
                    if s_num == -1:
                        return -1
                res = {'time':currentTime,'road_name':road_name,'description':description,'speed':speed,'congestion_distance':congestion_distance,'congestion_trend':congestion_trend,'section_desc':section_desc,'direction':direction,'section_id':s_num}
                return res
        else:
            res = {'time':currentTime,'road_name':road_name,'description':description,'speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
            return res

    else:
        res = {'time':currentTime,'road_name':road_name,'description':'路名有误','speed':'NULL','congestion_distance':'NULL','congestion_trend':'NULL','section_desc':'NULL','direction':direction,'section_id':s_num}
        return res


def getInfo(currentTime,path):
    data = []


    start_urls = 'http://service.jtw.beijing.gov.cn/sslk/Web-T_bjjt_new/query.do?serviceType=jam&acode=110000&type=3&cls=0&rcd=40'
    tex = requests.get(url=start_urls,headers=myheaders).text
    pattern = re.compile(r'road\"\:\"(.*?)\"\}\]\}\]\}')
    text = pattern.findall(tex)
    for road in text:
        road = road+"\"}]}"
        #获取地区名
        pattern = re.compile(r'[\u4e00-\u9fa5]+')
        local = pattern.match(road).group(0)
        #获取时间
        pattern = re.compile(r'time\"\:\"(.*?)\"')
        time = pattern.search(road).group(1)
        #获取经纬度
        pattern = re.compile(r'\"lonlat\"\:\[(.*?)\]')
        lonlat = pattern.search(road).group(1)
        if(lonlat!=','):
            lonlat = re.match(r'([0-9]+\.[0-9]+)\,([0-9]+\.[0-9]+)',lonlat)
            lon = float(lonlat.group(1))
            lat = float(lonlat.group(2))
        else:
            lon = 0
            lat = 0
        #分析各个方向
        pattern = re.compile(r'direction\"\:(.*?)\"\}\]\}')
        dir_group = pattern.findall(road)
        for dir in dir_group:
            pattern = re.compile(r'\"(.*?)\"')
            direction = pattern.match(dir).group(1)
            #分析每个方向不同路段
            pattern = re.compile(r'id\"\:(.*?)\。')
            streets = pattern.findall(dir)
            for street in streets:
                #获取起点经纬度
                pattern = re.compile(r'slonlat\"\:\[(.*?)\]\,')
                slonlat = pattern.search(street).group(1)
                if(slonlat!=','):
                    slonlat = re.match(r'([0-9]+\.[0-9]+)\,([0-9]+\.[0-9]+)',slonlat)
                    slon = str(slonlat.group(1))
                    slat = str(slonlat.group(2))
                else:
                    slon = 0
                    slat = 0
                #获取起点名称
                pattern = re.compile(r'sName\"\:\"(.*?)\"')
                sname = pattern.search(street).group(1)
                        
                        
                #获取终点经纬度
                pattern = re.compile(r'elonlat\"\:\[(.*?)\]\,')
                elonlat = pattern.search(street).group(1)
                if(elonlat!=','):
                    elonlat = re.match(r'([0-9]+\.[0-9]+)\,([0-9]+\.[0-9]+)',elonlat)
                    elon = str(elonlat.group(1))
                    elat = str(elonlat.group(2))
                else:
                    elon = 0
                    elat = 0
                #获取终点名称
                pattern = re.compile(r'eName\"\:\"(.*?)\"')
                ename = pattern.search(street).group(1)
                #获取拥堵信息
                pattern = re.compile(r'info\"\:\"(.*)')
                info = pattern.search(street).group(1)
                #获取起点终点距离
                TrafficStatusUrl = 'http://restapi.amap.com/v3/distance?key='+gd_ak+'&origins='+slon+','+slat+'&destination='+elon+','+elat+'&type=1&output=json'
                res = requests.get(url=TrafficStatusUrl).text
                pattern = re.compile(r'distance\"\:\"(.*?)\"')
                length= pattern.search(res).group(1)


                item = {'road_name':'NULL','time':'NULL','rs_length':'NULL','road_center_lon':'NULL','road_center_lat':'NULL','direction':'NULL','start_location':'NULL','slon':'NULL','slat':'NULL','end_location':'NULL','elon':'NULL','elat':'NULL','info':'NULL'}
                item['road_name']=local
                item['time']=time
                item['road_center_lon']=lon
                item['road_center_lat']=lat
                item['direction']=direction
                item['start_location']=sname
                item['slon']=slon
                item['slat']=slat
                item['end_location']=ename
                item['elon']=elon
                item['elat']=elat
                item['info']=info
                item['rs_length']=length

                # 入库
                # 打开数据库连接
                db = pymysql.connect(host='39.99.192.63',
                database='DEMODB',
                port=3306,
                user='devops',
                password='devops',
                charset="utf8",
                use_unicode=True)
                # 使用 cursor() 方法创建一个游标对象 cursor
                cursor = db.cursor()
                # SQL 插入语句
                sql = "INSERT INTO traffic_info (road_name,time,road_length,road_center_slon, \
                    road_center_slat, direction, start_road,start_location_slon,start_location_slat,\
                    end_road,end_location_slon,end_location_slat,info) \
                    VALUES (%s, %s,  %s,  %s,  %s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    
                try:
                    # 执行sql语句
                    cursor.execute("set names utf8;")
                    cursor.execute(sql,(local, time, length, lon, lat,direction,sname,slon,slat,ename,elon,elat,info))
                    # 提交到数据库执行
                    db.commit()
                except pymysql.Error as e:
                    print(e.args[0], e.args[1])
                    print(local, time, length, lon, lat,direction,sname,slon,slat,ename,elon,elat,info)
                    # 如果发生错误则回滚
                    db.rollback()
                # 关闭数据库连接
                db.close()
                data.append(item)
    columns = ['road_name','time','rs_length','road_center_lon','road_center_lat','direction','start_location','slon','slat','end_location','elon','elat','info']
    # feature = pd.DataFrame(data)
    # feature.to_csv(path+'/'+currentTime+'_路况.csv',columns=columns)


def Search(currentTime,path):
    data2 = []
    with open(path+'/'+currentTime+'_路况.csv', 'r') as f:
        reader = csv.reader(f)



        for row in reader:
            if row[1] in roads:
                row_init = {'road_name':'NULL','time':'NULL','rs_length':'NULL','road_center_lon':'NULL','road_center_lat':'NULL','direction':'NULL','start_location':'NULL','slon':'NULL','slat':'NULL','end_location':'NULL','elon':'NULL','elat':'NULL','info':'NULL'}
                row_init['road_name'] = row[1]
                row_init['time']=row[2]
                row_init['road_center_lon']=row[4]
                row_init['road_center_lat']=row[5]
                row_init['direction']=row[6]
                row_init['start_location']=row[7]
                row_init['slon']=row[8]
                row_init['slat']=row[9]
                row_init['end_location']=row[10]
                row_init['elon']=row[11]
                row_init['elat']=row[12]
                row_init['info']=row[13]
                row_init['rs_length']=row[3]
                data2.append(row_init)

    columns = ['road_name','time','rs_length','road_center_lon','road_center_lat','direction','start_location','slon','slat','end_location','elon','elat','info']
    # feature = pd.DataFrame(data2)
    # feature.to_csv(path+'/'+currentTime+'_match_路况.csv',columns=columns)
def job_function():
    cTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    currentTime = time.strftime("%Y_%m_%d___%H_%M_%S", time.localtime())
    day = time.strftime("%Y_%m_%d_",time.localtime())
    path = day+'data'
    # if not os.path.exists(path):
    #     os.mkdir(path)
    
    data = []
    count = 0
    # 入库
    # 打开数据库连接
    db = pymysql.connect(host='39.99.192.63',
    database='DEMODB',
                port=3306,
                user='devops',
                password='devops',
                charset="utf8",
                use_unicode=True)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql1 = "truncate table bd_road_yq"
    cursor.execute(sql1)
    for road in roads_yq:
        road_name=road
        ak = aks[count]
        count+=1
        res = getRoad_yq(road_name,ak,cTime,currentTime)
        if res == -1:
            if count > 3:
                count = 0
            continue
        # SQL 插入语句
        
        sql = "INSERT INTO bd_road_yq (time,road_name,description,speed,congestion_distance,congestion_trend,section_desc,direction,section_id)" \
                    "VALUES (%s, %s,  %s,  %s,  %s,%s,%s,%s,%s)"\
                "ON DUPLICATE KEY UPDATE time=%s,road_name=%s,description=%s,speed=%s,congestion_distance=%s,"\
                    "congestion_trend=%s,section_desc=%s,direction=%s,section_id=%s;"

        try:
            # 执行sql语句
            cursor.execute("set names utf8;")
            cursor.execute(sql,(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id'],res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id']))
            # 提交到数据库执行
            db.commit()
        except pymysql.Error as e:
            print(e.args[0], e.args[1])
            print(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['section_desc'])
            # 如果发生错误则回滚
            db.rollback()
            # 关闭数据库连接
        
        data.append(res)
        if count > 3:
            count = 0
    db.close()
    columns = ['time','road_name','description','speed','congestion_distance','congestion_trend','section_desc']
    # feature = pd.DataFrame(data)
    # feature.to_csv(path+'/'+currentTime+'_bd_路况yq.csv',columns=columns)


    data = []
    count = 0
    # 入库
    # 打开数据库连接
    db = pymysql.connect(host='39.99.192.63',
    database='DEMODB',
                port=3306,
                user='devops',
                password='devops',
                charset="utf8",
                use_unicode=True)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql1 = "truncate table bd_road_st"
    cursor.execute(sql1)
    for road in roads_st:
        road_name=road
        ak = aks[count]
        count+=1
        res = getRoad_st(road_name,ak,cTime,currentTime)
        if res == -1:
            if count > 3:
                count = 0
            continue
        # SQL 插入语句
        
        sql = "INSERT INTO bd_road_st (time,road_name,description,speed,congestion_distance,congestion_trend,section_desc,direction,section_id)" \
                    "VALUES (%s, %s,  %s,  %s,  %s,%s,%s,%s,%s)"\
                "ON DUPLICATE KEY UPDATE time=%s,road_name=%s,description=%s,speed=%s,congestion_distance=%s,"\
                    "congestion_trend=%s,section_desc=%s,direction=%s,section_id=%s;"

        try:
            # 执行sql语句
            cursor.execute("set names utf8;")
            cursor.execute(sql,(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id'],res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id']))
            # 提交到数据库执行
            db.commit()
        except pymysql.Error as e:
            print(e.args[0], e.args[1])
            print(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['section_desc'])
            # 如果发生错误则回滚
            db.rollback()
            # 关闭数据库连接
        
        data.append(res)
        if count > 3:
            count = 0
    db.close()
    columns = ['time','road_name','description','speed','congestion_distance','congestion_trend','section_desc']
    # feature = pd.DataFrame(data)
    # feature.to_csv(path+'/'+currentTime+'_bd_路况st.csv',columns=columns)


    data = []
    count = 0
    # 入库
    # 打开数据库连接
    db = pymysql.connect(host='39.99.192.63',
    database='DEMODB',
                port=3306,
                user='devops',
                password='devops',
                charset="utf8",
                use_unicode=True)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql1 = "truncate table bd_road_wks"
    cursor.execute(sql1)
    for road in roads_wks:
        road_name=road
        ak = aks[count]
        count+=1
        res = getRoad_wks(road_name,ak,cTime,currentTime)
        if res == -1:
            if count > 3:
                count = 0
            continue
        # SQL 插入语句
        
        sql = "INSERT INTO bd_road_wks (time,road_name,description,speed,congestion_distance,congestion_trend,section_desc,direction,section_id)" \
                    "VALUES (%s, %s,  %s,  %s,  %s,%s,%s,%s,%s)"\
                "ON DUPLICATE KEY UPDATE time=%s,road_name=%s,description=%s,speed=%s,congestion_distance=%s,"\
                    "congestion_trend=%s,section_desc=%s,direction=%s,section_id=%s;"

        try:
            # 执行sql语句
            cursor.execute("set names utf8;")
            cursor.execute(sql,(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id'],res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['congestion_trend'],res['section_desc'],res['direction'],res['section_id']))
            # 提交到数据库执行
            db.commit()
        except pymysql.Error as e:
            print(e.args[0], e.args[1])
            print(res['time'],res['road_name'],res['description'],res['speed'],res['congestion_distance'],res['section_desc'])
            # 如果发生错误则回滚
            db.rollback()
            # 关闭数据库连接
        
        data.append(res)
        if count > 3:
            count = 0
    db.close()
    columns = ['time','road_name','description','speed','congestion_distance','congestion_trend','section_desc']
    # feature = pd.DataFrame(data)
    # feature.to_csv(path+'/'+currentTime+'_bd_路况wks.csv',columns=columns)

    getInfo(currentTime,path)
    # Search(currentTime,path)
    getWeather(aks[0],currentTime,path)
    print(currentTime+'done')
    return 1

if __name__ == '__main__':
    today = time.strftime("%Y-%m-%d",time.localtime())
    # sched = BlockingScheduler()

    # sched.add_job(job_function, 'interval', minutes = 15, start_date=today+' 08:00:00', end_date=today+' 20:00:00')

    # sched.start()
    job_function()
