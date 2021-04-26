import psutil
import os, datetime, time
import json
import pymysql
import random
import numpy as np
from django.shortcuts import HttpResponse
from django.http import JsonResponse
from edge_node.apps.spiders.road import job_function
from edge_node.apps.ner.predict_span import predict,init
from edge_node.apps.spiders.event import get_event_yingjiju, get_event_bendibao, get_event_jiaoguanju, get_event_bus
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


def get_cpu_state(request):

    data = psutil.virtual_memory()
    total = data.total  # 总内存,单位为byte
    free = data.available  # 可以内存
    memory = (int(round(data.percent)))
    cpu = psutil.cpu_percent(interval=1)
    ret = {'memory': memory, 'cpu': cpu}
    # js = json.dumps(ret)
    return JsonResponse(ret)


def get_road_info(request):
    state = 0
    state = job_function()
    if state == 1:
        res = {'code': 200, 'text': '获取路况信息成功'}
        return JsonResponse(res)
    else:
        res = {'code': 400, 'text': '获取路况信息失败'}
        return JsonResponse(res)

road_info_state = 0
sched = 0


def get_road_info_state(request):
    global road_info_state
    return JsonResponse({'road_info_state':road_info_state})


def road_info_switch(request):
    global road_info_state
    global sched
    if road_info_state == 0:
        today = time.strftime("%Y-%m-%d",time.localtime())
        sched = BackgroundScheduler()
        sched.add_job(job_function, 'interval', minutes=15, start_date=today+' 08:00:00', end_date=today+' 20:00:00')
        sched.start()
        road_info_state = 1
        res = {'code': 200, 'text': '定时获取路况开启', 'state': road_info_state}
        return JsonResponse(res)
    if road_info_state == 1:
        sched.shutdown()
        road_info_state = 0
        res = {'code': 200, 'text': '定时获取路况关闭', 'state': road_info_state}
        return JsonResponse(res)


tokenizer, label_list, model, device, id2label = init()


def event_ner(request):
    input_text = "决定2020年8月12日至2020年9月10日期间，宫门口西岔(安平巷—阜成门内大街)采取禁止机动车由南向北方向行驶交通管理措施。"
    input_text = "决定2020年8月12日至2020年9月10日期间，半壁街（厂洼中路——西三环北路）禁止社会车辆及行人通行，"
    res = predict(input_text, tokenizer, label_list, model, device, id2label)

    return JsonResponse(res)


def getYingjiju(request):
    res = get_event_yingjiju()
    return JsonResponse(res, safe=False)


def getBendibao(request):
    res = get_event_bendibao()
    return JsonResponse(res, safe=False)


def getJiaoguanju(request):
    res = get_event_jiaoguanju()
    return JsonResponse(res, safe=False)


def getBus(request):
    res = get_event_bus()
    return JsonResponse(res, safe=False)