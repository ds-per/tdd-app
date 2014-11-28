# -*- coding: utf-8 -*-

import logging
from logging import FileHandler, StreamHandler
from os.path import join

# Loggin stuff
baselogdir = ""
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

console_handler = StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

prev_handler = FileHandler(join(baselogdir, "previsao.log"), "a")
prev_handler.setLevel(logging.DEBUG)
prev_handler.setFormatter(formatter)

handler = FileHandler(join(baselogdir, "etl-sync.log"), "a")
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)

error_handler = FileHandler(join(baselogdir, "error.log"), "a")
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

root = logging.getLogger()
root.addHandler(console_handler)
root.addHandler(handler)
root.addHandler(error_handler)

prev = logging.getLogger("previsao")
prev.addHandler(prev_handler)


# Databases

DATABASES = {
    "129-dwao": "mysql+pymysql://root:pinkfloyd@172.16.5.129/dwao",
    "129-dwmz": "mysql+pymysql://root:pinkfloyd@172.16.5.129/dwmz",
    "128-zapweb": "mysql+pymysql://root:pinkfloyd@172.16.5.128/zapweb",
    "128-zapwmz": "mysql+pymysql://root:pinkfloyd@172.16.5.128/zapwmz",
    "74-zapweb": "mysql://root:risingstar@172.16.5.74/zapweb",
    "128": "mysql+pymysql://root:pinkfloyd@172.16.5.128",
    "129": "mysql+pymysql://root:pinkfloyd@172.16.5.129",
}


DATE_FMT = '%Y-%m-%d'

# Deprecated settings

DATE_FORMAT = '%Y-%m-%d'
MES = ['NULL','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']
DIA_SEMANA = ['NULL','Segunda','Terca','Quarta','Quinta','Sexta',u'Sabado','Domingo']
TRIMESTRE = ['NULL','Q1','Q2','Q3','Q4']
TRIMESTREID = [1,1,1,2,2,2,3,3,3,4,4,4]
#class fields on the database has type smallint which max size is 65535
DCLASS = [0,3,6,12,24,48,59999]
INTERVENCAO_CLASS = [0,24,48,72,144,288,1000,59999]
CONSUMOS_CLASS = [0,1,2,3,4,5,10,15,30,60,90,180,360,59999]
