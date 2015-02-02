#!/bin/bash
cd /home/pentaho/new-etl/
py="/usr/local/bin/python2.7"

# AO
$py runner.py -p consumos -f fct_consumos -c 128-zapweb -t 129-dwao --now
$py runner.py -p parque -c 128-zapweb -t 129-dwao --now
$py runner.py -p tmpparque -c 128-zapweb -t 129-dwao --now

# mz
$py runner.py -p consumos -f fct_consumos -c 128-zapwmz -t 74-zapwmz --now
$py runner.py -p consumos -f fct_consumos -c 128-zapwmz -t 129-dwmz --now
$py runner.py -p parque -c 128-zapwmz -t 74-zapwmz --now
$py runner.py -p parque -c 128-zapwmz -t 129-dwmz --now
$py runner.py -p tmpparque -c 128-zapwmz -t 129-dwmz --now

# tvcabo
$py runner.py -p tvcabo -c 129-wholesale -t 129-dwao --now
