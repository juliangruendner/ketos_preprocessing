#!/bin/bash
pip3 install -r requirements.txt
python3 ../src/api.py
#uwsgi --ini uwsgi.ini