#!/bin/bash
pip3 install -r requirements.txt
python3 api.py
#uwsgi --ini uwsgi.ini