#!/bin/bash

conda init bash \
    && . ~/.bashrc \
    && conda activate dbconnect \
    && gunicorn --bind 0.0.0.0:5001 wsgi:app \
    