===============================
Mediaflow Analytics Core
===============================

.. image:: https://badge.fury.io/py/analyticsengine.png
    :target: http://badge.fury.io/py/analyticsengine
    
.. image:: https://travis-ci.org/sarink/analyticsengine.png?branch=master
        :target: https://travis-ci.org/sarink/analyticsengine

.. image:: https://pypip.in/d/analyticsengine/badge.png
        :target: https://pypi.python.org/pypi/analyticsengine


Mediaflow Analytics Core collects, processes and stores counters and config data from multiple Mediaflow devices.

checkout the repo
    git clone

    cd analyticsengine

create virtual environment and source it.
    virtualenv --no-site-packages venv
    source venv/bin/activate

install the dependencies
    pip install -r requirements.txt

start celery workers
    celery -A analyticsengine.celeryapp.celery worker -l debug

start the daemon 
    python daemon.py

* Documentation: http://analyticsengine.readthedocs.org.

Features
--------

* TODO
