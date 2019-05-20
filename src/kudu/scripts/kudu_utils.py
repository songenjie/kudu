#! /usr/bin/env python
# coding=utf-8

import datetime
import dateutil.relativedelta
from git import Repo
import logging
from logging.handlers import RotatingFileHandler
import os

git_repo_dir = ''
LOG = logging.getLogger()

def init_log(path):
    handler = RotatingFileHandler('%s/kudu.log' % path,
                                  mode='a',
                                  maxBytes=100*1024*1024,
                                  backupCount=10)
    handler.setFormatter(
        logging.Formatter(
            fmt='%(asctime)s [%(thread)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'))
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)


def make_dir(path):
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno != os.errno.EEXIST:
            raise
        pass


def script_path():
    return os.path.split(os.path.realpath(__file__))[0]


def prepare_pricing_data_path(last_month):
    time = datetime.datetime.now()
    if last_month:
        time += dateutil.relativedelta.relativedelta(months=-1)
    data_path = script_path() + '/year=' + time.strftime('%Y')
    make_dir(data_path)
    data_path += '/month=' + time.strftime('%m')
    make_dir(data_path)
    return data_path


def push_file_to_repo(filenames):
    repo = Repo(git_repo_dir)
    assert not repo.bare

    remote = repo.remote()
    remote.pull()

    index = repo.index
    index.add(filenames)
    index.commit('Kudu add statistics files')

    remote.push()

    LOG.info('Pushed files %s to repo' % str(filenames))


g_script_path = script_path()
init_log(g_script_path)
if not os.path.exists(git_repo_dir + '/.git'):
    LOG.fatal('You must set `git_repo_dir` to a valid directory contains `.git`')
    exit(1)
