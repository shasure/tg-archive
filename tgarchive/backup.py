#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author : zsy
Date : 2021/06/21"""
import logging

from .sync import Sync


class Backup:
    def __init__(self, sync):
        self.sync = sync
        self.client = sync.client

    def backup(self):
        # backup all dialogs
        for d in self.client.iter_dialogs():
            self.sync.config['group'] = d.entity  # change sync group target
            self.sync.sync()
            logging.info('-' * 50)
