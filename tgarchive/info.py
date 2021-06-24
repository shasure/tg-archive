import logging
from collections import defaultdict

import pandas as pd
import os


class Info:
    CHANNEL_FILE = "channel.xlsx"
    GROUPUSER_FILE = "groupuser.xlsx"

    def __init__(self, config, db):
        self.config = config
        self.db = db
        self.publish_dir = self.config['publish_dir']
        if not os.path.exists(self.publish_dir):
            os.mkdir(self.publish_dir)

    def extract(self):
        group_id = None
        try:
            group_id = int(self.config['group'])
        except:
            logging.warning("'{} is channel username.'".format(self.config['group']))
            group_id = self.db.get_channel_id_by_username(self.config['group'])
            if not group_id:
                logging.warning("'{} not find in db.'".format(self.config['group']))
                quit(1)
        else:
            if not self.db.check_channel_exists(self.config['group']):
                logging.warning("'{} not find in db.'".format(self.config['group']))
                quit(1)

        self._channel_extract(group_id)
        self._groupuser_extract(group_id)

    def _channel_extract(self, group_id):
        ch_list = list(self.db.query_channel_by_id(group_id))
        if ch_list[0]['linked_chat_id']:
            group_id = ch_list[0]['linked_chat_id']
            logging.warning("'{}' is broadcast channel. export linked group channel info.".format(self.config['group']))
            ch_list.extend(list(self.db.query_channel_by_id(group_id)))
        df = pd.DataFrame(ch_list)
        df['channel_create_date'] = df['channel_create_date'].dt.tz_localize(None)
        df['channel_last_message_date'] = df['channel_last_message_date'].dt.tz_localize(None)
        fn = os.path.join(self.publish_dir, self.CHANNEL_FILE)
        df.to_excel(fn, na_rep="", index=False, )
        logging.info("channel info export to '{}'".format(fn))

    def _groupuser_extract(self, group_id):
        user_list = []
        ch_list = list(self.db.query_channel_by_id(group_id))
        if ch_list[0]['linked_chat_id']:
            group_id = ch_list[0]['linked_chat_id']
            logging.warning("'{}' is broadcast channel. export linked group users.".format(self.config['group']))
        for u in self.db.query_groupuser_by_id(group_id):
            d = defaultdict()
            d['group_id'] = u['group_id']
            d['creator'] = u['creator']
            d['admin'] = u['admin']
            d.update(u['user'])
            user_list.append(d)
        df = pd.DataFrame(user_list)
        fn = os.path.join(self.publish_dir, self.GROUPUSER_FILE)
        df.to_excel(fn, na_rep="", index=False)
        logging.info("groupuser info export to '{}'".format(fn))
