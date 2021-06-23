import logging
import os


class Export:
    def __init__(self, build):
        self.build = build
        self.publish_dir = self.build.config['publish_dir']
        self.chat_list = None
        self.channel_list = None
        if not os.path.exists(self.publish_dir):
            os.mkdir(self.publish_dir)

    def export(self):
        if not self.build.config['bp_user']:
            logging.warning("backup user to export not specified, please use --bp_user.")
            quit(1)

        try:
            bp_user_id = int(self.build.config['bp_user'])
        except:
            bp_user_id = self.build.db.get_bpuser_id_by_username(self.build.config['bp_user'])
            if not bp_user_id:
                logging.warning("'{}' not exists in backup users.".format(self.build.config['bp_user']))
                quit(1)
        else:
            if not self.build.db.check_backupuser_exists(bp_user_id):
                logging.warning("'{}' not exists in backup users.".format(self.build.config['bp_user']))
                quit(1)

        # set bp_user id
        self.build.config['bp_user'] = bp_user_id

        os.chdir(self.publish_dir)  # change dir

        if self.build.config['all']:
            self._export_chat(bp_user_id)
            self._export_channel(bp_user_id)
        else:
            self._export_chat(bp_user_id, self.build.config['group'])
            self._export_channel(bp_user_id, self.build.config['group'])

    def _export_chat(self, bp_user_id, peer_id=None):
        self.chat_list = self.build.db.get_chat_id_by_owner_id(bp_user_id)
        if peer_id and peer_id in self.chat_list:
            self._build(peer_id)
        else:
            for chat_id in self.chat_list:
                self._build(chat_id)

    def _export_channel(self, bp_user_id, peer_id=None):
        self.build.config['bp_user'] = None  # unset bp_user
        self.channel_list = self.build.db.get_group_id_by_owner_id(bp_user_id)
        if peer_id and peer_id in self.channel_list:
            self._build(peer_id)
        else:
            for chat_id in self.channel_list:
                self._build(chat_id)

    def _build(self, id):
        self.build.config['group'] = id
        self.build.config['publish_dir'] = str(id)
        self.build.load_template("")
        self.build.build()
