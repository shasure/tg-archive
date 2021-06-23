import logging


class Backup:
    def __init__(self, sync):
        self.sync = sync
        self.client = sync.client

    def backup(self):
        # save 'me' in db.BACKUPUSER
        self.sync.save_me()
        if self.sync.config['all']:
            logging.info("start backup all dialogs in '{}'".format(self.sync.me.id))
            # backup all dialogs
            for d in self.client.iter_dialogs():
                self.sync.config['group'] = d.entity.id  # change sync group target
                self.sync.sync(add_me=True)
                logging.info('-' * 80)
        elif self.sync.config['group']:
            logging.info("start backup '{}' in '{}'".format(self.sync.config['group'], self.sync.me.id))
            self.sync.sync(add_me=True)
