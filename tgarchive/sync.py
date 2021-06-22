from collections import defaultdict
from io import BytesIO
from sys import exit
import json
import logging
import os
import re
import tempfile
import shutil
import time

from jinja2 import Template
from PIL import Image
from telethon.errors import FloodWaitError
from telethon.sync import TelegramClient
import telethon.tl.types
from telethon.tl import functions
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator, InputPeerUser

from . import basedb
from .basedb import BaseDB, Action, User, Message, Media, Channel, GroupUser


class SyncUtils:
    def __init__(self, config, client, db):
        self.config = config
        self.db = db
        self.client = client

    def get_user(self, u, related_col) -> User:
        tags = []
        is_normal_user = isinstance(u, telethon.tl.types.User)

        if is_normal_user:
            if u.bot:
                tags.append("bot")

        if u.scam:
            tags.append("scam")

        if u.fake:
            tags.append("fake")

        avatar_name = None
        # Download sender's profile photo if it's not already cached.
        if self.config["download_avatars"]:
            try:
                avatar_name = self._download_avatar(related_col, u)
            except Exception as e:
                logging.error(
                    "error downloading avatar: #{}: {}".format(u.id, e))

        return User(
            id=u.id,
            username=u.username if u.username else None,
            phone=u.phone if hasattr(u, 'phone') else None,
            bot=u.bot if hasattr(u, 'bot') else None,
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar_name)

    def _download_avatar(self, related_col, user):
        fname = "avatar_{}.jpg".format(user.id)

        logging.info("downloading avatar #{}".format(user.id))

        # Download the file into a container, resize it, and then write to disk.
        b = BytesIO()
        self.client.download_profile_photo(user, file=b)

        f_id = self.db.bytesio_to_gridfs(related_col, b, fname)

        return fname if f_id else None


class Sync:
    """
    Sync iterates and receives messages from the Telegram group to DB.
    """

    def __init__(self, config, session_file, db):
        self.config = config
        self.db = db
        self.group_id = None

        self.client = TelegramClient(
            session_file, self.config["api_id"], self.config["api_hash"])
        self.client.start()

        self.sync_utils = SyncUtils(self.config, self.client, self.db)

    def sync(self, ids=None):
        """
        Sync syncs messages from Telegram from the last synced message
        into the local SQLite DB.
        """
        # get group_id
        self.group_id = self._get_group_id(self.config["group"])

        # check if group_id
        entity = self.client.get_input_entity(self.group_id)
        if isinstance(entity, InputPeerUser):
            logging.warning("'{}' is not a group id. Skip.".format(self.group_id))
            return

        # first sync channel info
        logging.info("start fetching '{}' channel info...".format(self.config["group"]))
        channel_sync = ChannelSync(self.client, self.group_id, self.db)
        channel_sync.sync()

        if ids:
            last_id, last_date = (ids, None)
        else:
            last_id, last_date = self.db.get_last_message_id(self.group_id)

        if ids:
            logging.info("fetching message id={}".format(ids))
        elif last_id:
            logging.info("fetching from last message id={} ({})".format(
                last_id, last_date))

        if self.config['user']:
            logging.info("start fetching '{}' group user info...".format(self.config["group"]))
            groupuser_sync = GroupUserSync(self.config, self.client, self.group_id, self.db)
            groupuser_sync.sync()

        if self.config['message']:
            logging.info("start fetching '{}' messages...".format(self.config["group"]))
            n = 0
            while True:
                has = False
                for m in self._get_messages(
                        offset_id=last_id if last_id else 0,
                        ids=ids):
                    if not m:
                        continue

                    has = True

                    # insert message
                    # Note: user and media has been inserted into db in _get_messages
                    self.db.insert_message(m)

                    last_date = m.date
                    n += 1
                    if n % 300 == 0:
                        logging.info("fetched {} messages".format(n))

                    if self.config["fetch_limit"] > 0 and n >= self.config["fetch_limit"] or ids:
                        has = False
                        break

                if has:
                    last_id = m.message_id
                    logging.info("fetched {} messages. sleeping for {} seconds".format(
                        n, self.config["fetch_wait"]))
                    time.sleep(self.config["fetch_wait"])
                else:
                    break

            logging.info(
                "finished. fetched {} messages. last message = {}".format(n, last_date))

    def _get_messages(self, offset_id, ids=None) -> Message:
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message
        for m in self.client.get_messages(self.group_id, offset_id=offset_id,
                                          limit=self.config["fetch_batch_size"],
                                          ids=ids,
                                          reverse=True):

            if not m or not m.sender:
                continue

            # Media.
            sticker = None
            med = None
            if m.media:
                # If it's a sticker, get the alt value (unicode emoji).
                if isinstance(m.media, telethon.tl.types.MessageMediaDocument) and \
                        hasattr(m.media, "document") and \
                        m.media.document.mime_type == "application/x-tgsticker":
                    alt = [a.alt for a in m.media.document.attributes if isinstance(
                        a, telethon.tl.types.DocumentAttributeSticker)]
                    if len(alt) > 0:
                        sticker = alt[0]
                elif isinstance(m.media, telethon.tl.types.MessageMediaPoll):
                    med = self._make_poll(m)
                else:
                    med = self._get_media(m)

            # Message.
            action = None
            if m.action:
                if isinstance(m.action, telethon.tl.types.MessageActionChatAddUser):
                    if len(m.action.users) == 1 and len(m.action_entities) == 1 and \
                            isinstance(m.action_entities[0], telethon.tl.types.User):
                        self._get_and_insert_user(m.action_entities[0])
                        action = Action(type="user_joined", to_user=m.action_entities[0].id)

                elif isinstance(m.action, telethon.tl.types.MessageActionChatDeleteUser):
                    action = Action(type="user_left", to_user=None)

            yield Message(
                id=self.group_id,
                action=action,
                message_id=m.id,
                date=m.date,
                edit_date=m.edit_date,
                content=sticker if sticker else m.raw_text,
                reply_to=m.reply_to_msg_id if m.reply_to and m.reply_to.reply_to_msg_id else None,
                user=self._get_and_insert_user(m.sender),
                media=med if med else None
            )

    def _get_and_insert_user(self, u) -> int:
        # check if user in db
        if self.db.check_user_exists(u.id):
            return u.id
        user = self.sync_utils.get_user(u, BaseDB.USER)
        self.db.insert_user(user)
        return u.id

    def _make_poll(self, msg):
        options = [{"label": a.text, "count": 0, "correct": False}
                   for a in msg.media.poll.answers]

        total = msg.media.results.total_voters
        if msg.media.results.results:
            for i, r in enumerate(msg.media.results.results):
                options[i]["count"] = r.voters
                options[i]["percent"] = r.voters / total * 100 if total > 0 else 0
                options[i]["correct"] = r.correct

        return Media(
            type="poll",
            url=None,
            title=msg.media.poll.question,
            description=json.dumps(options),
            thumb=None
        )

    def _get_media(self, msg):
        if isinstance(msg.media, telethon.tl.types.MessageMediaWebPage) and \
                not isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
            return Media(
                type="webpage",
                url=msg.media.webpage.url,
                title=msg.media.webpage.title,
                description=msg.media.webpage.description if msg.media.webpage.description else None,
                thumb=None,
            )
        elif isinstance(msg.media, telethon.tl.types.MessageMediaPhoto) or \
                isinstance(msg.media, telethon.tl.types.MessageMediaDocument) or \
                isinstance(msg.media, telethon.tl.types.MessageMediaContact):
            if self.config["download_media"]:
                logging.info("downloading media #{}".format(msg.id))
                media_type = "photo"
                if isinstance(msg.media, telethon.tl.types.MessageMediaDocument):
                    media_type = "document"
                if isinstance(msg.media, telethon.tl.types.MessageMediaContact):
                    media_type = "contact"
                try:
                    basename, fname, thumb = self._download_media(msg)
                    return Media(
                        type=media_type,
                        url=fname,
                        title=basename,
                        description=None,
                        thumb=thumb,
                    )
                except Exception as e:
                    logging.error(
                        "error downloading media: #{}: {}".format(msg.id, e))

    def _download_media(self, msg) -> [str, str, str]:
        """
        Download a media / file attached to a message and return its original
        filename, sanitized name on disk, and the thumbnail (if any). 
        """
        # download media in-memory.
        b = BytesIO()
        b = self.client.download_media(msg, file=b)

        ext = msg.file.ext if msg.file.ext else ""
        basename = msg.file.name if msg.file and msg.file.name else \
            "{}_{}{}".format("media", msg.id, ext)
        newname = "{}_{}{}".format(self.group_id, msg.id, ext)

        self.db.bytesio_to_gridfs(BaseDB.MESSAGE, b, newname)

        # If it's a photo, download the thumbnail.
        tname = None
        if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
            thumb_b = BytesIO()
            thumb_b = self.client.download_media(msg, file=thumb_b, thumb=1)
            tname = "thumb_{}_{}.{}".format(self.group_id, msg.id, ext)

            self.db.bytesio_to_gridfs(BaseDB.MESSAGE, b, tname)

        return basename, newname, tname

    def _get_file_ext(self, f) -> str:
        if "." in f:
            e = f.split(".")[-1]
            if len(e) < 6:
                return e

        return ".file"

    def _get_group_id(self, group):
        """
        Syncs the Entity cache and returns the Entity ID for the specified group,
        which can be a str/int for group ID, group name, or a group username.

        The authorized user must be a part of the group.
        """
        # Get all dialogs for the authorized user, which also
        # syncs the entity cache to get latest entities
        # ref: https://docs.telethon.dev/en/latest/concepts/entities.html#getting-entities
        _ = self.client.get_dialogs()

        try:
            # If the passed group is a group ID, extract it.
            group = int(group)
        except:
            # Not a group ID, we have either a group name or
            # a group username: @group-username
            pass

        try:
            entity = self.client.get_entity(group)
        except ValueError:
            logging.critical("the group: {} does not exist,"
                             " or the authorized user is not a participant!".format(group))
            # This is a critical error, so exit with code: 1
            exit(1)

        return entity.id

    def _bytesio_to_file(self, b, fname):
        with open(fname, "wb") as f:
            f.write(b.getbuffer())

    def _bytesio_to_avatar(self, b, fname):
        im = Image.open(b)
        im.thumbnail(self.config["avatar_size"], Image.ANTIALIAS)
        im.save(fname, "JPEG")


class ChannelSync:
    def __init__(self, client, group_id, db):
        self.client = client
        self.group_id = group_id
        self.db = db

    def sync(self):
        # achieve input channel
        ch_doc_dict = {}
        is_req_success = False
        is_valid = True
        while not is_req_success and is_valid:
            try:
                input_link = self.client.get_input_entity(self.group_id)
                chatfull = self.client(functions.channels.GetFullChannelRequest(input_link))
                ch_doc_dict = self._get_channel_info(self.group_id, chatfull, {})
                is_req_success = True
            except FloodWaitError as e:
                logging.warning("获取channel info: ", e)
                time.sleep(e.seconds)
            except Exception as e:
                logging.warning("获取channel info: ", e)
                is_req_success = True
                is_valid = False
        for ch_doc in ch_doc_dict.values():
            self.db.insert_channel(ch_doc)

    def _get_channel_info(self, link, chatfull, channel_dict):
        # exist condition
        if chatfull.full_chat.id in channel_dict or self.db.check_channel_exists(chatfull.full_chat.id):
            return channel_dict
        ch_dict = defaultdict()
        channel_full = chatfull.full_chat
        channel = next(c for c in chatfull.chats if c.id == channel_full.id)
        ch_dict['id'] = channel.id
        ch_dict['username'] = channel.username if channel.username else ""
        ch_dict['title'] = channel.title if channel.title else ""
        ch_dict['about'] = channel_full.about if channel_full.about else ""
        ch_dict['channel_member_nums'] = channel_full.participants_count
        ch_dict['channel_create_date'] = channel.date
        ch_dict['linked_chat_id'] = channel_full.linked_chat_id if channel_full.linked_chat_id else None

        if channel.broadcast:  # 目标是broadcast channel
            ch_dict['broadcast'] = True
        elif channel.megagroup:  # 目标是group
            ch_dict['megagroup'] = True
        elif channel.gigagroup:
            ch_dict['gigagroup'] = True
        else:
            raise Exception('{} broadcast megagroup gigagroup 同时为False，请手动检查'.format(link))

        is_req_success = False
        last_message = None
        while not is_req_success:
            try:
                last_message = self.client.get_messages(channel_full.id)
                is_req_success = True
            except FloodWaitError as e:
                logging.warning("get last message: ", e)
                time.sleep(e.seconds)
        if last_message:
            ch_dict['channel_last_message_date'] = last_message[0].date

        # add to channel_list
        channel_dict[channel.id] = Channel(**ch_dict)

        if channel_full.linked_chat_id:  # 是否有相关联的group/broadcast
            linked_group = next(c for c in chatfull.chats if c.id == channel_full.linked_chat_id)
            if linked_group.id:
                ch_dict['linked_chat_id'] = linked_group.id
            is_req_success = False
            linked_group_chat_full = None
            while not is_req_success:
                try:
                    linked_group_chat_full = self.client(
                        functions.channels.GetFullChannelRequest(linked_group))
                    # recursive function
                    channel_dict = self._get_channel_info(linked_group.id, linked_group_chat_full, channel_dict)
                    is_req_success = True
                except FloodWaitError as e:
                    logging.warning("get linked group fullchannel: ", e)
                    time.sleep(e.seconds)

        return channel_dict


class GroupUserSync:
    def __init__(self, config, client, group_id, db):
        self.config = config
        self.client = client
        self.group_id = group_id
        self.db = db
        self.sync_utils = SyncUtils(self.config, self.client, self.db)

    def sync(self):
        # 判断是group还是broadcast channel
        full = self.client(functions.channels.GetFullChannelRequest(self.group_id))
        full_channel = full.full_chat  # full_channel is a ChannelFull
        channel = next(c for c in full.chats if c.id == full_channel.id)
        if channel.broadcast:  # 目标是broadcast channel
            logging.info('{}是broadcast channel，订阅人数 {}'.format(self.group_id, full_channel.participants_count))  # subscriber人数
            if full_channel.linked_chat_id:  # 是否有相关联的group
                linked_group = next(c for c in full.chats if c.id == full_channel.linked_chat_id)
                logging.info('关联group的title: {}  username: {}'.format(linked_group.title, linked_group.username))
                self.group_id = full_channel.linked_chat_id  # 更换拉取members的目标
            else:
                logging.info('broadcast channel {} 没有相关联的group'.format(self.group_id))
                return
        elif channel.megagroup:  # 目标是group
            logging.info('{}是group，group人数 {}'.format(self.group_id, full_channel.participants_count))
        else:
            raise Exception('{} broadcast megagroup同时为False，请手动检查'.format(self.group_id))

        # 获取members
        users = self.client.get_participants(self.group_id)  # list of User
        logging.info('获取member {} 人'.format(len(users)))

        # insert to db
        if self.config['groupsuser_remove_before_sync']:
            self.db.del_groupuser_by_groupid(self.group_id)
        for user in users:
            u = self._get_groupuser(user)
            self.db.insert_groupuser(u)

    def _get_groupuser(self, u) -> GroupUser:
        creator = None
        admin = None
        if isinstance(u.participant, ChannelParticipantCreator):  # 是否是创建者
            creator = True
        if isinstance(u.participant, ChannelParticipantAdmin):  # 是否是admin
            admin = True

        user = self.sync_utils.get_user(u, BaseDB.GROUPUSER)

        return GroupUser(group_id=self.group_id, user=user, creator=creator, admin=admin)


