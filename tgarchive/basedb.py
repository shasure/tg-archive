from collections import namedtuple
from dataclasses import dataclass
import datetime

import typing


@dataclass
class Channel:
    id: int
    username: str
    title: str
    about: str
    channel_member_nums: int = None
    channel_create_date: datetime.datetime = None
    channel_last_message_date: datetime.datetime = None
    broadcast: bool = False
    megagroup: bool = False
    gigagroup: bool = False
    linked_chat_id: 'typing.Any' = None

    def __post_init__(self):
        if isinstance(self.linked_chat_id, dict):
            self.linked_chat_id = Channel(**self.linked_chat_id)


@dataclass
class User:
    id: int
    username: str
    phone: str
    bot: bool
    first_name: str
    last_name: str
    tags: list
    avatar: str


@dataclass
class GroupUser:
    group_id: int
    user: User
    creator: bool = False
    admin: bool = False

@dataclass
class Action:
    type: str
    to_user: 'typing.Any'

    def __post_init__(self):
        if isinstance(self.to_user, dict):
            self.to_user = User(**self.to_user)


@dataclass
class Media:
    type: str
    url: str
    title: str
    description: str
    thumb: str


@dataclass
class Message:
    group_id: int
    id: int
    action: Action
    date: datetime.datetime
    edit_date: datetime.datetime
    content: str
    reply_to: int
    user: 'typing.Any'
    media: Media

    def __post_init__(self):
        if isinstance(self.user, dict):
            self.user = User(**self.user)
        if isinstance(self.media, dict):
            self.media = Media(**self.media)


Month = namedtuple("Month", ["date", "slug", "label", "count"])

Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])


class BaseDB:
    TG_ARCHIVE_DB = 'tg_archive'
    MESSAGE = 'message'
    CHANNEL = 'channel'
    USER = 'user'
    GROUPUSER = 'groupuser'
