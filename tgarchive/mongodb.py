import json
import logging
import os.path
from dataclasses import asdict
from typing import Iterator

import gridfs
import pymongo
import pytz as pytz
from PIL import Image
from gridfs import GridFS
from pymongo import MongoClient

from .basedb import *


class MongoDB(BaseDB):

    def __init__(self, conn_str, db_timezone):
        self.db_timezone = db_timezone
        try:
            tzinfo = pytz.timezone(db_timezone)
            self.conn = MongoClient(conn_str, tz_aware=True, tzinfo=tzinfo, serverSelectionTimeoutMS=5000)
        except:
            logging.error("Mongodb {} connect failed.".format(conn_str))
            raise
        # get db
        self.db = self.conn[self.TG_ARCHIVE_DB]
        # get col
        self.message_col = self.db[self.MESSAGE]
        self.channel_col = self.db[self.CHANNEL]
        self.user_col = self.db[self.USER]
        self.groupuser_col = self.db[self.GROUPUSER]
        # grid fs
        self.gfs = {BaseDB.MESSAGE: GridFS(self.db, collection=BaseDB.MESSAGE),
                    BaseDB.CHANNEL: GridFS(self.db, collection=BaseDB.CHANNEL),
                    BaseDB.USER: GridFS(self.db, collection=BaseDB.USER),
                    BaseDB.GROUPUSER: GridFS(self.db, collection=BaseDB.GROUPUSER)}

        self._create_index()

    def _create_index(self):
        # unique index
        self.message_col.create_index([("group_id", pymongo.ASCENDING), ("id", pymongo.ASCENDING)], unique=True)
        self.user_col.create_index([("id", pymongo.ASCENDING)], unique=True)
        self.channel_col.create_index([("id", pymongo.ASCENDING), ("username", pymongo.ASCENDING)], unique=True)
        self.groupuser_col.create_index([("group_id", pymongo.ASCENDING), ("user.id", pymongo.ASCENDING)], unique=True)

    def get_last_message_id(self, group_id) -> [int, datetime]:
        last_message = self.message_col.find({'group_id': group_id}, {"id": 1, "date": 1}) \
            .sort([('id', pymongo.DESCENDING)]).limit(1)
        last_message_l = list(last_message)
        if not last_message_l:
            return 0, None

        last_message = last_message_l[0]
        id, date = last_message['id'], last_message['date']
        return id, date

    def get_timeline(self, group_id) -> Iterator[Month]:
        """
        Get the list of all unique yyyy-mm month groups and
        the corresponding message counts per period in chronological order.
        """
        m_cursor = self.message_col.aggregate([
            {"$match": {"group_id": group_id}},
            {
                "$group": {
                    "_id": {
                        "timestamp": {"$dateToString":
                                          {"format": "%Y-%m", "date": "$date", "timezone": self.db_timezone}
                                      },
                    },
                    "count": {"$sum": 1},
                    "date": {"$first": "$date"},
                    "id": {"$first": "$id"},
                }
            },
            {"$sort": {"id": 1}},
            {"$project": {"count": 1, "date": 1}}
        ])
        for r in m_cursor:
            yield Month(date=r['date'],
                        slug=r['date'].strftime("%Y-%m"),
                        label=r['date'].strftime("%b %Y"),
                        count=r['count'])

    def get_dayline(self, group_id, year, month, limit=500) -> Iterator[Day]:
        """
        Get the list of all unique yyyy-mm-dd days corresponding
        message counts and the page number of the first occurrence of
        the date in the pool of messages for the whole month.
        """
        m_curor = self.message_col.aggregate([
            {"$project": {"date": 1, "_id": 0}},
            {"$match":
                {
                    "$expr": {
                        "$and": [
                            {"group_id": group_id},
                            {"$eq":
                                 ["{}{:02d}".format(year, month),
                                  {
                                      "$dateToString": {
                                          "date": "$date",
                                          "format": "%Y%m",
                                          "timezone": self.db_timezone
                                      }
                                  }]
                             }
                        ]
                    }
                }
            },
            {"$set": {
                "rank": {
                    "$function": {
                        "body": "function() {try {row_number+= 1;} catch (e) {row_number= 1;}return row_number;}",
                        "args": [],
                        "lang": "js"
                    }
                }
            }
            },
            {"$group": {
                "_id": {
                    "timestamp": {"$dateToString":
                                      {"format": "%Y-%m-%d 00:00:00", "date": "$date", "timezone": self.db_timezone}
                                  },
                },
                "count": {"$sum": 1},
                "page": {
                    "$first": {
                        "$ceil": {"$toInt": {"$divide": ["$rank", limit]}}
                    }
                },
                "date": {"$first": "$date"}
            }},
            {"$sort": {"date": 1}},
        ])
        for r in m_curor:
            yield Day(date=r['date'],
                      slug=r['date'].strftime("%Y-%m-%d"),
                      label=r['date'].strftime("%d %b %Y"),
                      count=r['count'],
                      page=r['page'])

    def get_messages(self, group_id, year, month, last_id=0, limit=500) -> Iterator[Message]:
        # date = "{}{:02d}".format(year, month)

        m_cursor = self.message_col.aggregate([
            {"$match":
                {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$group_id", group_id]},
                            {"$eq":
                                 ["{}{:02d}".format(year, month),
                                  {
                                      "$dateToString": {
                                          "date": "$date",
                                          "format": "%Y%m",
                                          "timezone": self.db_timezone
                                      }
                                  }]
                             },
                            {"$gt": ["$id", last_id]}
                        ]
                    }
                }
            },
            {"$sort": {"id": 1}},
            {"$limit": limit},
            {"$lookup": {
                "from": "user",
                "localField": "user",
                "foreignField": "id",
                "as": "user"
            }},
            {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}},
            {"$lookup": {
                "from": "user",
                "localField": "action.to_user",
                "foreignField": "id",
                "as": "action.to_user"
            }},
            {"$unwind": {"path": "$action.to_user", "preserveNullAndEmptyArrays": True}},
            {"$project": {"_id": 0, "user._id": 0}}
        ])

        for r in m_cursor:
            yield self._make_message(r)

    def get_message_count(self, group_id, year, month) -> int:
        date = "{}{:02d}".format(year, month)

        m_cursor = self.message_col.aggregate([
            {"$match":
                {
                    "$expr": {
                        "$and": [
                            {"group_id": group_id},
                            {"$eq":
                                 [date,
                                  {
                                      "$dateToString": {
                                          "date": "$date",
                                          "format": "%Y%m",
                                          "timezone": self.db_timezone
                                      }
                                  }]
                             }
                        ]
                    }
                }
            },
            {"$group": {
                "_id": "null",
                "count": {"$sum": 1}
            }}
        ])

        return list(m_cursor)[0]['count']

    def insert_user(self, u: User):
        """Insert a user and if they exist, update the fields."""
        self.user_col.update_one({'id': u.id}, {"$set": asdict(u)}, upsert=True)

    def insert_message(self, m: Message):
        self.message_col.update({'group_id': m.group_id, 'id': m.id}, {"$set": asdict(m)}, upsert=True)

    def insert_channel(self, ch: Channel):
        self.channel_col.update({'id': ch.id}, {"$set": asdict(ch)}, upsert=True)

    def del_groupuser_by_groupid(self, gid):
        self.groupuser_col.delete_many({'group_id': gid})

    def insert_groupuser(self, gu: GroupUser):
        self.groupuser_col.update({'group_id': gu.group_id, 'id': gu.user.id}, {"$set": asdict(gu)}, upsert=True)

    def commit(self):
        """Commit pending writes to the DB."""
        self.conn.commit()

    def _make_message(self, m) -> Message:
        """Makes a Message() object from an SQL result tuple."""
        if m['media']:
            if m['media']['type'] == "poll":
                m['media']['description'] = json.loads(m['media']['description'])
        res = Message(**m)
        return res

    def bytesio_to_gridfs(self, related_col, b, filename):
        if not b.seek(0, os.SEEK_END):
            return None
        b.seek(0, os.SEEK_SET)
        gridfs_col = self.gfs[related_col]
        filter_condition = {"filename": filename}
        file_ = None

        if gridfs_col.exists(filter_condition):
            logging.info("'{}' exist in GridFS".format(filename))
        else:
            file_ = gridfs_col.put(data=b, **filter_condition)
        return file_

    def gridfs_avatar_to_nativafs(self, related_col, avatar_size, fin, fout, ver=-1):
        gridfs_col = self.gfs[related_col]

        try:
            grid_out = gridfs_col.get_version(filename=fin, version=ver)
            im = Image.open(grid_out)
            im.thumbnail(avatar_size, Image.ANTIALIAS)
            im.save(fout, "JPEG")
        except gridfs.errors.NoFile:
            pass
        except:
            raise

    def gridfs_media_to_nativafs(self, related_col, fin, fout, ver=-1):
        gridfs_col = self.gfs[related_col]

        try:
            grid_out = gridfs_col.get_version(filename=fin, version=ver)
            with open(fout, 'wb') as file_w:
                file_w.write(grid_out.read())
        except gridfs.errors.NoFile:
            pass
        except:
            raise

    def check_user_exists(self, uid):
        user_iter = self.user_col.find({'id': uid})
        user = list(user_iter)
        if user:
            return True
        return False

    def check_channel_exists(self, cid):
        user_iter = self.channel_col.find({'id': cid})
        user = list(user_iter)
        if user:
            return True
        return False

    def get_channel_id_by_username(self, ch_username):
        user_iter = self.channel_col.find({'username': ch_username})
        user = list(user_iter)
        if user:
            return user[0]['id']
        return None


