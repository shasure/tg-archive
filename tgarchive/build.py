import logging
import math
import os
import re
import shutil
from collections import OrderedDict, deque

import pkg_resources
from feedgen.feed import FeedGenerator
from jinja2 import Template

from . import _TEMPLATE_HTML, _STATIC, __version__
from .basedb import *

_NL2BR = re.compile(r"\n\n+")


class Build:
    config = {}
    template = None
    db = None

    def __init__(self, config, db):
        self.config = config
        self.db = db

        # Map of all message IDs across all months and the slug of the page
        # in which they occur (paginated), used to link replies to their
        # parent messages that may be on arbitrary pages.
        # should clean these map after build() if build will call many times
        self.page_ids = {}
        self.timeline = OrderedDict()

        self.bp_user_id = None

    def build(self):
        # get backup user if backup
        if hasattr(self.config, 'bp_user') and self.config['bp_user']:
            self.bp_user_id = self.config['bp_user']

        # (Re)create the output directory.
        self._create_publish_dir()

        try:
            group_id = int(self.config['group'])
        except:
            logging.warning("'{}' is username, try to lookup username from db.".format(self.config['group']))
            group_id = self.db.get_channel_id_by_username(self.config['group'])
        if not group_id:
            logging.warning("'{}' not find in database. Please execute sync first.".format(self.config['group']))
            quit(1)

        timeline = list(self.db.get_timeline(group_id, self.bp_user_id))
        if len(timeline) == 0:
            logging.info("'{}' no data found to publish site".format(group_id))
            return

        for month in timeline:
            if month.date.year not in self.timeline:
                self.timeline[month.date.year] = []
            self.timeline[month.date.year].append(month)

        # Queue to store the latest N items to publish in the RSS feed.
        rss_entries = deque([], self.config["rss_feed_entries"])
        fname = None
        for month in timeline:
            # Get the days + message counts for the month.
            dayline = OrderedDict()
            for d in self.db.get_dayline(group_id, self.bp_user_id, month.date.year, month.date.month, self.config["per_page"]):
                dayline[d.slug] = d

            # Paginate and fetch messages for the month until the end..
            page = 0
            last_id = 0
            total = self.db.get_message_count(
                group_id, self.bp_user_id, month.date.year, month.date.month)
            total_pages = math.ceil(total / self.config["per_page"])

            while True:
                messages = list(self.db.get_messages(group_id, self.bp_user_id, month.date.year, month.date.month,
                                                     last_id, self.config["per_page"]))

                if len(messages) == 0:
                    break

                last_id = messages[-1].message_id
                page += 1
                fname = self.make_filename(month, page)

                # Collect the message ID -> page name for all messages in the set
                # to link to replies in arbitrary positions across months, paginated pages.
                for m in messages:
                    self.page_ids[m.message_id] = fname
                    # Extract media from MongoDB to Native file system
                    publish_media_dir = os.path.join(self.config["publish_dir"], self.config["media_dir"])

                    avatar_fn = os.path.join(publish_media_dir, m.user.avatar) if m.user and m.user.avatar else None
                    if avatar_fn and not os.path.exists(avatar_fn):
                        self.db.gridfs_avatar_to_nativafs(BaseDB.USER, self.config["avatar_size"],
                                                          m.user.avatar, avatar_fn)

                    media_fn = os.path.join(publish_media_dir, m.media.url) if m.media and m.media.url else None
                    if media_fn and not os.path.exists(media_fn):
                        self.db.gridfs_media_to_nativafs(BaseDB.MESSAGE, m.media.url, media_fn)

                    thumb_fn = os.path.join(publish_media_dir, m.media.thumb) if m.media and m.media.thumb else None
                    if thumb_fn and not os.path.exists(thumb_fn):
                        self.db.gridfs_media_to_nativafs(BaseDB.MESSAGE, m.media.thumb, thumb_fn)

                if self.config["publish_rss_feed"]:
                    rss_entries.extend(messages)

                self._render_page(messages, month, dayline,
                                  fname, page, total_pages)

        # The last page chronologically is the latest page. Make it index.
        if fname:
            shutil.copy(os.path.join(self.config["publish_dir"], fname),
                        os.path.join(self.config["publish_dir"], "index.html"))

        # Generate RSS feeds.
        if self.config["publish_rss_feed"]:
            self._build_rss(rss_entries, "index.rss", "index.atom")

        # must clean if build() be called many times
        self._clean()

    def load_template(self, fname):
        if fname:
            template_fn = fname
        else:
            exdir = os.path.join(os.path.dirname(__file__), "example")
            if not os.path.isdir(exdir):
                logging.error("unable to find bundled example directory")
                quit(1)

            template_fn = os.path.join(exdir, _TEMPLATE_HTML)
            if not os.path.exists(template_fn):
                logging.error("{} not found in package.".format(template_fn))
                quit(1)

        with open(template_fn, "r", encoding="utf-8") as f:
            self.template = Template(f.read())

    def make_filename(self, month, page) -> str:
        fname = "{}{}.html".format(
            month.slug, "_" + str(page) if page > 1 else "")
        return fname

    def _render_page(self, messages, month, dayline, fname, page, total_pages):
        # group id to group name
        if self.config['group']:
            res_id = self.db.get_channel_username_by_id(self.config['group'])
            if res_id:
                self.config['group'] = res_id
        html = self.template.render(config=self.config,
                                    timeline=self.timeline,
                                    dayline=dayline,
                                    month=month,
                                    messages=messages,
                                    page_ids=self.page_ids,
                                    pagination={"current": page,
                                                "total": total_pages},
                                    make_filename=self.make_filename,
                                    nl2br=self._nl2br)

        with open(os.path.join(self.config["publish_dir"], fname), "w", encoding="utf-8") as f:
            f.write(html)

    def _build_rss(self, messages, rss_file, atom_file):
        version = __version__
        try:
            version = pkg_resources.get_distribution("tg-archive").version
        except:
            pass
        f = FeedGenerator()
        f.id(self.config["site_url"])
        f.generator(
            "tg-archive {}".format(version))
        f.link(href=self.config["site_url"], rel="alternate")
        f.title(self.config["site_name"].format(group=self.config["group"]))
        f.subtitle(self.config["site_description"])

        for m in messages:
            url = "{}/{}#{}".format(self.config["site_url"],
                                    self.page_ids[m.message_id], m.message_id)
            e = f.add_entry()
            e.id(url)
            e.title("@{} on {} (#{})".format(m.user.username, m.date, m.message_id))
            e.description(self._make_abstract(m))

            if m.media and m.media.url:
                murl = "{}/{}/{}".format(self.config["site_url"],
                                         os.path.basename(self.config["media_dir"]), m.media.url)
                e.enclosure(murl, 0, "application/octet-stream")

            f.rss_file(os.path.join(self.config["publish_dir"], "index.xml"))
            f.atom_file(os.path.join(
                self.config["publish_dir"], "index.atom"))

    def _make_abstract(self, m):
        out = m.content
        if not out and m.media:
            out = m.media.title
        return out if out else ""

    def _nl2br(self, s) -> str:
        # There has to be a \n before <br> so as to not break
        # Jinja's automatic hyperlinking of URLs.
        return _NL2BR.sub("\n\n", s).replace("\n", "\n<br />")

    def _create_publish_dir(self):
        pubdir = self.config["publish_dir"]

        # Clear the output directory.
        if os.path.exists(pubdir):
            shutil.rmtree(pubdir)

        # Re-create the output directory.
        os.mkdir(pubdir)

        target_dir = os.path.join(pubdir, self.config["static_dir"])
        static_dir = os.path.join(os.path.dirname(__file__), "example", _STATIC)
        if not os.path.exists(static_dir):
            logging.error("unable to find bundled example/static directory")
            quit(1)
        # Copy the static directory into the output directory.
        for f in [static_dir]:
            if os.path.isfile(f):
                shutil.copyfile(f, target_dir)
            else:
                shutil.copytree(f, target_dir)

        # create media_dir in pubdir
        os.mkdir(os.path.join(pubdir, os.path.basename(self.config["media_dir"])))

    def _clean(self):
        self.page_ids = {}
        self.timeline = OrderedDict()
        self.bp_user_id = None
