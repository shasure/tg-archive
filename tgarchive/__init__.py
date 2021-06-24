import argparse
import logging
import os
import shutil
import sys
import yaml

from .mongodb import MongoDB

__version__ = "0.3.6.1"

logging.basicConfig(format="%(asctime)s: %(message)s",
                    level=logging.INFO)

_CONFIG = {
    "api_id": "",
    "api_hash": "",
    "group": "",
    "download_avatars": True,
    "avatar_size": [64, 64],
    "download_media": False,
    "media_dir": "media",
    "fetch_batch_size": 2000,
    "fetch_wait": 5,
    "fetch_limit": 0,

    "publish_rss_feed": True,
    "rss_feed_entries": 100,

    "publish_dir": "site",
    "site_url": "https://mysite.com",
    "static_dir": "static",
    "telegram_url": "https://t.me/{id}",
    "per_page": 1000,
    "show_sender_fullname": False,
    "site_name": "@{group} (Telegram) archive",
    "site_description": "Public archive of @{group} Telegram messages.",
    "meta_description": "@{group} {date} Telegram message archive.",
    "page_title": "{date} - @{group} Telegram message archive."
}

_CONFIG_YAML = "config.yaml"
_STATIC = "static"
_TEMPLATE_HTML = "template.html"


def get_config(path, args):
    config = {}
    # config priority : args > config.yaml > _CONFIG
    with open(path, "r") as f:
        config = {**_CONFIG, **yaml.safe_load(f.read())}
    # update config dict from args
    if hasattr(args, 'publish_dir') and args.publish_dir:
        config['publish_dir'] = args.publish_dir
    if hasattr(args, 'group') and args.group:
        config['group'] = args.group
    if hasattr(args, 'bp_user'):
        config['bp_user'] = args.bp_user
    if hasattr(args, 'all'):
        config['all'] = args.all
    config['user'] = args.user if hasattr(args, 'user') else None
    config['message'] = args.message if hasattr(args, 'message') else None

    return config


def main():
    """Run the CLI."""
    p = argparse.ArgumentParser(
        description="A tool for exporting and archiving Telegram groups to webpages.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-c", "--config", action="store", type=str, default="config.yaml",
                   dest="config", help="path to the config file")
    p.add_argument("-d", "--data", action="store", type=str, default="mongodb://localhost:27017/",
                   dest="data", help="path to the SQLite data file to store messages")
    p.add_argument("-g", "--group", action="store", type=str, default="",
                   dest="group", help="Telegram channel / group name or id to import. Group should be public group or "
                                      "your user account that was used to creat the API ID should be a member of this"
                                      " group.")
    p.add_argument("-se", "--session", action="store", type=str, default="session.session",
                   dest="session", help="path to the session file")
    p.add_argument("-v", "--version", action="store_true", dest="version", help="display version")

    # sub command
    subparsers = p.add_subparsers(help="subcommand")

    n = subparsers.add_parser("new", help="initialize a new site")
    n.set_defaults(cmd='new')  # set cmd to identity sub command
    n.add_argument("-p", "--path", action="store", type=str, default="",
                   dest="path", help="path to create config.yaml file")

    s = subparsers.add_parser("sync", help="sync data from telegram group to the local DB")
    s.set_defaults(cmd='sync')
    s.add_argument("-u", "--user", action="store_true",
                   dest="user", help="sync group user from telegram group to the local DB")
    s.add_argument("-m", "--message", action="store_true",
                   dest="message", help="sync group message from telegram group to the local DB")
    s.add_argument("-m_id", "--message_id", action="store", type=int, nargs="+",
                   dest="message_id", help="sync (or update) data for specific message ids")

    b = subparsers.add_parser("build", help="build the static site of channel/group")
    b.set_defaults(cmd='build')
    b.add_argument("-t", "--template", action="store", type=str, default="",
                   dest="template", help="path to the template file. If empty, use default template.html")
    b.add_argument("-pub", "--publish_dir", action="store", type=str, default="",
                   dest="publish_dir", help="path to the output directory")

    i = subparsers.add_parser("info", help="export channel or groupuser info")
    i.set_defaults(cmd='info')
    i.add_argument("-pub", "--publish_dir", action="store", type=str, default="",
                   dest="publish_dir", help="path to the output directory")

    bp = subparsers.add_parser("backup", help="backup all dialogs in current telegram account")
    bp.set_defaults(cmd='backup')
    bp.set_defaults(user=True)
    bp.set_defaults(message=True)
    bp.add_argument("-a", "--all", action="store_true",
                    dest="all", help="backup all dialogs in current telegram account")

    e = subparsers.add_parser("export", help="build the static site of user")
    e.set_defaults(cmd='export')
    e.add_argument("-bp_u", "--bp_user", action="store", type=str, required=True,
                   dest="bp_user", help="the user to export")
    e.add_argument("-a", "--all", action="store_true",
                    dest="all", help="export current telegram account associcated dialogs in db")
    e.add_argument("-pub", "--publish_dir", action="store", type=str, default="",
                   dest="publish_dir", help="path to the output directory")

    args = p.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.version:
        print("v{}".format(__version__))
        quit()

    # Setup new site.
    elif args.cmd == "new":
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logging.error("unable to find bundled example directory")
            quit(1)

        if args.path and not os.path.exists(args.path):
            os.mkdir(args.path)
        dst_fp = os.path.join(args.path, _CONFIG_YAML) if args.path else _CONFIG_YAML
        if os.path.exists(dst_fp):
            logging.error("file {} already exists.".format(dst_fp))
            quit(1)
        try:
            shutil.copyfile(os.path.join(exdir, _CONFIG_YAML), dst_fp)
        except FileExistsError:
            logging.error(
                "copyfile '{}' to '{}' failed".format(_CONFIG_YAML, dst_fp))
            quit(1)
        except:
            raise

        logging.info("create config file '{}'".format(dst_fp))

    # Sync from Telegram.
    elif args.cmd == "sync":
        # Import because the Telegram client import is quite heavy.
        from .sync import Sync

        cfg = get_config(args.config, args)
        logging.info("starting Telegram sync (batch_size={}, limit={}, wait={})".format(
            cfg["fetch_batch_size"], cfg["fetch_limit"], cfg["fetch_wait"]
        ))

        try:
            Sync(cfg, args.session, MongoDB(args.data, cfg['db_timezone'])).sync(args.message_id)
        except KeyboardInterrupt as e:
            logging.info("sync cancelled manually")
            quit()
        except:
            raise

    # Build static site.
    elif args.cmd == "build":
        from .build import Build

        logging.info("building site")
        cfg = get_config(args.config, args)
        b = Build(cfg, MongoDB(args.data, cfg['db_timezone']))
        b.load_template(args.template)
        b.build()

        logging.info("published to directory '{}'".format(cfg['publish_dir']))

    elif args.cmd == "info":
        from .info import Info
        cfg = get_config(args.config, args)
        logging.info("starting extract channel and group users info.")
        Info(cfg, MongoDB(args.data, cfg['db_timezone'])).extract()

        logging.info("published channel / user info to directory '{}'".format(cfg['publish_dir']))

    elif args.cmd == "backup":
        from .backup import Backup
        from .sync import Sync

        cfg = get_config(args.config, args)
        logging.info("starting Telegram sync (batch_size={}, limit={}, wait={})".format(
            cfg["fetch_batch_size"], cfg["fetch_limit"], cfg["fetch_wait"]
        ))

        try:
            s = Sync(cfg, args.session, MongoDB(args.data, cfg['db_timezone']))
            Backup(s).backup()
        except KeyboardInterrupt as e:
            logging.info("sync cancelled manually")
            quit()
        except:
            raise

    elif args.cmd == "export":
        from .export import Export
        from .build import Build

        logging.info("building site")
        cfg = get_config(args.config, args)
        export_dir = cfg['publish_dir']
        b = Build(cfg, MongoDB(args.data, cfg['db_timezone']))
        Export(b).export()

        logging.info("published to directory '{}'".format(export_dir))
