#!/usr/bin/python

import ConfigParser
import logging
from optparse import OptionGroup, OptionParser
import os
import sys
import time

from dsa_mq.connection import Connection

default_cfg = os.path.expanduser('~/.pubsub.conf')
# Get comand line args
description = """Designed for easy submission of messages
to an MQ topic, for example from a git commit hook.

Will accept messages being passed either on the command line
or on standard input."""

epilog = """If you have not talked to DSA about credentials,
This will not work.  Please go do so now.
"""

parser = OptionParser(description=description,
                      epilog=epilog)

parser.add_option("-d", "--debug",
                  action="store_true", default=False,
                  help="turn on debug output")

cfg = OptionGroup(parser, "Configuration options",
"""Defaults to trying to read from ~/.pubsub.conf.
Overridden by command line arguments.
Standard INI format.""")
cfg.add_option("-c", "--config",
               default=default_cfg,
                  help="config file for app")
cfg.add_option("-s", "--section",
               default='DEFAULT',
               help="section in config file")
parser.add_option_group(cfg)

auth = OptionGroup(parser, "Authentication Options",
                   "You should put these in the config file instead")
auth.add_option("-u", "--username",
                  help="authentication username")
auth.add_option("-p", "--password",
                  help="authentication password")
auth.add_option("-v", "--vhost",
                  help="vhost for connection")
parser.add_option_group(auth)

msging = OptionGroup(parser, "Message Options",
                     "How to send your message")
msging.add_option("-t", "--topic",
                  help="topic to publish on")
msging.add_option("-e", "--exchange",
                  help="exchange to publish to")
msging.add_option("-m", "--message",
                  help="message body")
parser.add_option_group(msging)

(options, args) = parser.parse_args()

# And config file defaults
config = ConfigParser.ConfigParser()
config.read(options.config)

# Set up logging
FORMAT = "%(asctime)-15s %(message)s"
lvl = logging.INFO
if options.debug:
    lvs = logging.DEBUG

logging.basicConfig(format=FORMAT, level=lvl)

LOG = logging.getLogger(__name__)

# Sanity checks
def get_value(opt):

    ret = None
    try:
        ret = config.get(options.section, opt)
    except ConfigParser.NoOptionError:
        pass

    return getattr(options, opt) or ret

for item in ['username', 'password', 'vhost', 'exchange', 'topic']:
    if get_value(item) is None:
        print("%s cannot be absent!" % item)
        sys.exit(1)

conf = {
    'rabbit_userid': get_value('username'),
    'rabbit_password': get_value('password'),
    'rabbit_virtual_host': get_value('vhost'),
    'rabbit_hosts': ['pubsub02.debian.org', 'pubsub01.debian.org'],
    'use_ssl': False
}

# read from stdin if no message passed on command line
msg = options.message
if not msg:
    msg = sys.stdin.read()

if not msg:
    print 'No message, aborting'
    sys.exit(1)

# Send message
ret = 0
conn = Connection(conf=conf)
try:
    conn.topic_send(get_value('topic'),
                    msg,
                    exchange_name=get_value('exchange'),
                    timeout=5)

except Exception, e:
    LOG.error("Error sending: %s" % e)
    ret = 1
finally:
    conn.close()

sys.exit(ret)
