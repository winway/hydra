import gc
import re
import sys
import gevent
import logging
import pymongo
import signal
import functools
from pymongo.errors import AutoReconnect, ConnectionFailure, OperationFailure, NetworkTimeout
from pymongo.read_preferences import ReadPreference
from faster_ordered_dict import FasterOrderedDict

loggers = {}
def get_logger(name):
    """
    get a logger object with reasonable defaults for formatting

    @param name used to identify the logger (though not currently useful for anything)
    """
    global loggers
    if name in loggers:
        return loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s:%(processName)s] %(message)s",
                                      "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(sh)

    loggers[name] = logger
    return logger

log = get_logger("utils")

def die(logger, msg):
    logger.error(msg)
    sys.exit(1)

def mongo_connect(host, port, user, password, secondary_only=False, maxPoolSize=1,
                  socketTimeoutMS=None, w=0, read_preference=None, document_class=dict,
                  replicaSet=None, slave_okay=None):
    """
    Same as MongoClient.connect, except that we are paranoid and ensure that cursors
    # point to what we intended to point them to. Also sets reasonable defaults for our
    needs.

    @param host            host to connect to
    @param port            port to connect to

    most other keyword arguments mirror those for pymongo.MongoClient
    """
    options = dict(
        host=host,
        port=port,
        socketTimeoutMS=socketTimeoutMS,
        maxPoolSize=maxPoolSize,
        w=1,
        document_class=document_class)
    if replicaSet is not None:
        options['replicaSet'] = replicaSet
    if read_preference is not None:
        options['read_preference'] = read_preference
    if slave_okay is not None:
        options['slave_okay'] = slave_okay
    client = pymongo.MongoClient(**options)

    client.admin.authenticate(user, password)
    return client


def parse_mongo_url(url):
    """
    Takes in pseudo-URL of form

    host[:port]/db/collection (e.g. localhost/prod_maestro/emails)

    and returns a dictionary containing elements 'host', 'port', 'db', 'collection'
    """
    try:
        host, db, collection = url.split('/')
    except ValueError:
        raise ValueError("urls be of format: host[:port]/db/collection")

    host_tokens = host.split(':')
    if len(host_tokens) == 2:
        host = host_tokens[0]
        port = int(host_tokens[1])
    elif len(host_tokens) == 1:
        port = 27017
    elif len(host_tokens) > 2:
        raise ValueError("urls be of format: host[:port]/db/collection")

    return dict(host=host, port=port, db=db, collection=collection)


def _source_file_syntax():
    print "--manifests file must be of the following format:"
    print "*.* [ {'a':1}]"
    print "a.* [ {'a':1}]"
    print "*.a [ {'a':1}]"
    print "a.* b.* [ {'a':1}]"
    print "*.a *.b [ {'a':1}]"
    print "a.b [ {'a':1}]"
    print "a.b c.d [ {'a':1}]"
    sys.exit(1)

def parse_manifests_file(args):
    """
    parses an input file passed to the --manifests parameter as a list of dict that contain
    these fields:

    srchost
    srcport
    srcuser: default read
    srcpwd: default Bizdev_readonly
    srcdb
    srccol
    desthost
    destport
    destuser
    destpwd
    destdb
    destcol
    query: default None
    """
    manifests = []

    with open(args.manifests, "r") as source_file:
        for source in [line.strip() for line in source_file]:
            if re.match(r"^(\S+)\.(\S+)\s+(\S+)\.(\S+)\s*(\{.*\})?$", source):
                srcdb, srccol, destdb, destcol, query  = re.match(r"(\S+)\.(\S+)\s+(\S+)\.(\S+)\s*({.*})*", source).groups()
            elif re.match(r"^(\S+)\.(\S+)\s*(\{.*\})?$", source):
                srcdb, srccol, query, destdb, destcol = re.match(r"(\S+)\.(\S+)\s*({.*})?", source).groups() + (None, None)
            else:
                _source_file_syntax()

            if query:
                query = eval(query)
            else:
                query = {}

            log.debug("raw manifests: %s.%s -> %s.%s [%s]" % (srcdb, srccol, destdb, destcol, query))

            for manifest in generate_manifests(args.srchost, args.srcport, "read", "Bizdev_readonly",
                                               args.desthost, args.destport, args.destuser, args.destpwd,
                                               srcdb=srcdb, srccol=srccol, destdb=destdb, destcol=destcol, query=query):
                manifests.append(manifest)

    # dedup manifests
    new_manifests = []
    for i in manifests:
        if i not in new_manifests:
            new_manifests.append(i)

    if len(manifests) != len(new_manifests):
        log.warn("there are some duplication, deduped")

    for i in new_manifests:
        log.info("formated manifests: %s.%s -> %s.%s [%s]" % (i["srcdb"], i["srccol"], i["destdb"], i["destcol"], i["query"]))

    return new_manifests

def generate_manifests(srchost, srcport, srcuser, srcpwd,
                       desthost, destport, destuser, destpwd,
                       srcdb=None, srccol=None, destdb=None, destcol=None, query={}):
    # connect to mongo
    source_client = mongo_connect(srchost, srcport, srcuser, srcpwd,
                                  maxPoolSize=30, read_preference=ReadPreference.SECONDARY,
                                  document_class=FasterOrderedDict)

    if srcdb == '*' and srccol == '*' and destdb == None and destcol == None:
        for db in source_client.database_names():
            if db in ["admin", "local", "test"]:
                continue
            for col in source_client[db].collection_names():
                if col.startswith("system."):
                    continue
                yield dict(srchost=srchost, srcport=srcport,
                           srcuser=srcuser, srcpwd=srcpwd,
                           srcdb=db, srccol=col,
                           desthost=desthost, destport=destport,
                           destuser=destuser, destpwd=destpwd,
                           destdb=db, destcol=col, query=query)
    elif srcdb != '*' and srccol == '*':
        for db in source_client.database_names():
            if db in ["admin", "local", "test"] or db != srcdb:
                continue
            for col in source_client[db].collection_names():
                if col.startswith("system."):
                    continue
                if destdb == None and destcol == None:
                    _destdb, _destcol= db, col
                elif destdb != '*' and destcol == '*':
                    _destdb, _destcol= destdb, col
                else:
                    die(log, "illegal manifest format: (%s.%s -> %s.%s [%s])" % (srcdb, srccol, destdb, destcol, query))
                yield dict(srchost=srchost, srcport=srcport,
                           srcuser=srcuser, srcpwd=srcpwd,
                           srcdb=db, srccol=col,
                           desthost=desthost, destport=destport,
                           destuser=destuser, destpwd=destpwd,
                           destdb=_destdb, destcol=_destcol, query=query)
    elif srcdb == '*' and srccol != '*':
        for db in source_client.database_names():
            if db in ["admin", "local", "test"]:
                continue
            for col in source_client[db].collection_names():
                if col.startswith("system.") or col != srccol:
                    continue
                if destdb == None and destcol == None:
                    _destdb, _destcol = db, col
                elif destdb == '*' and destcol != '*':
                    _destdb, _destcol = db, destcol
                else:
                    die(log, "illegal manifest format: (%s.%s -> %s.%s [%s])" % (srcdb, srccol, destdb, destcol, query))
                yield dict(srchost=srchost, srcport=srcport,
                           srcuser=srcuser, srcpwd=srcpwd,
                           srcdb=db, srccol=col,
                           desthost=desthost, destport=destport,
                           destuser=destuser, destpwd=destpwd,
                           destdb=_destdb, destcol=_destcol, query=query)
    elif srcdb != '*' and srccol != '*':
        for db in source_client.database_names():
            if db in ["admin", "local", "test"] or db != srcdb:
                continue
            for col in source_client[db].collection_names():
                if col.startswith("system.") or col != srccol:
                    continue
                if destdb == None and destcol == None:
                    _destdb, _destcol = db, col
                elif destdb != '*' and destcol != '*':
                    _destdb, _destcol = destdb, destcol
                else:
                    die(log, "illegal manifest format: (%s.%s -> %s.%s [%s])" % (srcdb, srccol, destdb, destcol, query))
                yield dict(srchost=srchost, srcport=srcport,
                           srcuser=srcuser, srcpwd=srcpwd,
                           srcdb=db, srccol=col,
                           desthost=desthost, destport=destport,
                           destuser=destuser, destpwd=destpwd,
                           destdb=_destdb, destcol=_destcol, query=query)
    else:
        die(log, "illegal manifest format: (%s.%s -> %s.%s [%s])" % (srcdb, srccol, destdb, destcol, query))

def get_last_oplog_entry(client):
    """
    gets most recent oplog entry from the given pymongo.MongoClient
    """
    oplog = client['local']['oplog.rs']
    cursor = oplog.find().sort('$natural', pymongo.DESCENDING).limit(1)
    docs = [doc for doc in cursor]
    if not docs:
        raise ValueError("oplog has no entries!")
    return docs[0]


def tune_gc():
    """
    normally, GC is too aggressive; use kmod's suggestion for tuning it
    """
    gc.set_threshold(25000, 25, 10)


def id_in_subset(_id, pct):
    """
    Returns True if _id fits in our definition of a "subset" of documents.
    Used for testing only.
    """
    return (hash(_id) % 100) < pct


def trim(s, prefixes, suffixes):
    """
    naive function that trims off prefixes and suffixes
    """
    for prefix in prefixes:
        if s.startswith(prefix):
            s = s[len(prefix):]

    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[:-len(suffix)]

    return s


def log_exceptions(func):
    """
    logs exceptions using logger, which includes host:port info
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'stats' in kwargs:
            # stats could be a keyword arg
            stats = kwargs['stats']
        elif len(args) > 0:
            # or the last positional arg
            stats = args[-1]
            if not hasattr(stats, 'exceptions'):
                stats = None
        else:
            # or not...
            stats = None

        try:
            return func(*args, **kwargs)
        except SystemExit:
            # just exit, don't log / catch when we're trying to exit()
            raise
        except:
            log.exception("uncaught exception")
            # increment exception counter if one is available to us
            if stats:
                stats.exceptions += 1
    return wrapper


def squelch_keyboard_interrupt(func):
    """
    suppresses KeyboardInterrupts, to avoid stack trace explosion when pressing Control-C
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)
    return wrapper


def wait_for_processes(processes):
    try:
        [process.join() for process in processes]
    except KeyboardInterrupt:
        # prevent a frustrated user from interrupting our cleanup
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # if a user presses Control-C, we still need to terminate child processes and join()
        # them, to avoid zombie processes
        for process in processes:
            process.terminate()
            process.join()
        log.error("exiting...")
        sys.exit(1)


def auto_retry(func):
    """
    decorator that automatically retries a MongoDB operation if we get an AutoReconnect
    exception

    do not combine with @log_exceptions!!
    """
    MAX_RETRIES = 20  # yes, this is sometimes needed
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # try to keep track of # of retries we've had to do
        if 'stats' in kwargs:
            # stats could be a keyword arg
            stats = kwargs['stats']
        elif len(args) > 0:
            # or the last positional arg
            stats = args[-1]
            if not hasattr(stats, 'retries'):
                stats = None
                log.warning("couldn't find stats")
        else:
            # or not...
            stats = None
            log.warning("couldn't find stats")

        failures = 0
        while True:
            try:
                return func(*args, **kwargs)
            except (AutoReconnect, ConnectionFailure, OperationFailure, NetworkTimeout):
                failures += 1
                if stats:
                    stats.retries += 1
                if failures >= MAX_RETRIES:
                    log.exception("FAILED after %d retries", MAX_RETRIES)
                    if stats:
                        stats.exceptions += 1
                    raise
                gevent.sleep(2 * failures)
                log.exception("retry %d after exception", failures)
    return wrapper
