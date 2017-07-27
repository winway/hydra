#! /usr/bin/env python


# dependencies to handle:
# - gevent
# - pymongo
# - apt: python-dev
# - apt: libevent-dev

import os
import os.path
import sys
import string
import multiprocessing
import utils
from utils import die
import copier
from copy_state_db import CopyStateDB
from pymongo.read_preferences import ReadPreference

log = utils.get_logger(__name__)

PARENT_PROCESS_NAME = 'CopyProcess'

#
# child processes
#

def copy_collection_parent(manifests, state_db, args):
    """
    drive the collection copying process by delegating work to a pool of worker processes
    """

    # ensure state db has rows for each source/dest pair
    for manifest in manifests:
        state_db.add_manifest(manifest)

    # space-pad all process names so that tabular output formats line up
    process_names = dict([(repr(manifest), "%s:%s.%s->%s:%s.%s" %
                             (manifest["srchost"], manifest["srcdb"], manifest["srccol"],
                              manifest["desthost"], manifest["destdb"], manifest["destcol"])
                           ) for manifest in manifests])
    process_names['parent'] = PARENT_PROCESS_NAME
    max_process_name_len = max(len(name) for name in process_names.itervalues())
    for key in process_names:
        process_names[key] = string.ljust(process_names[key], max_process_name_len)

    #multiprocessing.current_process().name = process_names['parent']

    # -----------------------------------------------------------------------
    # build indices on main process, since that only needs to be done once
    # -----------------------------------------------------------------------
    waiting_for_indices = len(state_db.select_by_state(CopyStateDB.STATE_WAITING_FOR_INDICES))
    if waiting_for_indices and waiting_for_indices < len(manifests):
        log.warn("prior attempt maybe failed; you can rerun from scratch with --restart")

    if waiting_for_indices > 0:
        log.info("building indices")
        copier.copy_indexes(manifests, args.drop)
        for manifest in manifests:
            state_db.update_state(manifest, CopyStateDB.STATE_INITIAL_COPY)

    # -----------------------------------------------------------------------
    # perform initial copy, if it hasn't been done yet
    # -----------------------------------------------------------------------
    in_initial_copy = len(state_db.select_by_state(CopyStateDB.STATE_INITIAL_COPY))
    if in_initial_copy and in_initial_copy < len(manifests):
        log.warn("prior attempt maybe failed; you can rerun from scratch with --restart")

    if in_initial_copy > 0:
        # each worker process copies one shard
        processes = []
        for manifest in manifests:
            name = process_names[repr(manifest)]
            process = multiprocessing.Process(target=copier.copy_collection,
                                              name=name,
                                              kwargs=dict(manifest=manifest,
                                                          state_path=state_db._path,
                                                          percent=args.percent))
            process.start()
            processes.append(process)


        # wait for all workers to finish
        utils.wait_for_processes(processes)



def main():
    # NOTE: we are not gevent monkey-patched here; only child processes are monkey-patched,
    #       so all ops below are synchronous

    # parse command-line options
    import argparse
    parser = argparse.ArgumentParser(description='Copy collections from one mongod to another. demo:\n'
                                                 'mongo_copier --srchost 192.168.37.12 --srcport 1234 '
                                                 '--desthost 192.168.37.14 --destport 5678 --destuser write --destpwd write '
                                                 '--manifests /tmp/1.txt --restart --drop',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '--srchost', type=str, required=True, metavar='HOST',
        help='hostname or IP of source mongod')
    parser.add_argument(
        '--srcport', type=int, required=True, metavar='PORT',
        help='port of source mongod')
    parser.add_argument(
        '--desthost', type=str, required=True, metavar='HOST',
        help='hostname or IP of destination mongod')
    parser.add_argument(
        '--destport', type=int, required=True, metavar='PORT',
        help='port of destination mongod')
    parser.add_argument(
        '--destuser', type=str, required=True, metavar='USERNAME',
        help='username of destination mongod, must be writable')
    parser.add_argument(
        '--destpwd', type=str, required=True, metavar='PASSWORD',
        help='password of destination mongod')
    parser.add_argument(
        '--percent', type=int, metavar='PCT', default=None,
        help='copy only PCT%% of data')
    parser.add_argument(
        '--drop', action='store_true',
        help='delete destination collection data before copy')
    parser.add_argument(
        '--restart', action='store_true',
        help='restart from the beginning, ignoring any prior progress')
    parser.add_argument(
        '--state-db', type=str, metavar='PATH', default=None,
        help='path to state file (defaults to\n/tmp/mongo_copier_states/<srchost>_<desthost>.db)')
    parser.add_argument(
        '--manifests', type=str, required=True, metavar='FILE',
        help='a file containing collections to copy, one strategy per line.\n'
             'e.g.\n'
             'copy a collection\n'
             '  dbname.colname\n'
             'copy a collection with change destination database or collection name\n'
             '  dbname.colname newdbname.newcolname\n'
             'copy a collection with query\n'
             '  dbname.colname {"id":{"$in":[123,456]}}\n'
             'copy all collections of a database\n'
             '  dbname.*\n'
             'copy all collections of a database with change destination database name\n'
             '  dbname.* newdbname.*\n'
             'copy all collections of a database with query\n'
             '  dbname.* {"id":{"$in":[123,456]}}\n'
             'copy the collection of all databases\n'
             '  *.dbname\n'
             'copy the collection of all databases with change destination collection name\n'
             '  *.dbname *.newdbname\n'
             'copy the collection of all databases with query\n'
             '  *.dbname {"id":{"$in":[123,456]}}\n'
             'copy all collections of all databases\n'
             '  *.*\n'
             'copy all collections of all databases with query\n'
             '  *.* {"id":{"$in":[123,456]}}\n')
    args = parser.parse_args()

    log.debug(args)

    # parse source and destination
    if os.path.exists(args.manifests):
        manifests = utils.parse_manifests_file(args)
    else:
        die(log, "manifests not exist: %s" % manifests)

    # initialize sqlite database that holds our state (this may seem like overkill,
    # but it's actually needed to ensure proper synchronization of subprocesses)
    if not args.state_db:
        args.state_db = '/tmp/mongo_copier_states/%s_%s.db' % (args.srchost, args.desthost)
        if not os.path.isdir("/tmp/mongo_copier_states/"):
            os.mkdir("/tmp/mongo_copier_states/")

    if args.state_db.startswith('/'):
        state_db_path = args.state_db
    else:
        state_db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                     args.state_db)

    log.info('using state db %s' % state_db_path)

    state_db_exists = os.path.exists(state_db_path)
    state_db = CopyStateDB(state_db_path)
    if not state_db_exists or args.restart:
        state_db.drop_and_create()

    # do the real work
    copy_collection_parent(manifests, state_db, args)

if __name__ == "__main__":
    main()
