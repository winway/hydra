import time
import gevent
import gevent.monkey
from gevent.pool import Pool
import utils
from pymongo.errors import BulkWriteError
from pymongo.read_preferences import ReadPreference
from copy_state_db import CopyStateDB
from faster_ordered_dict import FasterOrderedDict
from utils import auto_retry, log_exceptions, squelch_keyboard_interrupt

log = utils.get_logger(__name__)

INSERT_SIZE = 250
INSERT_POOL_SIZE = 20

#
# Copy collection
#

class Stats(object):
    def __init__(self):
        self.start_time = self.adj_start_time = time.time()
        self.inserted = 0
        self.total_docs = None
        self.duplicates = 0 # not a true count of duplicates; just an exception count
        self.exceptions = 0
        self.retries = 0

    def log(self, adjusted=False):
        start_time = self.adj_start_time if adjusted else self.start_time
        qps = int(float(self.inserted) / (time.time() - start_time))
        pct = int(float(self.inserted)/self.total_docs*100.0)
        log.info("%d%% | %d / %d copied | %d/sec | %d dupes | %d exceptions | %d retries" % 
                 (pct, self.inserted, self.total_docs, qps, self.duplicates,
                  self.exceptions, self.retries))


@auto_retry
def _find_and_insert_batch_worker(source_collection, dest_collection, ids, stats):
    """
    greenlet responsible for copying a set of documents
    """

    # read documents from source
    cursor = source_collection.find({'_id': {'$in': ids}})
    cursor.batch_size(len(ids))
    docs = [doc for doc in cursor]

    # perform copy as a single batch
    ids_inserted = []
    try:
        ids_inserted = dest_collection.insert_many(docs, ordered=False).inserted_ids
    except BulkWriteError:
        # this isn't an exact count, but it's more work than it's worth to get an exact
        # count of duplicate _id's
        stats.duplicates += 1
    stats.inserted += len(ids_inserted)


def _copy_stats_worker(stats):
    """
    Periodically print stats relating to the initial copy.
    """
    while True:
        stats.log()
        gevent.sleep(1)


@log_exceptions
@squelch_keyboard_interrupt
def copy_collection(manifest, state_path, percent):
    """
    Copies all documents from source to destination collection. Inserts documents in
    batches using insert workers, which are each run in their own greenlet.

    Does no safety checks -- this is up to the caller.

    @param manifest    dict of (srchost, srcport, srcuser, srcpwd, srcdb, srccol,
                                desthost, destport, destuser, destpwd, destdb, destcol)
    @param state_path  path of state database
    @param percent     percentage of documents to copy
    """
    gevent.monkey.patch_socket()

    # open state database
    state_db = CopyStateDB(state_path)

    # connect to mongo
    source_client = utils.mongo_connect(manifest['srchost'], manifest['srcport'],
                                        manifest['srcuser'], manifest['srcpwd'],
                                        maxPoolSize=30,
                                        read_preference=ReadPreference.SECONDARY,
                                        document_class=FasterOrderedDict)

    source_collection = source_client[manifest['srcdb']][manifest['srccol']]
    if source_client.is_mongos:
        raise Exception("for performance reasons, sources must be mongod instances; %s:%d is not",
                        manifest['srchost'], source['srcport'])

    dest_client = utils.mongo_connect(manifest['desthost'], manifest['destport'],
                                      manifest['destuser'], manifest['destpwd'],
                                      maxPoolSize=30,
                                      document_class=FasterOrderedDict)
    dest_collection = dest_client[manifest['destdb']][manifest['destcol']]

    # for testing copying of indices quickly
    if percent == 0:
        log.info("skipping copy because of --percent 0 parameters")
        state_db.update_state(manifest, CopyStateDB.STATE_APPLYING_OPLOG)
        return

    stats = Stats()
    stats.total_docs = int(source_collection.count(filter=manifest["query"]))
    if percent:
        # hack-ish but good enough for a testing-only feature
        stats.total_docs = int(stats.total_docs * (float(percent)/100.0))

    # get all _ids, which works around a mongo bug/feature that causes massive slowdowns
    # of long-running, large reads over time
    ids = []
    cursor = source_collection.find(filter=manifest["query"], projection={"_id":True}, no_cursor_timeout=False)
    cursor.batch_size(5000)
    insert_pool = Pool(INSERT_POOL_SIZE)
    stats_greenlet = gevent.spawn(_copy_stats_worker, stats)
    for doc in cursor:
        _id = doc['_id']

        if percent is not None and not utils.id_in_subset(_id, percent):
            continue

        # when we've gathered enough _ids, spawn a worker greenlet to batch copy the
        # documents corresponding to them
        ids.append(_id)
        if len(ids) % INSERT_SIZE == 0:
            outgoing_ids = ids
            ids = []
            insert_pool.spawn(_find_and_insert_batch_worker,
                              source_collection=source_collection,
                              dest_collection=dest_collection,
                              ids=outgoing_ids,
                              stats=stats)
        gevent.sleep()

    # insert last batch of documents
    if len(ids) > 0:        
        _find_and_insert_batch_worker(source_collection=source_collection,
                                      dest_collection=dest_collection,
                                      ids=ids,
                                      stats=stats)
        stats.log()

    # wait until all other outstanding inserts have finished
    insert_pool.join()
    stats_greenlet.kill()

    srccount = stats.total_docs
    destcount = dest_collection.count(filter=manifest["query"])
    if srccount == destcount:
        log.info("COPY SUCCEED. srccount(%d) == destcount(%d)" % (srccount, destcount))
    else:
        log.error("COPY FAILED. srccount(%d) != destcount(%d)" % (srccount, destcount))

    state_db.update_state(manifest, CopyStateDB.STATE_APPLYING_OPLOG)

    # yeah, we potentially leak connections here, but that shouldn't be a big deal


def copy_indexes(manifests, drop):
    """
    Copies all indexes from source to destination, preserving options such as unique
    and sparse.

    @param manifest    dict of (srchost, srcport, srcuser, srcpwd, srcdb, srccol,
                                desthost, destport, destuser, destpwd, destdb, destcol)
    @param drop        drop destination collection before create index
    """
    for manifest in manifests:
        # connect to mongo instances
        source_client = utils.mongo_connect(manifest['srchost'], manifest['srcport'],
                                            manifest['srcuser'], manifest['srcpwd'],
                                            maxPoolSize=1,
                                            read_preference=ReadPreference.SECONDARY)
        source_collection = source_client[manifest['srcdb']][manifest['srccol']]

        dest_client = utils.mongo_connect(manifest['desthost'], manifest['destport'],
                                          manifest['destuser'], manifest['destpwd'], maxPoolSize=1)
        dest_collection = dest_client[manifest['destdb']][manifest['destcol']]

        if dest_collection.count(filter=manifest["query"]) > 0:
            if drop:
                if manifest["query"] == {}:
                    log.info("drop destination collection: %s.%s" % (manifest['destdb'], manifest['destcol']))
                    dest_collection.drop()
                else:
                    log.info("delete destination collection: %s.%s [%s]" % (manifest['destdb'], manifest['destcol'], manifest['query']))
                    dest_collection.delete_many(filter=manifest["query"])
            else:
                log.warn("destination collection is not empty: %s.%s" % (manifest['destdb'], manifest['destcol']))

        # copy indices
        for name, index in source_collection.index_information().items():
            kwargs = { 'name': name }
            index_key = None
            for k, v in index.items():
                if k in ['unique', 'sparse']:
                    kwargs[k] = v
                elif k in ['v', 'ns', 'background']:
                    continue
                elif k == 'key':
                    # sometimes, pymongo will give us floating point numbers, so let's make sure
                    # they're ints instead
                    index_key = [(field, int(direction)) for (field, direction) in v]
                else:
                    raise NotImplementedError("don't know how to handle index info key %s" % k)
                # TODO: there are other index options that probably aren't handled here

            assert index_key is not None
            log.info("ensuring index on %s (options = %s)", index_key, kwargs)
            dest_collection.create_index(index_key, **kwargs)
