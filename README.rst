Copy collections from one mongod to another.
============================================
**demo:**

``mongo_copier --srchost 10.149.40.42 --srcport 1234 --desthost 10.153.58.50 --destport 5678 --destuser write --destpwd write --manifests /tmp/1.txt --restart --drop``

*optional arguments:*
  -h, --help           show this help message and exit
  --srchost HOST       hostname or IP of source mongod
  --srcport PORT       port of source mongod
  --desthost HOST      hostname or IP of destination mongod
  --destport PORT      port of destination mongod
  --destuser USERNAME  username of destination mongod, must be writable
  --destpwd PASSWORD   password of destination mongod
  --percent PCT        copy only PCT% of data
  --drop               delete destination collection data before copy
  --restart            restart from the beginning, ignoring any prior progress
  --state-db PATH      path to state file (defaults to
                       /tmp/mongo_copier_states/<srchost>_<desthost>.db)
  --manifests FILE     a file containing collections to copy, one strategy per line.
                       e.g.
    copy a collection
      dbname.colname
    copy a collection with change destination database or collection name
      dbname.colname newdbname.newcolname
    copy a collection with query
      dbname.colname {"aid":{"$in":[123,456]}}
    copy all collections of a database
      dbname.*
    copy all collections of a database with change destination database name
      dbname.* newdbname.*
    copy all collections of a database with query
      dbname.* {"aid":{"$in":[123,456]}}
    copy the collection of all databases
      \*.dbname
    copy the collection of all databases with change destination collection name
      \*.dbname \*.newdbname
    copy the collection of all databases with query
      \*.dbname {"aid":{"$in":[123,456]}}
    copy all collections of all databases
      \*.\*
    copy all collections of all databases with query
      \*.\* {"aid":{"$in":[123,456]}}
