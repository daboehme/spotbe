#!/usr/bin/env python3

import spotdb.spotsqlitedb as spotdb

import json
import sys
import subprocess

def _get_json_from_cali(filename):
    cmd = [ 'cali-query', '-q', 'format json(object)', filename ]
    proc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    cmdout, _ = proc.communicate()

    if (proc.returncode != 0):
        sys.exit('Command' + str(cmd) + ' exited with ' + str(proc.returncode))

    return cmdout


def _add(dbfile, files):
    db = spotdb.SpotSQLiteDB(dbfile)
    db.create_tables()

    for califile in files:
        jsonstr = _get_json_from_cali(califile)
        obj = json.loads(jsonstr)
        keys = obj.keys()

        if 'globals' not in keys or 'records' not in keys:
            sys.exit('{} is not a Spot file'.format(califile))
        if not 'spot.format.version' in obj['globals']:
            sys.exit('{} is not a Spot file: spot.format.version attribute is missing.'.format(califile))

        db.add_to_db(obj)


def help():
    print("Usage: spotdb-add.py caliper-files... sqlite3-file")


def main():
    args = sys.argv[1:]

    if (args[0] == "-h" or args[0] == "--help"):
        help()
        sys.exit()

    dbfile = args.pop()

    if not dbfile.endswith('.sqlite'):
        msg = "spotdb-add: Expected SQLite DB file (.sqlite) as last argument. Got " + dbfile + "."
        sys.exit(msg)

    _add(dbfile, args)


if __name__ == "__main__":
    main()
