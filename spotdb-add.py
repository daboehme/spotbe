#!/usr/bin/env python3

import json
import sys
import subprocess

from uuid import uuid4

from sina.datastore import create_datastore
from sina.model import Record


def _get_json_from_cali_with_caliquery(filename):
    cmd = [ 'cali-query', '-q', 'format json(object)', filename ]
    proc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    cmdout, _ = proc.communicate()

    if (proc.returncode != 0):
        sys.exit('Command' + str(cmd) + ' exited with ' + str(proc.returncode))

    return json.loads(cmdout)


def _make_string_from_list(list_or_elem):
    if isinstance(list_or_elem, list):
        return '/'.join(list_or_elem)
    else:
        return list_or_elem


def _filter_cali_profile_record(reader, record):
    """ Filter useful data out of the profiling records
    """

    channel = record.get("spot.channel", "regionprofile")
    out = { "spot.channel" : channel }

    if channel == "regionprofile":
        out["path"] = _make_string_from_list(record.pop("path", ""))

        # only include metrics
        for k, v in record.items():
            if reader.attribute(k).is_value():
                out[k] = v
    else:
        for k, v in record.items():
            out[k] = _make_string_from_list(v)

    return out


def _get_json_from_cali(filename):
    from caliperreader import CaliperReader

    r = CaliperReader()
    r.read(filename)

    # disregard base attributes because they cause issues with a.metadata()
    keys = list(r.attributes())
    keys.remove("cali.attribute.name")
    keys.remove("cali.attribute.type")
    keys.remove("cali.attribute.prop")

    # convert caliperreader output to Spot format
    attr = { k : r.attribute(k).metadata()       for k   in keys      }
    recs = [ _filter_cali_profile_record(r, rec) for rec in r.records ]

    return { "attributes": attr, "globals": r.globals, "records": recs }


def _update_attribute_records(ds, globals, attributes):
    def _update_record(name, rectype, values):
        if ds.records.exist(name):
            return
        rec = Record(id=name, type=rectype)
        for k,v in values.items():
            rec.add_data(k,v)
        ds.records.insert(rec)

    metrics = []

    if "spot.metrics" in globals:
        metrics.extend(filter(lambda m : len(m) > 0, globals["spot.metrics"].split(",")))
    if "spot.timeseries.metrics" in globals:
        metrics.extend(filter(lambda m : len(m) > 0, globals["spot.timeseries.metrics"].split(",")))

    for name in metrics:
        _update_record(name, "caliper_metric_attribute", attributes[name])

    for name in globals.keys():
        _update_record(name, "caliper_global_attribute", attributes[name])


def _add(dbfile, files):
    ds = create_datastore(dbfile)

    for califile in files:
        obj = _get_json_from_cali(califile)
        keys = obj.keys()

        if 'globals' not in keys or 'records' not in keys:
            sys.exit('{} is not a Spot file'.format(califile))
        if not 'spot.format.version' in obj['globals']:
            sys.exit('{} is not a Spot file: spot.format.version attribute is missing.'.format(califile))

        _update_attribute_records(ds, obj["globals"], obj["attributes"])

        # Add metadata

        rec = Record(id=str(uuid4()), type="run")

        for name, value in obj["globals"].items():
            if name.startswith("cali.") or name.startswith("spot."):
                continue

            type = obj["attributes"][name]["type"]

            if type == "int" or type == "uint":
                value = int(value)
            elif type == "double":
                value = float(value)

            rec.add_data(name, value)

        # Add profiling data

        channel_data = {}

        for entry in obj["records"]:
            channel = "regionprofile"

            if "spot.channel" in entry:
                channel = entry["spot.channel"]
                entry.pop("spot.channel")

            if not channel in channel_data:
                channel_data[channel] = []

            channel_data[channel].append(entry)

        rec.user_defined = channel_data
        rec.add_file(califile, tags=[ "caliper" ])

        ds.records.insert(rec)


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