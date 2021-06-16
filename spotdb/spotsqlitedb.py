import argparse, json, sqlite3, subprocess, sys, time

class Error(Exception):
    """ Error class when running a process fails
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class SpotSQLiteDB:
    """ SpotDB access and management """

    def __init__(self, filename):
        self.con = sqlite3.connect(filename)

    def __del__(self):
        self.con.close()

    def create_tables(self):
        cur = self.con.cursor()

        cur.execute('''\
            CREATE TABLE IF NOT EXISTS AttributeKeys (
                attr_id    INTEGER NOT NULL PRIMARY KEY,
                name       TEXT NOT NULL UNIQUE,
                datatype   TEXT,
                kind       TEXT CHECK ( kind IN ('global', 'metric', 'other') ) NOT NULL DEFAULT 'other',
                alias      TEXT,
                unit       TEXT,
                metadata   JSON
            )''')
        cur.execute('''\
            CREATE TABLE IF NOT EXISTS Runs (
                run        INTEGER NOT NULL PRIMARY KEY,
                launchdate INTEGER,
                spot_options  TEXT,
                spot_channels TEXT,
                globals    JSON,
                records    JSON
            )''')
        cur.execute('''\
            CREATE TABLE IF NOT EXISTS KeyVal (
                id         INTEGER NOT NULL PRIMARY KEY,
                attr_id    INTEGER,
                value      TEXT,
                run        INTEGER,
                FOREIGN KEY (attr_id) REFERENCES AttributeKeys(attr_id),
                FOREIGN KEY (run) REFERENCES Runs(run)
            )''')

        self.con.commit()
        cur.close()

    def update_attribute_keys(self, obj):
        cur = self.con.cursor()

        cur.execute('SELECT name FROM AttributeKeys')

        attributes = [ n[0] for n in cur.fetchall() ]

        for name,vals in obj['attributes'].items():
            if name.startswith('cali.') or name.startswith('spot.'):
                continue

            if name not in attributes:
                datatype  = None
                kind      = 'other'

                if 'adiak.type' in vals:
                    datatype = vals['adiak.type']
                else:
                    datatype = vals['cali.attribute.type']

                g = obj['globals']

                if 'is_global' in vals and vals['is_global'] == True:
                    kind = 'global'
                elif 'spot.metrics'            in g and name in g['spot.metrics']:
                    kind = 'metric'
                elif 'spot.timeseries.metrics' in g and name in g['spot.timeseries.metrics']:
                    kind = 'metric'

                alias = vals['attribute.alias'] if 'attribute.alias' in vals else None
                unit  = vals['attribute.unit' ] if 'attribute.unit'  in vals else None

                rec = (name, datatype, kind, alias, unit, json.dumps(vals))
                cur.execute('INSERT INTO AttributeKeys (name,datatype,kind,alias,unit,metadata) VALUES (?,?,?,?,?,?)', rec)

        self.con.commit()
        cur.close()

    def add_to_db(self, obj):
        self.update_attribute_keys(obj)

        g = obj['globals']

        launchdate = int(g['launchdate'])

        cur = self.con.cursor()

        # get the metadata attribute ids
        cur.execute("SELECT attr_id,name FROM AttributeKeys WHERE AttributeKeys.kind='global'")
        attr_ids = { name: id for (id,name) in cur.fetchall() }

        options  = g['spot.options']  if 'spot.options'  in g else None
        channels = g['spot.channels'] if 'spot.channels' in g else None

        sql = 'INSERT INTO Runs (launchdate,spot_options,spot_channels,globals,records) VALUES (?,?,?,?,?)'
        val = ( launchdate, options, channels, json.dumps(g), json.dumps(obj['records']) )

        cur.execute(sql, val)
        run_id = cur.lastrowid

        kvsql = 'INSERT INTO KeyVal (attr_id,value,run) VALUES (?,?,?)'

        for k,v in g.items():
            if k.startswith('cali.') or k.startswith('spot.'):
                continue

            cur.execute(kvsql, (attr_ids[k], v, run_id))

        self.con.commit()
        cur.close()

    def get_globals_using_keyval(self, keys):
        cur = self.con.cursor()
        sql = '''SELECT
                    AttributeKeys.name,KeyVal.value,KeyVal.run
                FROM
                    AttributeKeys,KeyVal
                WHERE
                    KeyVal.attr_id = AttributeKeys.attr_id
                AND
                    AttributeKeys.name
                IN
                    (''' + ','.join([ '?' for k in range(len(keys))]) + ')'

        cur.execute(sql, tuple(keys))

        res = {}
        rows = cur.fetchmany()
        while (len(rows) > 0):
            for (name,value,run) in rows:
                if run in res:
                    res[run][name] = value
                else:
                    res[run] = { name: value }
            rows = cur.fetchmany()

        cur.close()

        return res

    def extract_records(self, limit):
        cur = self.con.cursor()
        cur.execute('SELECT run,records FROM Runs LIMIT {}'.format(int(limit)))

        res = {}
        rows = cur.fetchmany()
        while (len(rows) > 0):
            for (run,records) in cur:
                res[run] = records
            rows = cur.fetchmany()

        cur.close()
        return res

    def get_global_attribute_info(self):
        cur = self.con.cursor()
        cur.execute("SELECT name,datatype FROM AttributeKeys WHERE AttributeKeys.kind='global'")
        res = { name: { 'type': datatype, } for (name,datatype) in cur }
        cur.close()
        return res

    def get_metric_attribute_info(self):
        cur = self.con.cursor()
        cur.execute("SELECT name,datatype,alias,unit FROM AttributeKeys WHERE AttributeKeys.kind='metric'")

        res = {}
        for (name,datatype,alias,unit) in cur:
            rec = { 'type': datatype }

            if alias:
                rec['alias'] = alias
            if unit:
                rec['unit']  = unit

            res[name] = rec
        
        cur.close()
        return res

    def extract_timeseries_data(self, run: int):
        cur = self.con.cursor()
        cur.execute("SELECT run,records FROM Runs WHERE run = ?", (run,))

        res = []

        (_,jrec) = cur.fetchone()

        for rec in json.loads(jrec):
            if "spot.channel" in rec:
                if rec["spot.channel"] != "timeseries":
                    continue
                rec.pop("spot.channel")        
                res.append(rec)                

        return res
        
    def extract_run_data(self, lastRead: int):
        cur = self.con.cursor()
        cur.execute("SELECT run,globals,records FROM Runs WHERE run > {}".format(lastRead))

        runs = {}
        run_id = 0

        rows = cur.fetchmany()
        while (len(rows) > 0): 
            for (run_id, jgbl, jrec) in rows:
                region_profile = {}
                for rec in json.loads(jrec):
                    if 'spot.channel' in rec:
                        if rec['spot.channel'] != 'regionprofile':
                            continue
                        rec.pop('spot.channel')

                    path = rec.pop('path', None)

                    if path:
                        region_profile[path] = rec

            runs[run_id] = { 'Globals': json.loads(jgbl), 'Data': region_profile }
    
            rows = cur.fetchmany()
    
        cur.close()
        
        return (runs, run_id)

