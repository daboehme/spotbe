import argparse, json, sys, pickle, os, subprocess, getpass, urllib
from functools import partial
import math

CALIQUERY       = '/usr/gapps/spot/caliper/bin/cali-query'
TEMPLATE_NOTEBOOK = '/usr/gapps/wf/web/spot/data/JupyterNotebooks/TemplateNotebook.ipynb'
INCLUS_DURATION = 'sum#time.inclusive.duration'

def _sub_call(cmd): 
    # call a subcommand in a new process and parse json results into object
    return json.loads(subprocess.check_output(cmd).decode('utf-8'))


def _cali_func_duration(inclus_dur, filepath):
    return _sub_call([CALIQUERY , '-q', 'SELECT function,{0} WHERE function FORMAT JSON'.format(inclus_dur), str(filepath) ])


def _cali_func_topdown(filepath):
    return _sub_call([CALIQUERY ,'-q', 'SELECT * FORMAT JSON', str(filepath) ])


def _cali_list_globals(inclus_dur, filepath):
    cali_globals = _sub_call( [CALIQUERY ,'-j', '--list-globals', str(filepath) ])[0]

    # duration needs to be added to cali globals... just get from select call for now
    # just finds the highest duration in all the functions, assuming that would be the root 
    cali_globals["Inclusive Duration"] = max( map( lambda item: item.get(inclus_dur, 0) 
                                                 , _cali_func_duration(inclus_dur, filepath)))
    return cali_globals 


def hierarchical(args):
    dirpath    = args.directory
    inclus_dur = args.durationKey

    #load cache or initiate if missing
    cache = []
    fnames = list(map(lambda fname: os.path.join(dirpath , fname), args.filenames) if args.filenames else [os.path.join(dirpath, fpath) for fpath in os.listdir(dirpath) if fpath.endswith('.cali')] )

    import multiprocessing
    metaList = multiprocessing.Pool(18).map(partial(_cali_list_globals, inclus_dur), fnames)
    dataList = multiprocessing.Pool(18).map(partial(_cali_func_duration, inclus_dur), fnames)
    dataList = list(map(lambda item: {entry['function']: entry.get(inclus_dur, 0) for entry in item}, dataList))

    out = [{'meta': m, 'data': d} for (m, d) in zip(metaList, dataList)]

    # dump summary stdout
    json.dump(out, sys.stdout)


def _generateLayout(metasFromFilenameDict):
        def genChartItem(meta):
            name = meta[0]
            val = meta[1]
            try:
                float(val)
                viz = "BarChart"
            except:
                viz = "PieChart"

            return {"dimension": name, "title": name, "viz": viz}

        def genTableItem(meta):
            name = meta[0]
            return {"dimension": name, "label": name}

        metas     = next(iter((metasFromFilenameDict.values()))).items()
        chartList = list(map(genChartItem, metas))
        tableList = list(map(genTableItem, metas))
        return {"charts": chartList, "table": tableList}


# returns a single durations hierarchy given a filepath
def durations(args):
    filepath = args.filepath
    inclus_dur = args.durationKey
    data_list = _cali_func_duration(inclus_dur, filepath)
    output = {}
    for item in data_list: 
        func_path = item["function"]
        duration = item.get(inclus_dur, 0)
        output[func_path] = max(duration, output.get(func_path, 0))
    json.dump(output, sys.stdout)   

def toggleChart(args):

    dirpath = args.dirpath
    chartname = args.chartname
    show = True if args.show == 'true' else False
    
    # load spot settings from user home dir
    spot_settings_filepath = os.path.expanduser('~/spot_settings.pk')

    spot_settings = {}
    try: spot_settings = pickle.load(open(spot_settings_filepath, 'rb'))
    except: pass

    # toggle setting
    hide_list = []
    if spot_settings.get(dirpath, None):
        if spot_settings[dirpath].get('hide', None):
            # already have the list in settings, repoint to it:
            hide_list = spot_settings[dirpath]['hide'] 
        else:
            # don't have it, so add it
            spot_settings[dirpath]['hide'] = hide_list
    else:
        spot_settings[dirpath] = {'hide': hide_list}

    if (show): 
        try:
            hide_list.remove(chartname)
        except:
            pass
    else: 
        if chartname not in hide_list:
            hide_list.append(chartname)

    # save settings to user home dir
    pickle.dump(spot_settings, open(spot_settings_filepath, 'wb'))

    
def summary(args):
    dirpath    = args.filepath
    cache_path = os.path.join(dirpath , "spot_cache.pkl")

    #load cache or initiate if missing
    cache = {}
    if os.path.exists(cache_path):
        try: cache = pickle.load(open(cache_path,'rb'))
        except:  pass
    else:
        open(cache_path, 'a').close()  # touch file
        os.chown(cache_path, -1 , os.stat(dirpath).st_gid)
        os.chmod(cache_path, 0o660)


    # check for new cali files, if so add to cache and write to disk
    cache_miss_fpaths = [fname for fname in os.listdir(dirpath) if not fname in cache and fname.endswith('.cali')]
    if cache_miss_fpaths:
        import multiprocessing
        cache = {**cache, **dict(zip(cache_miss_fpaths, multiprocessing.Pool(18).map( partial(_cali_list_globals, None), [os.path.join(dirpath, fname) for fname in cache_miss_fpaths])))}
        pickle.dump(cache, open(cache_path, 'wb'))

    
    # layout: if filename provided then return contents,  else generate a generic one
    layout = ""
    if args.layout:
        layout = json.load(open(args.layout))

    else:
        layout = _generateLayout(cache)

    # filter out hidden results:
    spot_settings_filepath = os.path.expanduser('~/spot_settings.pk')

    hide = None
    try: 
        hide = pickle.load(open(spot_settings_filepath, 'rb')).get(cache_path, None)
    except: pass
    
	
    filter(lambda el: el['dimension'] not in hide, layout['charts'])
    filter(lambda el: el['dimension'] not in hide, layout['table'])

    # dump summary stdout
    json.dump({'data': cache, 'layout': layout}, sys.stdout)


def topdown(args):
    """call cali on topdown file and return a json object with function keys and objects with duration and topdown info"""
    filepath = args.filepath
    json.dump( {item["function"]:{ "duration": item["count"] 
                                 , "topdown": {k[15:]:v for (k,v) in item.items() if k.startswith("libpfm.topdown#")} 
                                 } 
                    for item in _cali_func_topdown(filepath) if "function" in item }
             , sys.stdout
             )



def mpi_trace(args):
  print(open(args.filepath).read())



def jupyter(args):

  # create notebook in ~/spot_jupyter dir

  #  - first create directory
  cali_path = Path(args.cali_filepath).resolve()
  ntbk_dir = Path.home() / 'spot_jupyter'
  ntbk_dir.mkdir(exist_ok=True)

  #  - copy template (replacing CALI_FILE_NAME)
  ntbk_path = ntbk_dir / (cali_path.stem + '.ipynb')
  ntbk_template_str = open(TEMPLATE_NOTEBOOK).read().replace('CALI_FILE_NAME', str(cali_path))
  open(ntbk_path, 'w').write(ntbk_template_str)

  # return Jupyterhub address
  print('https://rzlc.llnl.gov/jupyter/user/{}/notebooks/spot_jupyter/{}'.format(getpass.getuser(), urllib.parse.quote(ntbk_path.name)))


# argparse
parser = argparse.ArgumentParser(description="sup")
subparsers = parser.add_subparsers(dest="sub_name")

summary_sub = subparsers.add_parser("summary")
summary_sub.add_argument("filepath", help="file and directory paths")
summary_sub.add_argument("--layout", help="layout json filepath")
#summary_sub.add_argument("--layout", help="layout json filepath", default="/usr/gapps/wf/web/spot/data/default_layout.json")
summary_sub.set_defaults(func=summary)

removeChart_sub = subparsers.add_parser("toggleChart")
removeChart_sub.add_argument("dirpath", help="directory path of data to toggle chart")
removeChart_sub.add_argument("chartname", help="chartname to toggle")
removeChart_sub.add_argument("show", help="either true or false")
removeChart_sub.set_defaults(func=toggleChart)

durations_sub = subparsers.add_parser("durations")
durations_sub.add_argument("durationKey", help="the key for the inclusive duration")
durations_sub.add_argument("filepath", help="file and directory paths")
durations_sub.set_defaults(func=durations)

hierarchical_sub = subparsers.add_parser("hierarchical")
hierarchical_sub.add_argument("directory", help="directory")
hierarchical_sub.add_argument("durationKey", help="the key for the inclusive duration")
hierarchical_sub.add_argument("--filenames", nargs="+", help="individual filenames sep by space")
hierarchical_sub.set_defaults(func=hierarchical)

topdown_sub = subparsers.add_parser("topdown")
topdown_sub.add_argument("filepath", help="file and directory paths")
topdown_sub.set_defaults(func=topdown)

jupyter_sub = subparsers.add_parser("jupyter")
jupyter_sub.add_argument("cali_filepath", help="create a notebook to check out a sweet cali file")
jupyter_sub.set_defaults(func=jupyter)

mpitrace = subparsers.add_parser("mpitrace")
mpitrace.add_argument("filepath", nargs="?", help="filepath to mpidata", default="/usr/gapps/wf/web/spot/data/test_mpi.json")
mpitrace.set_defaults(func=mpi_trace)


# get input names from command line args  (these are filenames and directory names)
args = parser.parse_args()
args.func(args)