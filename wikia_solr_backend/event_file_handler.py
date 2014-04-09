"""
Polls folders we assign
"""

import os
import time
import json
import shutil
from . import default_args, get_logger, page_solr_extract_transform, page_solr_load
from collections import defaultdict
from multiprocessing import Pool
from argparse import ArgumentParser, Namespace


def get_args():
    """
     Instantiate the argparse Namespace class based on arguments on the command line
     :return: parsed args
     :rtype: `argparse.Namespace`
    """
    ap = default_args(ArgumentParser(u"Handles indexing using event files"))
    ap.add_argument(u'--event-folder-root', dest=u'event_folder_root', default=u'/var/spool/scribe/')
    ap.add_argument(u'--folder-ordering', dest=u'folder_ordering', default=u'events,retries,bulk')
    ap.add_argument(u'--num-processes', dest=u'num_processes', default=6, type=int)
    return ap.parse_args()


def attach_to_file(namespace):
    """
     Reads events from a file
     :param namespace: an argparse namespace with filename pushed into it
     :type namespace:class:`argparse.Namespace`
     :return: AsyncResult so we can continue queueing
     :rtype :class:`multiprocessing.pool.AsyncResult`
    """

    pool = namespace.pool
    namespace.pool = None  # can't pickle it like the rest of the namespace

    start_time = time.time()

    host_hash = defaultdict(list)
    with open(namespace.filename, u'r') as fl:
        for line_number, line in enumerate(fl):
            try:
                event = json.loads(line)
                if u"pageId" not in event or u"serverName" not in event:
                    get_logger().info(u"Event in line number %d of %s is malformed: %s"
                                      % (line_number, namespace.filename, line))
                    continue
                host_hash[event[u"serverName"]].append(event[u"pageId"])
            except ValueError:
                get_logger().warn(u"Could not decode event in line number %d of %s" % (line_number, namespace.filename),
                                  extras={u'data': line})

    if not host_hash:
        get_logger().error(u"No events found in %s" % namespace.filename)
        return None

    events_by_host_and_slice = [Namespace(host=host, ids=host_hash[host][i:i+15], **vars(namespace))
                                for host in host_hash
                                for i in range(0, len(host_hash[host]), 15)]
    try:
        async_result = pool.map_async(page_solr_extract_transform, events_by_host_and_slice)
        return {u'result': async_result, u'start_time': start_time, u'lines': line_number, u'step': 1}
    except Exception as e:
        get_logger().error(e)
        return None


def monitor_async_files(pool, solr_update_url, async_files):
    """
    Pushes async result instances in a defaultdict through ETL process
    :param pool: mp pool
    :type pool:class:`multiprocessing.pool.Pool`
    :param async_files: default dict keying file names to dictionaries holding data about an async result
    :type async_files: defaultdict
    :return: the async_files dict with any finished items removed
    """
    for filename, result_dict in async_files.items():
            result = result_dict[u'result']
            start_time = result_dict[u'start_time']
            lines = result_dict[u'lines']
            if result.ready():
                if result.successful():
                    result_output = result.get()
                    if result_output and result_dict[u'step'] == 1:
                        result_data = [item for grouping in result_output if grouping for item in grouping if item]
                        get_logger().debug(u"Sending %d updates to load step" % len(result_data))
                        result_dict[u'result'] = pool.apply_async(page_solr_load, (solr_update_url, result_data,))
                        result_dict[u'result'].get()
                        result_dict[u'step'] = 2
                    else:
                        os.remove(filename)
                        get_logger().debug(u'Finished %s in %.2f seconds (%d lines)' %
                                           (filename, time.time() - start_time, lines))
                        del async_files[filename]
                else:
                    err = None
                    try:
                        result.get()
                    except Exception as e:
                        err = e
                    get_logger().error(u'%s: something was not succesful: %s' % (filename, err))
                    splt = filename.split(u'/')
                    splt[-2] = u'failures'
                    shutil.move(filename, u"/".join(splt))
                    del async_files[filename]
    return async_files


def main():
    """ Main script method -- poll folders and spawn workers  """
    args = get_args()
    pool = Pool(processes=args.num_processes)
    dirs = os.listdir(args.event_folder_root)
    prioritized_dirs = [x for x in args.folder_ordering.split(u',') if x in dirs]
    remaining_dirs = [x for x in dirs if x not in prioritized_dirs and x != u'failures']
    ordered_existing_dirs = prioritized_dirs + remaining_dirs
    async_files = {}
    while True:
        async_files = monitor_async_files(pool, args.solr_update_url, async_files)

        if len(async_files) < args.num_processes:
            for folder in ordered_existing_dirs:
                if len(async_files) >= args.num_processes:
                        break
                files = os.listdir(args.event_folder_root + u'/' + folder)
                for fl in files:
                    if len(async_files) >= args.num_processes:
                        break

                    filename = u'%s/%s/%s' % (args.event_folder_root, folder, fl)
                    get_logger().debug(u'Attaching to %s' % filename)
                    async_result_dict = attach_to_file(Namespace(filename=filename, pool=pool, **vars(args)))
                    if not async_result_dict:
                        shutil.move(filename, filename.replace(folder, u"failures"))
                    async_files[filename] = async_result_dict

        time.sleep(5)


if __name__ == u'__main__':
    main()