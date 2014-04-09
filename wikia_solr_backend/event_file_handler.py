"""
Polls folders we assign
"""

import os
import time
import json
import shutil
from . import default_args, get_logger, page_solr_etl
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
        return pool.map_async(page_solr_etl, events_by_host_and_slice)
    except Exception as e:
        get_logger().error(e)
        return None


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
        for filename in async_files.keys():
            result = async_files[filename][u'result']
            start_time = async_files[filename][u'start_time']
            if result.ready():
                if result.successful():
                    os.remove(filename)
                    get_logger().debug(u'Finished %s in %.2f seconds' % (filename, time.time() - start_time))
                else:
                    err = None
                    try:
                        result.get()
                    except Exception as e:
                        err = e
                    get_logger().error(u'%s: something was not succesful: %s' % (filename, err))
                    shutil.move(filename, filename.replace(folder, u"failures"))
                del async_files[filename]

        if len(async_files) < 10:
            for folder in ordered_existing_dirs:
                if len(async_files) >= 10:
                        break
                files = os.listdir(args.event_folder_root + u'/' + folder)
                for fl in files:
                    if len(async_files) >= 10:
                        break
                    start_time = time.time()
                    filename = u'%s/%s/%s' % (args.event_folder_root, folder, fl)
                    get_logger().debug(u'Attaching to %s' % filename)
                    async_result = attach_to_file(Namespace(filename=filename, pool=pool, **vars(args)))
                    if not async_result:
                        shutil.move(filename, filename.replace(folder, u"failures"))
                    async_files[filename] = {u'result': async_result, u'start_time': start_time}

        time.sleep(15)


if __name__ == u'__main__':
    main()