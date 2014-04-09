"""
Provides a consistent and maintainable library for interacting with Solr
"""

import requests
import logging
import wikiautils.logger as wl
import json
import datetime

logger = None


def get_logger():
    """ Keeps us using a single logger without using too many global declarations
    :rtype :class:`logging.logger`
    :return: logger
    """
    global logger
    if not logger:
        logger = logging.getLogger(u'solr_backend')
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        wl.Logger.use(logger, logging.WARN)
    return logger


def default_args(ap):
    """ Gives us an ArgumentParser object with default values that can be reused across scripts
     :param ap: An argument parser
     :type ap:class:`argparse.ArgumentParser`
     :return: argparser
     :rtype :class:`argparse.ArgumentParser`
    """
    ap.add_argument(u'--index-service', dest=u'index_service', default=u'All')
    ap.add_argument(u'--solr-update-url', dest=u'solr_update_url',
                    default=u'http://search-master:8983/solr/main/update/')
    ap.add_argument(u'--dont-add-last-indexed', dest=u'add_last_indexed', default=True, action=u'store_false')
    return ap


def page_solr_etl(namespace):
    """ Extracts data from the appropriate IndexService, pushes it to Solr.
    :param namespace: A namespace instance from argparse with host and ids pushed into it
    :type namespace:class:`argparse.namespace`
    """
    if not namespace.ids or not namespace.host:
        get_logger().error(u"page_solr_etl invoked with ids or host missing", extra=vars(namespace))
        return

    params = dict(controller=u"WikiaSearchIndexer",
                  method=u"get",
                  service=namespace.index_service,
                  ids=u"|".join(map(str, namespace.ids)))

    try:
        app_response = requests.get(u'%s/wikia.php' % namespace.host,
                                    params=params)
    except requests.exceptions.ConnectionError as e:
        get_logger().error(u"Connection error for %s" % namespace.host, extras={u'exception': e})
        return

    if app_response.status_code != 200:
        extras_dict = dict(vars(app_response).items() + vars(namespace).items())
        get_logger().error(u"Request to index service failed", extra=extras_dict)
        return

    try:
        response_json = app_response.json()
    except ValueError:
        extras_dict = vars(namespace)
        extras_dict[u'application_response'] = app_response.content
        get_logger().error(u"Could not decode application JSON", extra=extras_dict)
        return

    docs = response_json.get(u'contents', [])
    deletes = filter(lambda x: u'delete' in x and u'id' in x[u'delete'], docs)
    adds = filter(lambda y: y not in deletes, docs)
    if namespace.add_last_indexed:
        timestamp = datetime.datetime.utcnow().isoformat()+u'Z'
        map(lambda z: z.update({u'indexed': {u'set': timestamp}}), adds)

    return send_solr_updates(namespace.solr_update_url, adds + deletes)   # we originally split these out



def send_solr_updates(solr_update_url, data):
    """
    Wraps posting with all the logging we want
    :param solr_update_url: the update url for solr
    :type solr_update_url: str
    :param data: a list of update dicts
    :type data: list
    :return: True or False, depending on success
    :rtype: bool
    """
    try:
        solr_response = requests.post(solr_update_url, data=json.dumps(data),
                                      headers={u'Content-type': u'application/json'})
    except requests.exceptions.ConnectionError as e:
        get_logger().error(u"Could not connect to %s" % solr_update_url, extras={u'exception': e})
        return False

    if solr_response.status_code != 200:
        extras_dict = vars(solr_response)
        extras_dict[u'data'] = data
        get_logger().error(u"Status code for update on %s was not 200" % solr_update_url, extras=extras_dict)
        return False

    return True




