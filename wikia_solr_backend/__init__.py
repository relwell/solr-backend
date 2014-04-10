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


def handle_grouped_adds_and_deletes(solr_update_url, result_output):
    """
    Takes extract results, groups them by function, and pushes them to their respective page_solr
    :param solr_update_url: The solr endpoint URL
    :type solr_update_url: str
    :param result_output: a list of dicts with add and delete directives
    :type result_output: list
    :return: whether add and delete worked
    :rtype: bool
    """
    result_output = filter(lambda x: x, result_output)  # remove nones
    adds = [doc for grouping in result_output for doc in grouping.get(u'adds', [])]
    deletes = [doc for grouping in result_output for doc in grouping.get(u'deletes', [])]
    psa_result = page_solr_add(solr_update_url, adds)
    psd_result = page_solr_delete(solr_update_url, deletes)
    return psa_result and psd_result


def page_solr_extract_transform(namespace):
    """ Extracts data from the appropriate IndexService, pushes it to Solr.
    :param namespace: A namespace instance from argparse with host and ids pushed into it
    :type namespace:class:`argparse.namespace`
    :return: add and delete dict
    :rtype: dict
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
        extras_dict = vars(namespace).items()
        extras_dict[u'response_content'] = app_response.content
        extras_dict[u'response_status'] = app_response.status_code
        get_logger().error(u"Request to index service failed", extra=extras_dict)
        return

    try:
        response_json = app_response.json()
    except ValueError:
        extras_dict = vars(namespace)
        extras_dict[u'application_response'] = app_response.content
        get_logger().error(u"Could not decode application JSON for %s" % app_response.url, extra=extras_dict)
        return

    docs = response_json.get(u'contents', [])
    deletes = filter(lambda x: u'delete' in x and u'id' in x[u'delete'], docs)
    adds = filter(lambda y: y not in deletes, docs)
    if namespace.add_last_indexed:
        timestamp = datetime.datetime.utcnow().isoformat()+u'Z'
        map(lambda z: z.update({u'indexed': {u'set': timestamp}}), adds)

    return {u'adds': adds, u'deletes': [{u'id': doc[u'delete'][u'id']} for doc in deletes]}


def page_solr_add(solr_update_url, dataset):
    """
    Wraps posting with all the logging we want
    :param solr_update_url: the update url for solr
    :type solr_update_url: str
    :param dataset: a list of update dicts
    :type dataset: list
    :return: True or False, depending on success
    :rtype: bool
    """
    for data in [dataset[i:i+250] for i in range(0, len(dataset), 250)]:
        try:
            solr_response = requests.post(solr_update_url, data=json.dumps(data),
                                          headers={u'Content-type': u'application/json'})
            get_logger().debug(u"Sent %d updates to to %s" % (len(data), solr_update_url))
        except requests.exceptions.ConnectionError as e:
            get_logger().error(u"Could not connect to %s" % solr_update_url, extra={u'exception': e})
            continue

        if solr_response.status_code != 200:
            extras_dict = dict(data=data, response_content=solr_response.content,
                               response_status=solr_response.status_code)
            get_logger().error(u"Status code for update on %s was not 200" % solr_update_url, extra=extras_dict)
            continue

    return True


def page_solr_delete(solr_update_url, data):
    """
    Wraps posting with all the logging we want
    :param solr_update_url: the update url for solr
    :type solr_update_url: str
    :param data: a dict with delete directives
    :type data: dict
    :return: True or False, depending on success
    :rtype: bool
    """
    try:
        solr_response = requests.post(solr_update_url, data=json.dumps(data),
                                      headers={u'Content-type': u'application/json'})
        get_logger().debug(u"Sent %d updates to to %s" % (len(data), solr_update_url))
    except requests.exceptions.ConnectionError as e:
        get_logger().error(u"Could not connect to %s" % solr_update_url, extra={u'exception': e})
        return False

    if solr_response.status_code != 200:
        extras_dict = dict(data=data, response_content=solr_response.content,
                           response_status=solr_response.status_code)
        get_logger().error(u"Status code for update on %s was not 200" % solr_update_url, extra=extras_dict)
        return False

    return True

