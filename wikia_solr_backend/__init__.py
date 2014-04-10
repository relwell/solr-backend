"""
Provides a consistent and maintainable library for interacting with Solr
"""

import requests
import logging
import wikiautils.logger as wl
import json
import time
import datetime
import traceback

logger = None
log_level = logging.INFO


class SolrHealthCheckError(Exception):
    """
    Throw if it healthcheck doesn't work.
    """
    def __init__(self, response=None):
        """
        Store a response to get info out of
        :param response: a requests response
        :type response:class:`requests.Response`
        """
        self.response = response

    def __str__(self):
        return u"Status Code %d: %s" % (self.response.status_code, self.response.content)


def get_logger():
    """ Keeps us using a single logger without using too many global declarations
    :rtype :class:`logging.logger`
    :return: logger
    """
    global logger, log_level
    if not logger:
        logger = logging.getLogger(u'solr_backend')
        logger.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        wl.Logger.use(logger, log_level)
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

    assume_healthy = True
    waited = 0
    while True:
        try:
            if not assume_healthy:
                splt = solr_update_url.split(u'/')
                splt[-2:] = [u'admin', u'ping']
                healthcheck_response = requests.get(u'/'.join(splt), params=dict(wt=u'json'))
                if healthcheck_response.status_code != 200:
                    raise SolrHealthCheckError(healthcheck_response)
                assume_healthy = healthcheck_response.json().get(u'responseHeader', {}).get(u'status', None) == 0

            result_output = filter(lambda x: x, result_output)  # remove nones
            adds = [doc for grouping in result_output for doc in grouping.get(u'adds', [])]
            deletes = [doc for grouping in result_output for doc in grouping.get(u'deletes', [])]
            get_logger().info(u"Sending %d adds and %d deletes" % (len(adds), len(deletes)))
            psa_result = page_solr_add(solr_update_url, adds)
            psd_result = page_solr_delete(solr_update_url, deletes)
            return assume_healthy and psa_result and psd_result
        except (requests.exceptions.ConnectionError, SolrHealthCheckError) as e:
            get_logger().error(u"Could not connect to %s (%s). Blocking for 30 seconds (waited %d seconds so far)"
                               % (solr_update_url, e, waited))
            time.sleep(30)
            waited += 30
            assume_healthy = False
        except Exception as e:
            get_logger().error(u"Error caught in handle_grouped_adds_and_deletes: %s: %s"
                               % (str(e), traceback.format_exc()))
            return False


def page_solr_extract_transform(namespace):
    """ Extracts data from the appropriate IndexService, pushes it to Solr.
    :param namespace: A namespace instance from argparse with host and ids pushed into it
    :type namespace:class:`argparse.namespace`
    :return: add and delete dict
    :rtype: dict
    """
    try:
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
            get_logger().error(u"Connection error for %s: %s: %s" % (namespace.host, str(e), traceback.format_exc()))
            return

        if app_response.status_code != 200:
            extras_dict = vars(namespace)
            extras_dict[u'response_content'] = app_response.content
            extras_dict[u'response_status'] = app_response.status_code
            get_logger().error(u"Request to index service failed (%s)" % app_response.url, extra=extras_dict)
            return

        try:
            response_json = app_response.json()
        except ValueError:
            if app_response.url != u"http://community.wikia.com/wiki/Community_Central:Not_a_valid_Wikia":
                extras_dict = vars(namespace)
                extras_dict[u'application_response'] = app_response.content
                get_logger().error(u"Could not decode application JSON for %s" % app_response.url, extra=extras_dict)
            # todo: maybe add some logic to make sure this url is not in the cross-wiki index?
            return

        docs = response_json.get(u'contents', [])
        deletes = filter(lambda x: u'delete' in x and u'id' in x[u'delete'], docs)
        adds = filter(lambda y: y not in deletes, docs)
        if namespace.add_last_indexed:
            timestamp = datetime.datetime.utcnow().isoformat()+u'Z'
            map(lambda z: z.update({u'indexed': {u'set': timestamp}}), adds)

        return {u'adds': adds, u'deletes': [{u'id': doc[u'delete'][u'id']} for doc in deletes]}
    except Exception as e:
        get_logger().error(u"Error caught in page_solr_extract_transform: %s: %s"
                           % (str(e), traceback.format_exc()))


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
        solr_response = requests.post(solr_update_url, data=json.dumps(data),
                                      headers={u'Content-type': u'application/json'})
        get_logger().debug(u"Sent %d updates to to %s" % (len(data), solr_update_url))

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
    solr_response = requests.post(solr_update_url, data=json.dumps(data),
                                  headers={u'Content-type': u'application/json'})
    get_logger().debug(u"Sent %d updates to to %s" % (len(data), solr_update_url))

    if solr_response.status_code != 200:
        extras_dict = dict(data=data, response_content=solr_response.content,
                           response_status=solr_response.status_code)
        get_logger().error(u"Status code for update on %s was not 200" % solr_update_url, extra=extras_dict)
        return False

    return True

