# -*- coding: utf-8 -*-

"""
$Id: $
"""

import base64
import urllib
import urllib2

import config

basic_auth = 'basic %s' % base64.encodestring('%s:%s' % (config.PBUS_MY_NAME, config.PBUS_PASSWORD)).rstrip()

#pbus_auth_handler = urllib2.HTTPBasicAuthHandler()
#pbus_auth_handler.add_password('pbus', config.PBUS_URL, config.PBUS_MY_NAME, config.PBUS_PASSWORD)
#pbus_opener = urllib2.build_opener(pbus_auth_handler)

def subscribe(topic):
    if not config.PBUS_URL:
        return

    callbackUrl = 'http://%s:%s%s/notify/%s' % (config.PBUS_CALLBACK_HOST, config.PBUS_CALLBACK_PORT, config.VIRTUAL_BASE, topic)
    callbackUrl = urllib.quote(callbackUrl, safe='')
    callbackUrl = urllib.quote(callbackUrl)
    url = '%s/%s/subscribe/%s' % (config.PBUS_URL, topic, callbackUrl)
    req = urllib2.Request(url=url,
                          data='',
                          headers={'content-type': 'application/json;charset=utf-8',
                                   'authorization': basic_auth})
    urllib2.urlopen(req)
    #pbus_opener.open(url, '')


def post(topic, msg, recipient=None, ttl=None, reply_ttl=None):
    """ Если указан recipient - сообщение приватное (point-to-point) """
    if not config.PBUS_URL:
        return

    url = '%s/%s/post' % (config.PBUS_URL, topic)
    if recipient:
        url += '/private/%s' % recipient

    headers = {'content-type': 'application/json; charset=utf-8',
               'authorization': basic_auth,}
    if ttl is not None:
        headers['X-TTL'] = str(ttl)
    if reply_ttl is not None:
        headers['X-Reply-TTL'] = str(reply_ttl)

    req = urllib2.Request(url=url, data=msg, headers=headers)
    urllib2.urlopen(req)
    #pbus_opener.open(req)


def ping():
    url = '%s/ping' % (config.PBUS_URL)
    headers = {'content-type': 'application/json; charset=utf-8',
               'authorization': basic_auth,}
    req = urllib2.Request(url=url, headers=headers)
    return urllib2.urlopen(req).read()
