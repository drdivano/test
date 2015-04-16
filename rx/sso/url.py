# -*- coding: utf-8 -*-

"""
$Id: $
"""

import cherrypy
import urllib
import cgi
from urlparse import urlparse, urlunparse, ParseResult
from urllib import urlencode

import config

EXT_SERVER_BASE = '%s://%s' % tuple(urlparse(config.EXT_SERVER_URL))[:2]

def inject_params(url, **params):
    ''' Добавить параметр(ы) к URL в строковой форме. Убираем параметры, начинающиеся с подчерка и submit '''
    o = urlparse(url)
    r_params = {}
    for name, value in cgi.parse_qs(o.query).items():
        if not name.startswith('_') and not name.startswith('submit'):
            r_params[name] = value
    r_params.update(params)
    query = urlencode(r_params)
    o = ParseResult(o.scheme, o.netloc, o.path, o.params, query, o.fragment)
    return urlunparse(o)

# параметр _loginUrl используется в обработчике ошибок для редиректа на страницу логина
# мы вынуждены динамически вычислять loginUrl из-за необходимости добавлять return_url
def get_login_url(page):
    return_url = inject_params(EXT_SERVER_BASE + cherrypy.request.path_info, **cherrypy.request.params)
    return_url_q = urllib.quote(return_url)
    return '%s?return_url=%s' % (config.SSO_LOGIN_URL, return_url_q)
