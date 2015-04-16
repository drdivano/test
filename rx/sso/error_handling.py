# -*- coding: utf-8 -*-
"""
$Id: $
"""

from datetime import timedelta, datetime
import cherrypy

from zope.interface import implements, Interface
from pyramid.ui.error import ErrorHandler, StdErrorReporter
from pyramid.auth.exc import AuthorizationError, AuthenticationError
from rx.i18n import _
from rx.utils.dt import parse_iso_datetime

MIN_AUTH_TIME = timedelta(minutes=1)


class AntiLoopErrorReporter(StdErrorReporter):
    def msg(self, exc):
        if (
            (isinstance(exc, (AuthorizationError, AuthenticationError)) and
            ('_auth_time' in cherrypy.request.params) and
            (datetime.now() - parse_iso_datetime(cherrypy.request.params['_auth_time']) < MIN_AUTH_TIME))
        ):
            msg = _(u'Произошла ошибка проверки авторизации. Нажмите <a href="%s">сюда</a> для входа в систему') % (self.page._loginUrl)
            return msg

        return super(AntiLoopErrorReporter, self).msg(exc)


class IErrorHandling(Interface):
    pass


class ErrorHandling(object):
    implements(IErrorHandling)
    def __init__(self, *args, **kw):
        super(ErrorHandling, self).__init__(*args, **kw)
        self.errorHandler = ErrorHandler(self)
        self._cp_config['request.error_response'] = self.errorHandler.index
