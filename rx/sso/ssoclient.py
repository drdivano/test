# -*- coding: utf-8 -*-

"""
$Id: $
"""
from __future__ import with_statement

from pyramid.auth.exc import NoCredentialsError, BadCredentialsError

import threading
import time
import binascii
import hashlib
import httplib
import socket
import urllib

import config

class SSOClient(object):
    _nonce = 1
    _lock = threading.Lock()
    
    @classmethod
    def _call_sso_service(cls, url, encoded_auth_token, nonce):
        ''' Обращаемся к серверу SSO для валидации сессии пользователя. '''
        post_data = urllib.urlencode({'encoded_auth_token': encoded_auth_token,
                                      'nonce': str(nonce)}) 
        headers = {'Content-type': 'application/x-www-form-urlencoded',}
        http_con = httplib.HTTPConnection(config.SSO_SERVER)
        try:
            http_con.request('POST', url, post_data, headers)
        except (httplib.HTTPException, socket.error), e:
            raise NoCredentialsError(repr(e))
        return http_con.getresponse()

    @classmethod
    def _encode_auth_token(cls, s, nonce):
        ''' Кодируем id сессии, используя системный пароль SSO. Генерируем уникальное число nonce для борьбы с атаками на повторение. '''
        m = hashlib.sha512()
        assert len(s) <= m.digest_size, s
        m.update(config.SSO_PASSWORD)
        m.update(str(nonce))
        digest = m.digest()
        return ''.join([ chr(ord(c) ^ ord(digest[i])) for i, c in enumerate(s) ]) 

    @classmethod
    def call(cls, url, sso_session_id):
        # сервер SSO проверяет, что переданный nonce больше, чем предыдущий        
        with cls._lock:  # инкремент nonce нужно выполнять атомарно
            cls._nonce += 1
            if cls._nonce >= 1000:
                cls._nonce = 0
            nonce = int(time.time()) * 10000 + cls._nonce

        encoded_auth_token = binascii.hexlify(cls._encode_auth_token(sso_session_id, nonce))

        response = cls._call_sso_service(url, encoded_auth_token, nonce)

        if response.status != 200:
            print 'SSO failed with status', response.status
        else:
            print 'SSO is successful'

        if response.status == 401:
            raise BadCredentialsError

        if response.status != 200:
            raise NoCredentialsError

        return response.read()
