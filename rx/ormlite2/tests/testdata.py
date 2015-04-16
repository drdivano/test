# -*- coding: utf-8 -*-

"""Test data initializers.

$Id: testdata.py 643 2010-03-30 15:01:34Z vmozhaev $
"""

import re
from datetime import datetime

from rx.ormlite2.dbop import dbquery as _dbquery, getParamName
#import config
from pyramid.utils import Config as config



def dbquery(sql):
    sql = sql.replace('\n', ' ')
    sql = sql.strip()
    sql_exprs = [ s.strip() for s in sql.replace('\n', ' ').split(';') ]
    for expr in sql_exprs:
        if expr:
            _dbquery(expr)


def deleteAllFrom(*args):
    for tab in args:
        dbquery("delete from %s" % tab)

def _initObjectData():
    sql = '''
insert into ut__orm_objects (object_id, deleted_date) values (1, NULL);
insert into ut__orm_objects (object_id, deleted_date) values (2, '2008-06-01');
insert into ut__orm_objects (object_id, deleted_date) values (3, NULL);
'''
    dbquery(sql)


def _initMonthsData():
    sql = '''
insert into ut__orm_months values (01, 2008, 'C');
insert into ut__orm_months values (02, 2008, 'C');
insert into ut__orm_months values (03, 2008, 'O');
insert into ut__orm_months values (04, 2008, 'O');
'''
    dbquery(sql)


def _initReportsData():
    sql = '''
insert into ut__orm_reports values (88991, 03, 2008);
insert into ut__orm_reports values (88992, 03, 2008);
insert into ut__orm_reports values (88993, 03, 2008);
insert into ut__orm_reports values (88999, 01, 2008);
'''
    dbquery(sql)


def _initUnitsData():
    sql = '''
insert into ut__orm_units (unit_id, leader_id, name, short_name, unit_index) values (1200, 1926, 'Test Unit', 'TU', '12');
insert into ut__orm_units (unit_id, leader_id, name, short_name, unit_index) values (800, 2398, 'Test Unit 2', 'TU 2', '8');
'''
    dbquery(sql)


def _initOrmUsersData():
    sql = '''
insert into ut__orm_users (user_id, user_dn, fname, patronymic, lname, email, rank, unit_id, extra_roles, terminated) values (1926, 'CN=Test User', 'Алексей', 'Александрович', 'Новгородов', 'unittest@test.com', 'Начальник отдела', 1200, '', NULL);
insert into ut__orm_users (user_id, user_dn, fname, patronymic, lname, email, rank, unit_id, extra_roles, terminated) values (1927, 'CN=Test User 0', 'Владимир', 'Борисович', 'Можаев', 'unittest0@test.com', 'Зам. начальника отдела', 1200, '', '2008-08-15');
insert into ut__orm_users (user_id, user_dn, fname, patronymic, lname, email, rank, unit_id, extra_roles, terminated) values (2398, 'CN=Test User 2', 'Сергей', 'Анатольевич', 'Костин', 'unittest2@test.com', 'Начальник отдела', 800, '', NULL);
'''
    dbquery(sql)


def _initLOBData():
    pass


def _initPasswdData():
    sql = '''
delete from ut__auth_passwd;
insert into ut__auth_passwd (login, passwd_sha1, user_id) values ('user1', 'f0578f1e7174b1a41c4ea8c6e17f7a8a3b88c92a', 1);
insert into ut__auth_passwd (login, passwd_sha1, user_id) values ('user2', '8be52126a6fde450a7162a3651d589bb51e9579d', 2);
'''
    dbquery(sql)


def _initUsersData():
    sql = '''
delete from ut__auth_users;
insert into ut__auth_users (user_id, first_name, middle_name, last_name, email, role) values (1, 'Иван', 'Петрович', 'Сидоров', 'ivan@sidorov.net', 'manager1');
insert into ut__auth_users (user_id, first_name, middle_name, last_name, email, role) values (2, null, null, 'Пупкин', null, 'manager2');
insert into ut__auth_users (user_id, first_name, middle_name, last_name, email, role) values (3, 'Абдулла', null, null, null, null);
'''
    dbquery(sql)


def _initRolePermsData():
    sql = '''
delete from ut__auth_role_perms;
insert into ut__auth_role_perms (object, role, perm) values ('object1', 'manager1', 'view');
insert into ut__auth_role_perms (object, role, perm) values ('object1', 'manager1', 'edit');
insert into ut__auth_role_perms (object, role, perm) values ('object1', 'manager2', 'view');
insert into ut__auth_role_perms (object, role, perm) values ('object2', 'manager1', 'view');
insert into ut__auth_role_perms (object, role, perm) values ('object2', 'manager2', NULL);
'''
    dbquery(sql)


def _initUserPermsData():
    sql = '''
delete from ut__auth_user_perms;
insert into ut__auth_user_perms (object, user_id, perm) values ('object2', 2, 'edit');
'''
    dbquery(sql)


def _initRulesData():
    sql = '''
delete from ut__auth_rules;
insert into ut__auth_rules (object, perm, title) values ('object1', 'view', 'View Object 1');
insert into ut__auth_rules (object, perm, title) values ('object1', 'edit', 'Edit Object 1');
insert into ut__auth_rules (object, perm, title) values ('object2', 'view', 'View Object 2');
insert into ut__auth_rules (object, perm, title) values ('object2', ' ', 'Deny Access to Object 2');
insert into ut__auth_rules (object, perm, title) values ('object2', 'edit', 'Edit Object 2');
'''
    dbquery(sql)


def _initRoleRulesData():
    sql = '''
delete from ut__auth_role_rules;
insert into ut__auth_role_rules (role, object, perm) values ('manager1', 'object1', 'view');
insert into ut__auth_role_rules (role, object, perm) values ('manager1', 'object1', 'edit');
insert into ut__auth_role_rules (role, object, perm) values ('manager1', 'object2', 'view');
insert into ut__auth_role_rules (role, object, perm) values ('manager2', 'object1', 'view');
insert into ut__auth_role_rules (role, object, perm) values ('manager2', 'object2', ' ');
'''
    dbquery(sql)


def _initUserRulesData():
    sql = '''
delete from ut__auth_user_rules;
insert into ut__auth_user_rules (user_id, object, perm) values (2, 'object2', 'edit');
'''
    dbquery(sql)

# 
# Более актуальный вариант организации начальных данных для тестов.
#

from pyramid.parts.models.tests._test_data import PersonTestData
from pyramid.parts.models.tests._test_data import runSQL


class RoleTestData(object):
    u"""Test data for roles"""

    @classmethod
    def createTestData(self):    
        cmd = u"""
                insert into roles (role, title) values ('manager', 'Менеджер');
                insert into roles (role, title) values ('chief', 'Руководитель');
                insert into roles (role, title) values ('contractor', 'Контрагент');
                insert into roles (role, title) values ('admin', 'Администратор');
        """
        runSQL(cmd)
    

class SimpleUserTestData:
    
    @classmethod
    def createTestData(self):

        cmd = u"""
                insert into users (user_id, first_name, middle_name, last_name, email, role) values (1, 'Иван', 'Иванович', 'Иванов', 'user1@mail.ru', 'manager');
                insert into passwd (login, passwd_sha1, user_id) values ('ivanov', '40bd001563085fc35165329ea1ff5c5ecbdbbeef', 1);
                insert into users (user_id, first_name, middle_name, last_name, email, role) values (2, 'Петр', 'Петрович', 'Петров', 'user2@mail.ru', 'manager');
                insert into passwd (login, passwd_sha1, user_id) values ('petrov', '40bd001563085fc35165329ea1ff5c5ecbdbbeef', 2);
                insert into users (user_id, first_name, middle_name, last_name, email, role) values (3, 'Глеб', 'Глебович', 'Глебов', 'user3@mail.ru', 'chief');
                insert into passwd (login, passwd_sha1, user_id) values ('glebov', '40bd001563085fc35165329ea1ff5c5ecbdbbeef', 3);
                insert into users (user_id, first_name, middle_name, last_name, email, role) values (4, 'Сер', 'Серович', 'Серый', 'user4@mail.ru', 'contractor');
                insert into passwd (login, passwd_sha1, user_id) values ('ser', '40bd001563085fc35165329ea1ff5c5ecbdbbeef', 4);
                insert into users (user_id, first_name, middle_name, last_name, email, role) values (5, 'Админ', 'Админович', 'Админов', 'admin@mail.ru', 'admin');
                insert into passwd (login, passwd_sha1, user_id) values ('admin', '40bd001563085fc35165329ea1ff5c5ecbdbbeef', 5);

        """
        runSQL(cmd)


class UserPersonTestData:
        
    @classmethod
    def createTestData(self):

        PersonTestData.createTestData()
        
        sql_text = u'''
    insert into user_persons (user_id, role, person_id) values
        (1, 'manager', 1001);
    insert into user_persons (user_id, role, person_id) values
        (2, 'manager', 1002);
    insert into user_persons (user_id, role, person_id) values
        (3, 'manager', 1003);
    insert into user_persons (user_id, role, person_id) values
        (4, 'manager', 1004);
        '''

        runSQL(sql_text)
