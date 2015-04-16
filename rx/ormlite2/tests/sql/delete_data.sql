--------------------------------------------------------------------------------
-- Delete Test ORMLite Data
--
-- $Id: delete_data.sql 643 2010-03-30 15:01:34Z vmozhaev $
--------------------------------------------------------------------------------


-- Test Models -----------------------------------------------------------------

DELETE FROM ut__meta_data_aware;
DELETE FROM ut__date_safe_deletable;
DELETE FROM ut__bool_test;

-- Test Authentication/Authorization -------------------------------------------

DELETE FROM ut__auth_user_rules;
DELETE FROM ut__auth_role_rules;
DELETE FROM ut__auth_rules;
DELETE FROM ut__auth_user_perms;
DELETE FROM ut__auth_role_perms;
DELETE FROM ut__auth_passwd; 
DELETE FROM ut__auth_users;


-- Test ORMLite ----------------------------------------------------------------

DELETE FROM ut__orm_users;
DELETE FROM ut__orm_units;
DELETE FROM ut__orm_reports;
DELETE FROM ut__orm_months;
DELETE FROM ut__orm_objects;
DELETE FROM ut__orm_lob_test;
DELETE FROM ut__orm_test;

--------------------------------------------------------------------------------