--------------------------------------------------------------------------------
-- Drop Test ORMLite Data Structures
--
-- $Id: drop_schema.sql 643 2010-03-30 15:01:34Z vmozhaev $
--------------------------------------------------------------------------------


-- Test Models -----------------------------------------------------------------

DROP TABLE ut__meta_data_aware;
DROP TABLE ut__date_safe_deletable;
DROP TABLE ut__bool_test;

-- Test Authentication/Authorization -------------------------------------------

DROP TABLE ut__auth_user_rules;
DROP TABLE ut__auth_role_rules;
DROP TABLE ut__auth_rules;
DROP TABLE ut__auth_user_perms;
DROP TABLE ut__auth_role_perms;
DROP TABLE ut__auth_passwd; 
DROP TABLE ut__auth_users;


-- Test ORMLite ----------------------------------------------------------------

DROP TABLE ut__orm_users;
DROP TABLE ut__orm_units;
DROP TABLE ut__orm_reports;
DROP TABLE ut__orm_months;
DROP TABLE ut__orm_objects;
DROP TABLE ut__orm_lob_test;
DROP TABLE ut__orm_test;

--------------------------------------------------------------------------------
