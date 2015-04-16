--------------------------------------------------------------------------------
-- Create Test data structures specific for Oracle
--
-- $Id: create_schema_ora.sql 643 2010-03-30 15:01:34Z vmozhaev $
--------------------------------------------------------------------------------


-- Test ORMLite ----------------------------------------------------------------

CREATE TABLE ut__orm_objects
(
    object_id INTEGER,
    deleted_date DATE
);

CREATE TABLE ut__orm_months (
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    status CHAR(1),
    PRIMARY KEY(month, year)
);

CREATE TABLE ut__orm_reports (
    report_id INTEGER PRIMARY KEY,
    month INTEGER,
    year INTEGER
);

CREATE TABLE ut__orm_units
(
    unit_id INTEGER NOT NULL PRIMARY KEY,
    leader_id INTEGER,
    name NVARCHAR2(500) NOT NULL,
    short_name NVARCHAR2(500),
    unit_index VARCHAR2(50),
    subst_unit_id INTEGER,
    subst_leader_id INTEGER,
    deleted_date DATE
);

CREATE TABLE ut__orm_users
(
    user_id INTEGER NOT NULL PRIMARY KEY,
    user_dn NVARCHAR2(255) NOT NULL,
    fname NVARCHAR2(100),
    patronymic NVARCHAR2(100),
    lname NVARCHAR2(100),
    email VARCHAR2(255),
    rank NVARCHAR2(255),
    unit_id INTEGER
        REFERENCES ut__orm_units (unit_id) DEFERRABLE,
    extra_roles VARCHAR2(255),
    terminated DATE
);


-- Test Authentication/Authorization -------------------------------------------

CREATE TABLE ut__auth_users
(
    user_id INTEGER PRIMARY KEY,
    first_name NVARCHAR2(255),
    middle_name NVARCHAR2(255),
    last_name NVARCHAR2(255),
    email VARCHAR2(255),
    role VARCHAR2(255)
);

CREATE TABLE ut__auth_passwd 
(
    login VARCHAR2(255) PRIMARY KEY,
    passwd_sha1 VARCHAR2(40),
    user_id INTEGER
);

CREATE TABLE ut__auth_role_perms
(
    object VARCHAR2(255),
    role VARCHAR2(255),
    perm VARCHAR2(255)
);

CREATE TABLE ut__auth_user_perms
(
    object VARCHAR2(255),
    user_id INTEGER,
    perm VARCHAR2(255)
 );

CREATE TABLE ut__auth_rules
(
    object VARCHAR2(255),
    perm VARCHAR2(255),
    title NVARCHAR2(255),
    PRIMARY KEY (object, perm)
 );

CREATE TABLE ut__auth_role_rules
(
    role VARCHAR2(255),
    object VARCHAR2(255),
    perm VARCHAR2(255),
    PRIMARY KEY (role, object, perm),
    FOREIGN KEY (object, perm)
        REFERENCES ut__auth_rules (object, perm) DEFERRABLE
);

CREATE TABLE ut__auth_user_rules
(
    user_id INTEGER,
    object VARCHAR2(255),
    perm VARCHAR2(255),
    PRIMARY KEY (user_id, object, perm),
    FOREIGN KEY (object, perm)
        REFERENCES ut__auth_rules (object, perm) DEFERRABLE
);


-- Test ORMLite Oracle-specific ------------------------------------------------

CREATE TABLE ut__date_safe_deletable
(
    object_id INTEGER NOT NULL PRIMARY KEY,
    deleted_date TIMESTAMP
);

CREATE TABLE ut__meta_data_aware
(
    object_id INTEGER NOT NULL PRIMARY KEY,
    created_date TIMESTAMP NOT NULL,
    created_by INTEGER,
    modified_date TIMESTAMP NOT NULL,
    modified_by INTEGER,
    deleted_date TIMESTAMP,
    deleted_by INTEGER
);


CREATE TABLE ut__orm_test
(
    k VARCHAR2(16) PRIMARY KEY,
    v VARCHAR2(16),
    timestamp DATE DEFAULT SYSDATE,
    amount DECIMAL(10, 4),
    rec_date DATE,
    rec_time TIMESTAMP
);

CREATE TABLE ut__orm_lob_test
(
    k VARCHAR2(16),
    data_lob BLOB
);

--------------------------------------------------------------------------------
