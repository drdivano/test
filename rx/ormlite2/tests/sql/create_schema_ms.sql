--------------------------------------------------------------------------------
-- Create Test ORMLite MS SQL Server-specific Data Structures
--
-- $Id: create_schema_ms.sql 643 2010-03-30 15:01:34Z vmozhaev $
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
    name NVARCHAR(500) NOT NULL,
    short_name NVARCHAR(500),
    unit_index VARCHAR(50),
    subst_unit_id INTEGER,
    subst_leader_id INTEGER,
    deleted_date DATE
);

CREATE TABLE ut__orm_users
(
    user_id INTEGER NOT NULL PRIMARY KEY,
    user_dn NVARCHAR(255) NOT NULL,
    fname NVARCHAR(100),
    patronymic NVARCHAR(100),
    lname NVARCHAR(100),
    email VARCHAR(255),
    rank NVARCHAR(255),
    unit_id INTEGER,
    extra_roles VARCHAR(255),
    terminated DATE
);


-- Test Authentication/Authorization -------------------------------------------

CREATE TABLE ut__auth_users
(
    user_id INTEGER PRIMARY KEY,
    first_name NVARCHAR(255),
    middle_name NVARCHAR(255),
    last_name NVARCHAR(255),
    email VARCHAR(255),
    role VARCHAR(255)
);

CREATE TABLE ut__auth_passwd 
(
    login VARCHAR(255) PRIMARY KEY,
    passwd_sha1 VARCHAR(40),
    user_id INTEGER
);

CREATE TABLE ut__auth_role_perms
(
    object VARCHAR(255),
    role VARCHAR(255),
    perm VARCHAR(255)
);

CREATE TABLE ut__auth_user_perms
(
    object VARCHAR(255),
    user_id INTEGER,
    perm VARCHAR(255)
 );

CREATE TABLE ut__auth_rules
(
    object VARCHAR(255),
    perm VARCHAR(255),
    title NVARCHAR(255),
    PRIMARY KEY (object, perm)
 );

CREATE TABLE ut__auth_role_rules
(
    role VARCHAR(255),
    object VARCHAR(255),
    perm VARCHAR(255),
    PRIMARY KEY (role, object, perm)
);

CREATE TABLE ut__auth_user_rules
(
    user_id INTEGER,
    object VARCHAR(255),
    perm VARCHAR(255),
    PRIMARY KEY (user_id, object, perm)
);


-- Test ORMLite MS SQL Server-specific ------------------------------------------------

CREATE TABLE ut__date_safe_deletable
(
    object_id INTEGER NOT NULL PRIMARY KEY,
    deleted_date DATETIME
);

CREATE TABLE ut__meta_data_aware
(
    object_id INTEGER NOT NULL PRIMARY KEY,
    created_date DATETIME NOT NULL,
    created_by INTEGER,
    modified_date DATETIME NOT NULL,
    modified_by INTEGER,
    deleted_date DATETIME,
    deleted_by INTEGER
);


CREATE TABLE ut__orm_test
(
    k VARCHAR(16) PRIMARY KEY,
    v VARCHAR(16),
    timestamp DATETIME DEFAULT SYSDATETIME(),
    amount DECIMAL(10, 4),
    rec_date DATE,
    rec_time DATETIME
);

CREATE TABLE ut__orm_lob_test
(
    k VARCHAR(16),
    data_lob VARBINARY(MAX)
);

--------------------------------------------------------------------------------
