--------------------------------------------------------------------------------
-- Create system objects specific for MS SQL Server
--
-- $Id: create_schema_ms.sql 643 2010-03-30 15:01:34Z vmozhaev $
--------------------------------------------------------------------------------


-- Global Sequence -------------------------------------------------------------

CREATE PROCEDURE global_seq_nextval AS
BEGIN
    DECLARE @nextval INTEGER
    SET NOCOUNT ON
    INSERT INTO global_seq (seq_value) values ('a')
    SET @nextval = scope_identity()
    DELETE FROM global_seq WITH (READPAST)
    RETURN @nextval
END;

CREATE TABLE global_seq (
    seq_id INTEGER IDENTITY(1001, 1) PRIMARY KEY,
    seq_value VARCHAR(1) 
);


--------------------------------------------------------------------------------
