--------------------------------------------------------------------------------
-- Drop system objects specific for MS SQL Server
--
-- $Id: drop_schema_ms.sql 643 2010-03-30 15:01:34Z vmozhaev $
--------------------------------------------------------------------------------


-- Global Sequence -------------------------------------------------------------

DROP PROCEDURE global_seq_nextval;
DROP TABLE global_seq;

--------------------------------------------------------------------------------