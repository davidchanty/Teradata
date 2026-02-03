REPLACE PROCEDURE [install_database].drop_vt_sp
(
    IN   tbl_name         VARCHAR(100)
)
BEGIN

  --------------------------
  -- Define all variables --
  --------------------------
  DECLARE check_point_id   INTEGER      DEFAULT 1;
  DECLARE RC               INTEGER;

  DECLARE SqlStmt          VARCHAR(10000);

  ---------------------------------
  -- SQL EXCEPTION HANDLER BLOCK --
  ---------------------------------

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    IF SQLCODE = 3807 THEN 
      SET RC = 0;
    ELSE
      RESIGNAL;
    END IF;
  END;

  -----------------
  -- Start OF SP --
  -----------------
  SET SqlStmt = 'DROP TABLE '||TRIM(tbl_name)||';';

  CALL DBC.SysExecSQL(SqlStmt);

  ---------------
  -- END OF SP --
  ---------------
  SET RC = 0;

END;
