REPLACE PROCEDURE graphlib.graph_shortest_paths_multi
(
  IN in_dbname              VARCHAR(1024),
  IN in_tblname             VARCHAR(1024),
  IN in_from_node_name      VARCHAR(1024),
  IN in_to_node_name        VARCHAR(1024),
  IN in_weight_name         VARCHAR(1024),
  IN in_directed            BYTEINT,
  IN in_src_dst_tblname     VARCHAR(1024),
  IN in_src_node_name       VARCHAR(1024),
  IN in_trg_node_name       VARCHAR(1024),
  IN in_max_level           INTEGER,
  IN in_output_tblname      VARCHAR(1024),
  IN in_output_volatile     BYTEINT
)
BEGIN
  DECLARE SqlStr                  VARCHAR(32000);
  DECLARE SqlStr2                 VARCHAR(1024);
  DECLARE CondStr                 VARCHAR(1024);
  DECLARE weight_name             VARCHAR(1024);
  DECLARE weight_name2            VARCHAR(1024);
  DECLARE from_id                 INTEGER;
  DECLARE to_id                   INTEGER;
  DECLARE max_level               INTEGER;
  DECLARE cur_level               INTEGER;
  DECLARE rec_cnt                 BIGINT;
  DECLARE current_cost            FLOAT;
  DECLARE lowest_cost             FLOAT;

  DECLARE sp_sql_code             INTEGER;
  DECLARE sp_sql_state            VARCHAR(10);

  DECLARE c1 CURSOR FOR s1;


  ----------------------
  -- Setup parameters --
  ----------------------
  IF in_weight_name IS NULL OR in_weight_name = '' THEN
    SET weight_name = '1.0(FLOAT) ';
    SET weight_name2 = '1.0';
  ELSE
    SET weight_name = TRIM(in_weight_name);
    SET weight_name2 = 'e.'||TRIM(in_weight_name);
  END IF;


  IF in_max_level IS NULL THEN
    SET max_level = 10;
  ELSE
    SET max_level = in_max_level;
  END IF;


  ----------------------------------------
  -- Drop all volatile tables if exists --
  ----------------------------------------
  CALL graphlib.drop_vt_sp('cur_best_path_vt');
  CALL graphlib.drop_vt_sp('edges_vt');


  --------------------------------------------------
  -- Step 1. Setup the initial Cost for all pairs --
  --------------------------------------------------
  SET SqlStr =
    'CREATE MULTISET VOLATILE TABLE cur_best_path_vt AS (
     SELECT 
       CAST('||TRIM(in_src_node_name)||' AS BIGINT) AS src,
       CAST('||TRIM(in_trg_node_name)||' AS BIGINT) AS trg,
       NULL(FLOAT) AS cur_weight
     FROM '||TRIM(in_src_dst_tblname)||'
     WHERE src <> trg 
     ) WITH DATA
     PRIMARY INDEX (src)
     ON COMMIT PRESERVE ROWS';
  EXECUTE IMMEDIATE SqlStr;


  -------------------------------------------------------------------------------
  -- Step 2. Edge list
  -- A weight column is always present; set to 1.0 for the unweighted case
  -- so a single schema serves both algorithm branches.
  -------------------------------------------------------------------------------
  SET SqlStr =
    'CREATE MULTISET VOLATILE TABLE edges_vt AS (
     SELECT
       CAST('||in_from_node_name||' AS BIGINT) AS src,
       CAST('||in_to_node_name||' AS BIGINT) AS trg,
       CAST('||weight_name||' AS FLOAT) AS weight
     FROM '||TRIM(in_dbname)||'.'||TRIM(in_tblname)||'
     WHERE src<>trg
     ) WITH DATA
     PRIMARY INDEX (src)
     ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  -- Undirected: materialise the reverse direction; skip self-loops
  IF in_directed = 0 THEN
    SET SqlStr = 
      'INSERT INTO vt_edges
       SELECT trg, src, weight
       FROM vt_edges
       WHERE src <> trg;';
    EXECUTE IMMEDIATE SqlStr;
  END IF;


END;


