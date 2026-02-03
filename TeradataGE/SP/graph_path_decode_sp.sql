REPLACE PROCEDURE [install_database].graph_path_decode_sp
(
  IN in_path_tblname              VARCHAR(1024),
  IN in_path_colname              VARCHAR(1024),
  IN in_edge_tblname              VARCHAR(1024),
  IN in_edge_from_colname         VARCHAR(1024),
  IN in_edge_to_colname           VARCHAR(1024),
  IN in_edge_labels_colname       VARCHAR(1024),
  IN in_node_tblname              VARCHAR(1024),
  IN in_node_colname              VARCHAR(1024),
  IN in_node_label_colname        VARCHAR(1024),
  IN in_output_tblname            VARCHAR(1024)        
)
DYNAMIC RESULT SETS 1
BEGIN
  DECLARE SqlStr                  VARCHAR(32000);
  DECLARE edge_labels_colname     VARCHAR(32000);
  DECLARE sp_sql_code             INTEGER;
  DECLARE sp_sql_state            VARCHAR(10);

  DECLARE c1 CURSOR WITH RETURN ONLY FOR s1;

  -- Drop all volatile tables if exists --
  CALL [install_database].drop_vt_sp('graph_topoplogy_decode_vt');

  -- Select the columnsname --
  SELECT OREPLACE(:in_edge_labels_colname, '|', ',e.') INTO :edge_labels_colname;
  SET edge_labels_colname = 'e.'||edge_labels_colname;

  ----------------------------------------
  -- Prepare all the unique pairs nodes --
  ----------------------------------------
  SET SqlStr = 'CREATE MULTISET VOLATILE TABLE graph_topoplogy_decode_vt AS (
  WITH RECURSIVE split_cte (fullpath, token_no, token1, token2) AS
  (
    SELECT
      '||in_path_colname||' AS fullpath,
      1 AS token_no,
      STRTOK('||in_path_colname||', '','', 1) AS token1,
      STRTOK('||in_path_colname||', '','', 2) AS token2
    FROM '||in_path_tblname||'
    UNION ALL
    SELECT
      fullpath,
      token_no + 1,
      STRTOK(fullpath, '','', token_no + 1) AS token1,
      STRTOK(fullpath, '','', token_no + 2) AS token2
    FROM split_cte
    WHERE STRTOK(fullpath, '','', token_no + 2) IS NOT NULL
  )
  SELECT fullpath, token_no, 
  CAST(token1 AS BIGINT) AS from_id,
  CAST(token2 AS BIGINT) AS to_id
  FROM split_cte
  ) WITH DATA
  PRIMARY INDEX (from_id, to_id)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;


  SET SqlStr = 'COLLECT STAT ON graph_topoplogy_decode_vt INDEX (from_id, to_id);';
  EXECUTE IMMEDIATE SqlStr;


  -------------------------------
  -- Prepare the return result --
  -------------------------------
  SET SqlStr = 'SELECT
    t.token_no,
    t.from_id, t.to_id,
    n1.'||in_node_label_colname||' AS n1_label,
    '||edge_labels_colname||',
    n2.'||in_node_label_colname||' AS n2_label
  FROM 
    (SELECT from_id, to_id, MIN(token_no) AS token_no FROM graph_topoplogy_decode_vt GROUP BY 1,2) t,
    '||in_node_tblname||' n1,
    '||in_node_tblname||' n2,
    '||in_edge_tblname||' e
  WHERE t.from_id = n1.'||in_node_colname||'
  AND   t.to_id = n2.'||in_node_colname||'
  AND   t.from_id = e.'||in_edge_from_colname||'
  AND   t.to_id   = e.'||in_edge_to_colname||'
  ';

  IF in_output_tblname IS NULL OR in_output_tblname='' THEN
    SET SqlStr = SqlStr||' ORDER BY 1,2,3';
  ELSE
    CALL [install_database].drop_vt_sp(in_output_tblname);
    SET SqlStr = 'CREATE MULTISET TABLE '||in_output_tblname||' AS ('||SqlStr||') WITH DATA PRIMARY INDEX (token_no, from_id, to_id)';
    EXECUTE IMMEDIATE SqlStr;
    SET SqlStr = 'SELECT NULL';
  END IF;

  PREPARE s1 FROM SqlStr;
  OPEN c1;
  SET sp_sql_code = SQLCODE ;
  SET sp_sql_state = SQLSTATE ;
END;

