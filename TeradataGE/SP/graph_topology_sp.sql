REPLACE PROCEDURE [install_database].graph_topology_sp
(
  IN in_tblname             VARCHAR(1024),
  IN in_from_node_name      VARCHAR(1024),
  IN in_to_node_name        VARCHAR(1024),
  IN in_weight_name         VARCHAR(1024),
  IN in_from_id             INTEGER,
  IN in_max_level           INTEGER,
  IN in_return_type         CHAR(1),
  IN in_output_tblname      VARCHAR(1024)        
)
DYNAMIC RESULT SETS 1
BEGIN
  DECLARE SqlStr                  VARCHAR(32000);
  DECLARE SqlStr2                 VARCHAR(1024);
  DECLARE CondStr                 VARCHAR(1024);
  DECLARE weight_name             VARCHAR(1024);
  DECLARE weight_name2            VARCHAR(1024);
  DECLARE from_id                 INTEGER;
  DECLARE max_level               INTEGER;
  DECLARE cur_level               INTEGER;
  DECLARE rec_cnt                 BIGINT;
  DECLARE return_type             CHAR(1);

  DECLARE sp_sql_code             INTEGER;
  DECLARE sp_sql_state            VARCHAR(10);

  DECLARE c1 CURSOR WITH RETURN ONLY FOR s1;

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

  SET from_id = in_from_id;

  IF in_max_level IS NULL THEN
    SET max_level = 10;
  ELSE
    SET max_level = in_max_level;
  END IF;

  IF in_return_type in ('p','P','n','N') THEN
    SET return_type = UPPER(in_return_type);
  ELSE
    SET return_type = 'P';
  END IF;

  SET CondStr = '';
  SET rec_cnt = 0;

  -- Drop all volatile tables if exists --
  CALL [install_database].drop_vt_sp('all_possible_path_vt');
  CALL [install_database].drop_vt_sp('cur_shortest_path_vt');


  -- Prepare the data in 1st level --
  SET cur_level = 1;
  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE all_possible_path_vt AS (
  SELECT 
    '||in_from_node_name||' AS from_id, 
    '||in_to_node_name||' AS to_id,
    '||weight_name||' AS weight,
    1(INTEGER) AS path_level,
    CAST(TRIM('||in_from_node_name||')||'',''||TRIM('||in_to_node_name||') AS VARCHAR(16000)) AS fullpath
  FROM '||TRIM(in_tblname)||'
  WHERE '||in_from_node_name||' ='''||TRIM(from_id)||'''
  AND '||in_from_node_name||' <> '||in_to_node_name||'
  ) WITH DATA
  PRIMARY INDEX (from_id, to_id)
  PARTITION BY path_level
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE cur_shortest_path_vt AS (
  SELECT to_id, MIN(weight) AS weight
  FROM all_possible_path_vt
  GROUP BY 1
  ) WITH DATA
  UNIQUE PRIMARY INDEX (to_id)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'INSERT INTO cur_shortest_path_vt VALUES ('||TRIM(from_id)||',0.0)';
  EXECUTE IMMEDIATE SqlStr;

  WHILE (rec_cnt > 0 OR cur_level=1) AND (cur_level < max_level) DO
    SET SqlStr = 'INSERT INTO all_possible_path_vt
    SELECT
      e.'||in_from_node_name||', e.'||in_to_node_name||',
      a.weight + '||weight_name2||' AS new_weight,
      a.path_level +1,
      a.fullpath||'',''||TRIM(e.'||in_to_node_name||') AS fullpath
    FROM all_possible_path_vt a 
    INNER JOIN '||TRIM(in_tblname)||' e
    ON (a.path_level=' || TRIM(cur_level) ||'
        AND a.to_id = e.'||in_from_node_name||'
        AND (e.'||in_from_node_name||', e.'||in_to_node_name||') NOT IN (SELECT from_id, to_id FROM all_possible_path_vt)
        AND (e.'||in_from_node_name||', e.'||in_to_node_name||') NOT IN (SELECT to_id, from_id FROM all_possible_path_vt)
    '||CondStr||' 
    )
    LEFT JOIN cur_shortest_path_vt c
    ON (e.'||in_to_node_name||' = c.to_id)
    WHERE (new_weight < c.weight OR c.weight IS NULL)
    ';
    EXECUTE IMMEDIATE SqlStr;
    SET rec_cnt = ACTIVITY_COUNT;

    
    SET SqlStr = 'COLLECT STAT ON all_possible_path_vt INDEX ( from_id ,to_id )';
    EXECUTE IMMEDIATE SqlStr;

    SET SqlStr = 'DELETE FROM cur_shortest_path_vt WHERE to_id <>'||TRIM(from_id);
    EXECUTE IMMEDIATE SqlStr;

    SET SqlStr = 'INSERT INTO cur_shortest_path_vt
    SELECT to_id, MIN(weight)
    FROM all_possible_path_vt
    GROUP BY to_id';
    EXECUTE IMMEDIATE SqlStr;

    SET cur_level = cur_level + 1;

  END WHILE;

  ----------------------------------------
  -- Generate the final output dataset  --
  ----------------------------------------
  IF in_output_tblname IS NOT NULL THEN
    CALL [install_database].drop_vt_sp(in_output_tblname);
  END IF;

  IF return_type ='P' THEN
    SET SqlStr = 'SELECT fullpath, weight
    FROM all_possible_path_vt';

    IF in_output_tblname IS NOT NULL THEN   
      SET SqlStr2 = 'CREATE MULTISET TABLE '||TRIM(in_output_tblname)||' AS ('||SqlStr||') WITH DATA NO PRIMARY INDEX';
      EXECUTE IMMEDIATE SqlStr2;
    END IF;
    
    SET SqlStr = SqlStr||' ORDER BY 2, 1;';

  ELSE
    SET SqlStr = 'SELECT to_id AS node_id, MIN(weight) AS weight
    FROM all_possible_path_vt
    GROUP BY 1 ';

    IF in_output_tblname IS NOT NULL THEN
      SET SqlStr2 = 'CREATE MULTISET TABLE '||TRIM(in_output_tblname)||' AS ('||SqlStr||') WITH DATA PRIMARY INDEX (node_id)';
      EXECUTE IMMEDIATE SqlStr2;
    END IF;

    SET SqlStr = SqlStr||' ORDER BY 2, 1;';

  END IF;


  PREPARE s1 FROM SqlStr;
  OPEN c1;
  SET sp_sql_code = SQLCODE ;
  SET sp_sql_state = SQLSTATE ;
END;


