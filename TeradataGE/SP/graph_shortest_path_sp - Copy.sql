REPLACE PROCEDURE [install_database].graph_shortest_path_sp
(
  IN in_tblname             VARCHAR(1024),
  IN in_from_node_name      VARCHAR(1024),
  IN in_to_node_name        VARCHAR(1024),
  IN in_weight_name         VARCHAR(1024),
  IN in_from_id             INTEGER,
  IN in_to_id               INTEGER,
  IN in_max_level           INTEGER,
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
  DECLARE to_id                   INTEGER;
  DECLARE max_level               INTEGER;
  DECLARE cur_level               INTEGER;
  DECLARE rec_cnt                 BIGINT;
  DECLARE current_cost            FLOAT;
  DECLARE lowest_cost             FLOAT;

  DECLARE sp_sql_code             INTEGER;
  DECLARE sp_sql_state            VARCHAR(10);

  DECLARE c1 CURSOR FOR s1;
  DECLARE c2 CURSOR WITH RETURN ONLY FOR s2;

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

  SET CondStr = 'AND a.to_id<>'||TRIM(in_to_id);
  SET current_cost = NULL;
  SET lowest_cost = NULL;
  SET rec_cnt = 0;

  ----------------------------------------
  -- Drop all volatile tables if exists --
  ----------------------------------------
  CALL [install_database].drop_vt_sp('all_possible_path_vt');

  -----------------------------------
  -- Prepare the data in 1st level --
  -----------------------------------
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


  WHILE (rec_cnt > 0 OR cur_level=1) AND (cur_level < max_level) DO
    -- Get current target cost  --
    SET SqlStr = 'SELECT MIN(weight) 
    FROM all_possible_path_vt
    WHERE path_level = '||TRIM(cur_level)||'
    AND to_id = '||TRIM(in_to_id);
    PREPARE s1 FROM SqlStr;
    OPEN c1;
    FETCH c1 INTO current_cost;
    CLOSE c1;
    IF current_cost IS NOT NULL THEN
      IF (lowest_cost IS NULL) OR (lowest_cost>current_cost) THEN
        SET lowest_cost = current_cost;
      END IF;
    END IF;

    IF lowest_cost IS NOT NULL THEN
      SET CondStr = 'AND new_weight<'||TRIM(lowest_cost);
    ELSE
      SET CondStr = '';
    END IF;
	
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
        AND a.to_id<>'||TRIM(in_to_id)||'
    '||CondStr||')
    ';
    EXECUTE IMMEDIATE SqlStr;
    SET rec_cnt = ACTIVITY_COUNT;
  
    SET SqlStr = 'COLLECT STAT ON all_possible_path_vt INDEX ( from_id ,to_id )';
    EXECUTE IMMEDIATE SqlStr;

	SET cur_level = cur_level + 1;

  END WHILE;

  ----------------------------------------
  -- Generate the final output dataset  --
  ----------------------------------------

  SET SqlStr = 'SELECT fullpath, weight
  FROM all_possible_path_vt
  WHERE to_id = '||TRIM(in_to_id)||'
  QUALIFY RANK() OVER (ORDER BY weight, path_level) = 1';

  IF in_output_tblname IS NOT NULL THEN
    CALL [install_database].drop_vt_sp(in_output_tblname);
    SET SqlStr2 = 'CREATE MULTISET TABLE '||in_output_tblname||' AS ('||SqlStr||') WITH DATA NO PRIMARY INDEX';
    EXECUTE IMMEDIATE SqlStr2;
  END IF;

  PREPARE s2 FROM SqlStr;
  OPEN c2;
  SET sp_sql_code = SQLCODE ;
  SET sp_sql_state = SQLSTATE ;
END;
