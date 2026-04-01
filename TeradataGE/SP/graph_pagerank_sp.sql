REPLACE PROCEDURE [install_database].graph_pagerank_sp
(
  IN in_edge_tblname              VARCHAR(1024),
  IN in_edge_from_colname         VARCHAR(1024),
  IN in_edge_to_colname           VARCHAR(1024),
  IN in_edge_weight_colname       VARCHAR(1024),
  IN in_p_direct                  CHAR(1),
  IN in_p_damping                 FLOAT,
  IN in_p_max_iters               INTEGER,
  IN in_p_tolerance               FLOAT,
  OUT out_v_iter                  INTEGER
)
BEGIN

  DECLARE SqlStr                  VARCHAR(32000);
  DECLARE CondStr                 VARCHAR(1024);

  DECLARE v_iter       INTEGER;
  DECLARE v_n          FLOAT;
  DECLARE v_dangling   FLOAT;
  DECLARE v_teleport   FLOAT;
  DECLARE v_dang_share FLOAT;
  DECLARE v_init_score FLOAT;
  DECLARE v_max_diff   FLOAT;

  DECLARE c1 CURSOR FOR s1;

  ----------------------
  -- Setup parameters --
  ----------------------
  IF in_edge_weight_colname IS NOT NULL THEN
    SET CondStr = 'CAST('||TRIM(in_edge_weight_colname)||' AS FLOAT) AS wt';
  ELSE
    SET CondStr = 'CAST(1.00000 AS FLOAT) AS wt';
  END IF;


  ----------------------------------------
  -- Drop all volatile tables if exists --
  ----------------------------------------
  CALL [install_database].drop_vt_sp('pr_sp_work_edges_vt');
  CALL [install_database].drop_vt_sp('pr_sp_nodes_vt');
  CALL [install_database].drop_vt_sp('pr_sp_out_vt');
  CALL [install_database].drop_vt_sp('pr_sp_rank_curr_vt');
  CALL [install_database].drop_vt_sp('pr_sp_rank_next_vt');
  CALL [install_database].drop_vt_sp('pr_sp_result_vt');


  ----------------------------
  -- Create volatile tables --
  ----------------------------
  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE pr_sp_work_edges_vt (
    src      INTEGER,
    dst      INTEGER,
    wt       FLOAT
    ) PRIMARY INDEX (src)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE pr_sp_nodes_vt (
    node     INTEGER
    ) PRIMARY INDEX (node)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE  pr_sp_out_vt (
    src      INTEGER,
    total_wt FLOAT
    ) PRIMARY INDEX (src)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE pr_sp_rank_curr_vt (
    node     INTEGER,
    pr_score FLOAT
    ) PRIMARY INDEX (node) 
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE pr_sp_rank_next_vt (
    node     INTEGER,
    pr_score FLOAT
    ) PRIMARY INDEX (node)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'CREATE VOLATILE MULTISET TABLE pr_sp_result_vt (
    node     INTEGER,
    pr_score FLOAT
    ) PRIMARY INDEX (node)
  ON COMMIT PRESERVE ROWS;';
  EXECUTE IMMEDIATE SqlStr;


  ---------------------------------
  -- Step 1: Build working edges --
  ---------------------------------
  IF in_p_direct = 'Y' THEN
    SET SqlStr = 'INSERT INTO pr_sp_work_edges_vt
      SELECT '||TRIM(in_edge_from_colname)||' AS src, '||TRIM(in_edge_to_colname)||' AS dst,
      '||CondStr||'
      FROM '||TRIM(in_edge_tblname)||';';
    EXECUTE IMMEDIATE SqlStr;
  ELSE
    SET SqlStr = 'INSERT INTO pr_sp_work_edges_vt
      SELECT '||TRIM(in_edge_from_colname)||' AS src, '||TRIM(in_edge_to_colname)||' AS dst,
      '||CondStr||'
      FROM '||TRIM(in_edge_tblname)||';';
    EXECUTE IMMEDIATE SqlStr;
    SET SqlStr = 'INSERT INTO pr_sp_work_edges_vt
      SELECT '||TRIM(in_edge_to_colname)||' AS src, '||TRIM(in_edge_from_colname)||' AS dst,
      '||CondStr||'
      FROM '||TRIM(in_edge_tblname)||'
      WHERE NOT EXISTS (
        SELECT 1 FROM pr_sp_work_edges_vt w
        WHERE w.src = '||in_edge_tblname||'.'||TRIM(in_edge_to_colname)||' 
        AND w.dst = '||in_edge_tblname||'.'||TRIM(in_edge_from_colname)||'
       );';
    EXECUTE IMMEDIATE SqlStr;
  END IF;


  -----------------------------
  -- Step 2: Build node list --
  -----------------------------
  SET SqlStr = 'INSERT INTO pr_sp_nodes_vt
    SELECT DISTINCT node FROM (
    SELECT DISTINCT src AS node FROM pr_sp_work_edges_vt
    UNION ALL 
    SELECT DISTINCT dst AS node FROM pr_sp_work_edges_vt
    ) t;';
  EXECUTE IMMEDIATE SqlStr;
  SET v_n = CAST(ACTIVITY_COUNT AS FLOAT);


  ----------------------------------------
  -- Step 3: Outgoing weight per source --
  ----------------------------------------
  SET SqlStr = 'INSERT INTO pr_sp_out_vt
    SELECT src, CAST(SUM(wt) AS FLOAT) FROM pr_sp_work_edges_vt 
    GROUP BY src;';
  EXECUTE IMMEDIATE SqlStr;


  -----------------------------------------------------------------------
  -- Step 4: Collect statistics on join columns for optimizer accuracy --
  -----------------------------------------------------------------------
  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_work_edges_vt COLUMN (src);';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_work_edges_vt COLUMN (dst);';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_nodes_vt COLUMN (node);';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_out_vt COLUMN (src);';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_rank_curr_vt COLUMN (node);';
  EXECUTE IMMEDIATE SqlStr;

  SET SqlStr = 'COLLECT STATISTICS ON pr_sp_rank_next_vt COLUMN (node);';
  EXECUTE IMMEDIATE SqlStr;


  ------------------------------------
  -- Step 5: Initialize ranks = 1/N --
  ------------------------------------
  SET v_init_score = CAST(1.0 AS FLOAT) / v_n;
  SET SqlStr = 'INSERT INTO pr_sp_rank_curr_vt
    SELECT node, '||v_init_score||' FROM pr_sp_nodes_vt;';
  EXECUTE IMMEDIATE SqlStr;


  ---------------------------------
  -- Step 6: --
  ---------------------------------
  SET v_iter = 0;
  SET v_max_diff = CAST(1.0 AS FLOAT);

  WHILE v_iter < in_p_max_iters AND v_max_diff >= in_p_tolerance DO

    /* Dangling node mass */
    SET SqlStr = 'SELECT CAST(COALESCE(SUM(rc.pr_score), 0.0) AS FLOAT)
      FROM pr_sp_rank_curr_vt rc
      LEFT JOIN pr_sp_out_vt o ON o.src = rc.node
      WHERE o.src IS NULL;';
    PREPARE s1 FROM SqlStr;
    OPEN c1;
    FETCH c1 INTO v_dangling;
    CLOSE c1;

    SET v_teleport   = CAST((CAST(1.0 AS FLOAT) - in_p_damping) / v_n AS FLOAT);
    SET v_dang_share = CAST(in_p_damping * v_dangling / v_n AS FLOAT);

    /* Compute next iteration */
    SET SqlStr = 'DELETE FROM pr_sp_rank_next_vt;';
    EXECUTE IMMEDIATE SqlStr;


    SET SqlStr = 'INSERT INTO pr_sp_rank_next_vt
      SELECT
        nd.node,
        CAST(
             '||TRIM(v_teleport)||'
             + '||TRIM(in_p_damping)||' * COALESCE(incoming.contrib, CAST(0.0 AS FLOAT))
             + '||TRIM(v_dang_share)||'
        AS FLOAT)
      FROM pr_sp_nodes_vt nd
      LEFT JOIN (
        SELECT
           e.dst AS node,
            CAST(SUM(rc.pr_score * e.wt / o.total_wt) AS FLOAT) AS contrib
        FROM pr_sp_work_edges_vt e
        INNER JOIN pr_sp_rank_curr_vt rc ON rc.node = e.src
        INNER JOIN pr_sp_out_vt o ON o.src = e.src
        GROUP BY e.dst
        ) incoming
        ON nd.node = incoming.node;';
    EXECUTE IMMEDIATE SqlStr;

    /* Convergence check: max absolute difference */
    SET SqlStr = 'SELECT CAST(MAX(ABS(nxt.pr_score - cur.pr_score)) AS FLOAT)
      FROM pr_sp_rank_next_vt nxt
      INNER JOIN pr_sp_rank_curr_vt cur ON cur.node = nxt.node;';
    PREPARE s1 FROM SqlStr;
    OPEN c1;
    FETCH c1 INTO v_max_diff;
    CLOSE c1;

    /* Swap current and next */
    SET SqlStr = 'DELETE FROM pr_sp_rank_curr_vt;';
    EXECUTE IMMEDIATE SqlStr;
    SET SqlStr = 'INSERT INTO pr_sp_rank_curr_vt 
      SELECT node, pr_score FROM pr_sp_rank_next_vt;';
    EXECUTE IMMEDIATE SqlStr;

    SET v_iter = v_iter + 1;

  END WHILE;



  ------------------------------------------------
  -- Step 7: Write results with iteration count --
  ------------------------------------------------
  SET SqlStr = 'INSERT INTO pr_sp_result_vt 
    SELECT node, pr_score
    FROM pr_sp_rank_curr_vt;';

  EXECUTE IMMEDIATE SqlStr;

  SET out_v_iter = v_iter;

END;
