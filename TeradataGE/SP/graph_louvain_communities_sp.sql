REPLACE PROCEDURE [install_database].graph_louvain_communities_sp (
    IN in_dbname                 VARCHAR(1024),
    IN in_tblname                VARCHAR(1024),
    IN in_from_node_name         VARCHAR(1024),
    IN in_to_node_name           VARCHAR(1024),
    IN in_weight_name            VARCHAR(1024),
    IN in_direct                 BYTEINT,
    IN in_pr_tblname             VARCHAR(1024),
    IN in_pr_node_name           VARCHAR(1024),
    IN in_pr_score_name          VARCHAR(1024),
    IN p_threshold               FLOAT,
    IN p_max_iter                INTEGER,
    IN p_resolution              FLOAT,
    IN in_output_tblname         VARCHAR(1024),
    IN in_output_volatile        BYTEINT,
    OUT p_iterations             INTEGER,
    OUT p_communities            INTEGER,
    OUT p_nodes                  INTEGER,
    OUT p_modularity             FLOAT
)
BEGIN

    DECLARE SqlStr      VARCHAR(32000);
    DECLARE v_changed   INTEGER DEFAULT 1;
    DECLARE v_iter      INTEGER DEFAULT 0;
    DECLARE v_count     INTEGER DEFAULT 0;
    DECLARE v_m         FLOAT   DEFAULT 1.0;
    DECLARE v_two_m     FLOAT   DEFAULT 2.0;
    DECLARE v_m_str     VARCHAR(50);
    DECLARE v_2m2_str   VARCHAR(50);
    DECLARE v_res_str   VARCHAR(50);
    DECLARE weight_name VARCHAR(1024);
    DECLARE c1 CURSOR FOR s1;

    -- Setup parameters --
    IF in_weight_name IS NULL THEN
        SET weight_name = '1';
    ELSE
        SET weight_name = in_weight_name;
    END IF;

    -- Pre-clean: drop any leftover volatile tables from prior runs --
    CALL [install_database].drop_vt_sp('vt_edges');
    CALL [install_database].drop_vt_sp('vt_degree');
    CALL [install_database].drop_vt_sp('vt_community');
    CALL [install_database].drop_vt_sp('vt_comm_sigma');
    CALL [install_database].drop_vt_sp('vt_best_comm');


    -- =========================================================================
    -- STEP 1  Build bidirectional weighted edge list
    --         Store both directions (A-->B and B-->A) so gain propagates both ways.
    --         Edge weight = similarity score from in_tblname.
    -- =========================================================================
    IF in_direct > 0 THEN
        SET SqlStr =
        'CREATE MULTISET VOLATILE TABLE vt_edges AS (
             SELECT '||TRIM(in_from_node_name)||'             AS node_a,
                    '||TRIM(in_to_node_name)||'             AS node_b,
                    CAST('||TRIM(weight_name)||' AS FLOAT) AS weight
             FROM   '||TRIM(in_dbname)||'.'||TRIM(in_tblname)||'
             WHERE  score >= ' || TRIM(p_threshold) || '
             UNION ALL
             SELECT '||TRIM(in_to_node_name)||',
                    '||TRIM(in_from_node_name)||',
                    CAST('||TRIM(weight_name)||' AS FLOAT)
             FROM   '||TRIM(in_dbname)||'.'||TRIM(in_tblname)||'
             WHERE  score >= ' || TRIM(p_threshold) ||
        ' ) WITH DATA
          PRIMARY INDEX (node_a)
          ON COMMIT PRESERVE ROWS;';
    ELSE
        SET SqlStr =
        'CREATE MULTISET VOLATILE TABLE vt_edges AS (
             SELECT '||TRIM(in_from_node_name)||'             AS node_a,
                    '||TRIM(in_to_node_name)||'             AS node_b,
                    CAST('||TRIM(weight_name)||' AS FLOAT) AS weight
             FROM   '||TRIM(in_dbname)||'.'||TRIM(in_tblname)||'
             WHERE  score >= ' || TRIM(p_threshold) || '
          ) WITH DATA
          PRIMARY INDEX (node_a)
          ON COMMIT PRESERVE ROWS;';

    END IF;
    EXECUTE IMMEDIATE SqlStr;


    -- =========================================================================
    -- STEP 2  Compute node degrees k(i) = SUM edge weights incident to node i
    --         (bidirectional table means we sum only one direction --> correct k(i))
    -- =========================================================================
    SET SqlStr =
        'CREATE MULTISET VOLATILE TABLE vt_degree AS (
             SELECT node_a AS node,
                    SUM(weight) AS ki
             FROM   vt_edges
             GROUP BY node_a
         ) WITH DATA
         PRIMARY INDEX (node)
         ON COMMIT PRESERVE ROWS;';
    EXECUTE IMMEDIATE SqlStr;


    -- STEP 3 is removed --

    -- =========================================================================
    -- STEP 4  Initialise community labels
    --         Every node begins as its own community (comm_id = node).
    --         Isolated nodes (present in pr_score but below threshold) are
    --         included so the output covers the full node population.
    -- =========================================================================
    SET SqlStr = 
        'CREATE MULTISET VOLATILE TABLE vt_community AS (
             SELECT '||TRIM(in_pr_node_name)||' AS node, '||TRIM(in_pr_node_name)||' AS comm_id 
             FROM vt_degree
             UNION
             SELECT CAST('||TRIM(in_pr_node_name)||' AS BIGINT) AS node, CAST('||TRIM(in_pr_node_name)||' AS BIGINT) AS comm_id 
             FROM   '||TRIM(in_dbname)||'.'||TRIM(in_pr_tblname)||'
         ) WITH DATA
         PRIMARY INDEX (node)
         ON COMMIT PRESERVE ROWS;';
    EXECUTE IMMEDIATE SqlStr;


    -- =========================================================================
    -- STEP 5  Initialise community degree sums
    --         Initially each community holds exactly one node
    -- =========================================================================
    SET SqlStr = 
        'CREATE MULTISET VOLATILE TABLE vt_comm_sigma AS (
             SELECT c.comm_id,
                    SUM(d.ki) AS sigma_tot
             FROM   vt_community c
             JOIN   vt_degree    d ON d.node = c.node
             GROUP BY c.comm_id
         ) WITH DATA
         PRIMARY INDEX (comm_id)
         ON COMMIT PRESERVE ROWS;';
    EXECUTE IMMEDIATE SqlStr;


    -- =========================================================================
    -- STEP 6  Phase 1: Iterative local modularity optimisation

    -- =========================================================================
    WHILE v_changed > 0 AND v_iter < p_max_iter DO

        SET v_iter = v_iter + 1;

        -- a) Find best community for every node
        SET SqlStr =
            'CREATE MULTISET VOLATILE TABLE vt_best_comm AS (
                 SELECT node, best_comm_id
                 FROM (
                     SELECT
                         c_src.node,
                         c_nbr.comm_id AS best_comm_id,
                         -- Modularity gain formula
                         SUM(e.weight) / t.v_m
                           - ('||TRIM(CAST(1.000 AS DECIMAL(4,3)))||' * csg.sigma_tot * d.ki)
                             / t.v_2m2 AS gain,
                         -- Window MAX to identify the single best community
                         MAX( SUM(e.weight) / t.v_m
                                - ('||TRIM(CAST(1.000 AS DECIMAL(4,3)))||' * csg.sigma_tot * d.ki)
                                  / t.v_2m2 )
                              OVER (PARTITION BY c_src.node)          AS max_gain,
                         -- Tie-break: prefer smallest comm_id
                         MIN(c_nbr.comm_id)
                              OVER (PARTITION BY c_src.node,
                                    SUM(e.weight) / t.v_m
                                      - ('||TRIM(CAST(1.000 AS DECIMAL(4,3)))||' * csg.sigma_tot * d.ki)
                                        / t.v_2m2)        AS tie_break_comm

                     FROM   vt_community  c_src
                     JOIN   vt_edges      e     ON  e.node_a   = c_src.node
                     JOIN   vt_community  c_nbr ON  c_nbr.node = e.node_b
                     JOIN   vt_degree     d     ON  d.node     = c_src.node
                     JOIN   vt_comm_sigma csg    ON  csg.comm_id = c_nbr.comm_id
                     JOIN    (SELECT SUM(weight) / 2.0 AS v_m, 2 * v_m * v_m AS v_2m2 FROM vt_edges) t ON 1=1
                     WHERE  c_nbr.comm_id <> c_src.comm_id     -- only other communities
                     GROUP BY c_src.node, c_nbr.comm_id, d.ki, csg.sigma_tot
                 ) ranked
                 WHERE gain  = max_gain           -- best gain row
                   AND gain  > 0.0                -- must be a real improvement
                   AND best_comm_id = tie_break_comm  -- deterministic tie-break
             ) WITH DATA
             PRIMARY INDEX (node)
             ON COMMIT PRESERVE ROWS;';
        EXECUTE IMMEDIATE SqlStr;

        -- b) Apply community reassignments
        SET SqlStr = 'UPDATE vt_community
        FROM   vt_best_comm bc
        SET    comm_id  = bc.best_comm_id
        WHERE  vt_community.node     = bc.node
          AND  vt_community.comm_id <> bc.best_comm_id;';
        EXECUTE IMMEDIATE SqlStr;
        SET v_changed = ACTIVITY_COUNT;

        --  c) Recompute delta_tot after batch reassignment
        CALL [install_database].drop_vt_sp('vt_comm_sigma');
        SET SqlStr =
            'CREATE MULTISET VOLATILE TABLE vt_comm_sigma AS (
                 SELECT c.comm_id,
                        SUM(d.ki) AS sigma_tot
                 FROM   vt_community c
                 JOIN   vt_degree    d ON d.node = c.node
                 GROUP BY c.comm_id
             ) WITH DATA
             PRIMARY INDEX (comm_id)
             ON COMMIT PRESERVE ROWS;';
        EXECUTE IMMEDIATE SqlStr;

        CALL [install_database].drop_vt_sp('vt_best_comm');

    END WHILE;


    -- =========================================================================
    -- STEP 7  Compute final modularity  Q 
    -- =========================================================================
    SET SqlStr = '
    SELECT SUM(
               CAST(lc.internal_w AS FLOAT) / t.v_m
             - ( CAST(csg.sigma_tot AS FLOAT) / v_2m )
               * ( CAST(csg.sigma_tot AS FLOAT) / v_2m )
           )
    FROM (
        SELECT  c1.comm_id,
                SUM(CASE WHEN c2.comm_id = c1.comm_id
                         THEN e.weight ELSE 0 END) / 2.0  AS internal_w
        FROM    vt_community c1
        JOIN    vt_edges     e   ON  e.node_a  = c1.node
        JOIN    vt_community c2  ON  c2.node   = e.node_b
        GROUP BY c1.comm_id
    ) lc
    JOIN vt_comm_sigma csg 
    ON csg.comm_id = lc.comm_id
    JOIN (SELECT SUM(weight) / 2.0 AS v_m, 2 * v_m AS v_2m FROM vt_edges) t 
    ON 1=1;';
    PREPARE s1 FROM SqlStr;
    OPEN c1;
    FETCH c1 INTO p_modularity;
    CLOSE c1;


    -- =========================================================================
    -- STEP 8  Persist results to output
    -- =========================================================================

    SET SqlStr = '
            SELECT
            c.node,
            c.comm_id,
            pr.'||TRIM(in_pr_score_name)||',
            agg.comm_size,
            agg.representative_node,
            agg.intra_edge_weight
        FROM vt_community c
        -- PageRank scores
        LEFT JOIN '||TRIM(in_dbname)||'.'||TRIM(in_pr_tblname)||' pr
               ON CAST(pr.'||TRIM(in_pr_node_name)||' AS BIGINT) = c.node
        -- Community-level aggregates
        JOIN (
            SELECT
                t.comm_id,
                COUNT(*)                                                            AS comm_size,
                MAX(CASE WHEN t.pr_rank = 1 THEN t.node ELSE NULL END)             AS representative_node,
                SUM(t.internal_w) / 2.0                                            AS intra_edge_weight
            FROM (
                SELECT
                    c2.comm_id,
                    c2.node,
                    -- Highest-PageRank node per community
                    RANK() OVER (
                        PARTITION BY c2.comm_id
                        ORDER BY COALESCE(pr2.'||TRIM(in_pr_score_name)||', 0.0) DESC
                    )                                                               AS pr_rank,
                    -- Internal edge weight contribution per node
                    COALESCE((
                        SELECT SUM(e2.weight)
                        FROM   vt_edges      e2
                        JOIN   vt_community  c3 ON c3.node = e2.node_b
                        WHERE  e2.node_a   = c2.node
                          AND  c3.comm_id  = c2.comm_id
                    ), 0.0)                                                         AS internal_w
                FROM vt_community c2
                LEFT JOIN '||TRIM(in_dbname)||'.'||TRIM(in_pr_tblname)||' pr2
                       ON CAST(pr2.'||TRIM(in_pr_node_name)||' AS BIGINT) = c2.node
            ) t
            GROUP BY t.comm_id
        ) agg ON agg.comm_id = c.comm_id
    ) WITH DATA 
    PRIMARY INDEX (node)
    ';

    IF in_output_volatile >0 THEN
      CALL [install_database].drop_vt_sp(TRIM(in_output_tblname));
      SET SqlStr = 'CREATE MULTISET VOLATILE TABLE '||TRIM(in_output_tblname)||' AS (
      '||SqlStr||'
      ON COMMIT PRESERVE ROWS';
    ELSE
      CALL [install_database].drop_vt_sp(TRIM(in_dbname)||'.'||TRIM(in_output_tblname));
      SET SqlStr = 'CREATE MULTISET TABLE '||TRIM(in_dbname)||'.'||TRIM(in_output_tblname)||' AS (
      '||SqlStr;
    END IF;

    EXECUTE IMMEDIATE SqlStr;

    -- =========================================================================
    -- STEP 9  Set OUT parameters
    -- =========================================================================
    --SELECT COUNT(*)                INTO p_nodes       FROM vt_community;
    --SELECT COUNT(DISTINCT comm_id) INTO p_communities FROM vt_community;

    SET SqlStr = 'SELECT COUNT(0), COUNT(DISTINCT comm_id) FROM vt_community;';
    PREPARE s1 FROM SqlStr;
    OPEN c1;
    FETCH c1 INTO p_nodes, p_communities;
    CLOSE c1;

    SET p_iterations = v_iter;


    -- =========================================================================
    -- STEP 10  Cleanup volatile tables
    -- =========================================================================
    CALL [install_database].drop_vt_sp('vt_edges');
    CALL [install_database].drop_vt_sp('vt_degree');
    CALL [install_database].drop_vt_sp('vt_community');
    CALL [install_database].drop_vt_sp('vt_comm_sigma');

END;

