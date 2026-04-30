REPLACE PROCEDURE find_all_shortest_paths ()
SQL SECURITY CREATOR
BEGIN
    /* ── working variables ─────────────────────────────────── */
    DECLARE v_src_id   INTEGER;
    DECLARE v_trg_id   INTEGER;
    DECLARE v_req_id   INTEGER;
    DECLARE v_found    BYTEINT DEFAULT 0;
    DECLARE v_level    INTEGER DEFAULT 0;
    DECLARE v_count    INTEGER DEFAULT 0;

    /* ── BFS volatile tables ────────────────────────────────── */

    -- Current BFS frontier: one row per node being expanded
    CREATE VOLATILE TABLE vt_frontier
    (
        node_id  INTEGER         NOT NULL,
        path     VARCHAR(32000)  CHARACTER SET LATIN NOT NULL
    ) PRIMARY INDEX (node_id)
    ON COMMIT PRESERVE ROWS;

    -- Nodes already visited (prevents cycles / re-expansion)
    CREATE VOLATILE TABLE vt_visited
    (
        node_id  INTEGER NOT NULL
    ) PRIMARY INDEX (node_id)
    ON COMMIT PRESERVE ROWS;

    -- Next BFS level candidates
    CREATE VOLATILE TABLE vt_next
    (
        node_id  INTEGER         NOT NULL,
        path     VARCHAR(32000)  CHARACTER SET LATIN NOT NULL
    ) PRIMARY INDEX (node_id)
    ON COMMIT PRESERVE ROWS;

    /* ── iterate over every (src, trg) request ──────────────── */
    FOR req AS req_cur CURSOR FOR
        SELECT request_id, src_id, trg_id
        FROM   path_requests
        ORDER  BY request_id
    DO
        SET v_req_id = req.request_id;
        SET v_src_id = req.src_id;
        SET v_trg_id = req.trg_id;
        SET v_found  = 0;
        SET v_level  = 0;

        DELETE FROM vt_frontier ALL;
        DELETE FROM vt_visited  ALL;
        DELETE FROM vt_next     ALL;

        /* trivial case: source == target */
        IF v_src_id = v_trg_id THEN
            INSERT INTO shortest_path_results
            VALUES (v_req_id, v_src_id, v_trg_id,
                    TRIM(CAST(v_src_id AS VARCHAR(20))), 0);
            SET v_found = 1;
        ELSE
            /* seed BFS with the source node */
            INSERT INTO vt_frontier VALUES (v_src_id, TRIM(CAST(v_src_id AS VARCHAR(20))));
            INSERT INTO vt_visited  VALUES (v_src_id);
        END IF;

        /* ── BFS loop ─────────────────────────────────────────── */
        bfs: WHILE v_found = 0 DO

            SELECT COUNT(*) INTO v_count FROM vt_frontier;

            /* empty frontier → no path exists */
            IF v_count = 0 THEN
                INSERT INTO shortest_path_results
                VALUES (v_req_id, v_src_id, v_trg_id, 'NO PATH FOUND', -1);
                LEAVE bfs;
            END IF;

            SET v_level = v_level + 1;
            DELETE FROM vt_next ALL;

            /*
             * Expand every frontier node one hop.
             * GROUP BY node_id + MIN(path) keeps exactly one
             * (lexicographically smallest) path per newly reached node,
             * which is sufficient for a single shortest-path answer.
             * Remove the GROUP BY / MIN if you want ALL shortest paths
             * to the same node at this level.
             */
            INSERT INTO vt_next
            SELECT   e.to_id,
                     MIN(f.path || ' -> ' || TRIM(CAST(e.to_id AS VARCHAR(20))))
            FROM     vt_frontier  f
            JOIN     graph_edges  e  ON  f.node_id = e.from_id
            LEFT JOIN vt_visited  v  ON  e.to_id   = v.node_id
            WHERE    v.node_id IS NULL       -- skip already-visited nodes
            GROUP BY e.to_id;

            /* did we reach the target at this BFS level? */
            SELECT COUNT(*) INTO v_count
            FROM   vt_next
            WHERE  node_id = v_trg_id;

            IF v_count > 0 THEN
                INSERT INTO shortest_path_results
                SELECT v_req_id, v_src_id, v_trg_id, path, v_level
                FROM   vt_next
                WHERE  node_id = v_trg_id;
                SET v_found = 1;
                LEAVE bfs;
            END IF;

            /* mark new nodes visited and advance the frontier */
            INSERT INTO vt_visited SELECT node_id FROM vt_next;

            DELETE FROM vt_frontier ALL;
            INSERT INTO vt_frontier SELECT node_id, path FROM vt_next;

        END WHILE bfs;

    END FOR;

    /* cleanup */
    DROP TABLE vt_frontier;
    DROP TABLE vt_visited;
    DROP TABLE vt_next;

END;