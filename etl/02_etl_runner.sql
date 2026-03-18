-- ================================================================
-- ETL RUNNER — calls run_etl_for_school() for every active school
-- Run this file to ETL ALL schools in one shot.
-- ================================================================

USE u333015459_EvolvuUsrsTest;

DROP PROCEDURE IF EXISTS run_all_schools_etl;

DELIMITER $$

CREATE PROCEDURE run_all_schools_etl()
BEGIN
    DECLARE done        INT DEFAULT 0;
    DECLARE v_src       VARCHAR(100);
    DECLARE v_analytics VARCHAR(100);
    DECLARE v_name      VARCHAR(255);

    DECLARE school_cur CURSOR FOR
        SELECT source_db, analytics_db, name
        FROM u333015459_EvolvuUsrsTest.school
        WHERE is_active = 1
        ORDER BY school_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN school_cur;

    school_loop: LOOP
        FETCH school_cur INTO v_src, v_analytics, v_name;
        IF done THEN LEAVE school_loop; END IF;

        -- Log which school is starting (visible in process list)
        SET @progress = CONCAT('ETL starting: ', v_name,
            ' (', v_src, ' → ', v_analytics, ')');
        SELECT @progress AS etl_progress;

        CALL run_etl_for_school(v_src, v_analytics);

        SET @progress = CONCAT('ETL complete: ', v_name);
        SELECT @progress AS etl_progress;

    END LOOP;

    CLOSE school_cur;

    -- Final summary
    SELECT
        l.school_id,
        l.school_name,
        l.source_db,
        l.analytics_db,
        l.status,
        TIMESTAMPDIFF(SECOND, l.run_start, l.run_end) AS duration_sec,
        l.rows_raw,
        l.rows_components,
        l.rows_facts,
        l.error_msg
    FROM u333015459_EvolvuUsrsTest.etl_master_run_log l
    WHERE DATE(l.run_start) = CURDATE()
    ORDER BY l.run_start DESC;

END$$

DELIMITER ;