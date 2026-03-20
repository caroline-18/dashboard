-- ================================================================
-- ETL PIPELINE PROCEDURE v9
-- Run against: u333015459_EvolvuUsrsTest
-- Call: CALL run_etl_for_school('arnolds_live', 'arnolds1_analytics')
-- Fix: JSON keys are numeric strings, path must be $."1" not $.1
-- ================================================================


DROP PROCEDURE IF EXISTS run_etl_for_school;

DELIMITER $$

CREATE PROCEDURE run_etl_for_school(
    IN p_source_db    VARCHAR(100),
    IN p_analytics_db VARCHAR(100)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @err = MESSAGE_TEXT;
        --         UPDATE u333015459_EvolvuUsrsTest.etl_master_run_log
        --         SET status='ERROR', run_end=NOW(), error_msg=@err
        --         WHERE log_id=@log_id;
        RESIGNAL;
    END;

    -- --------------------------------------------------------
    -- LOG: start
    -- --------------------------------------------------------
    --     INSERT INTO u333015459_EvolvuUsrsTest.etl_master_run_log
    --         (school_id, school_name, source_db, analytics_db, run_start, status)
    --     SELECT s.school_id, s.name, p_source_db, p_analytics_db, NOW(), 'RUNNING'
    --     FROM u333015459_EvolvuUsrsTest.school s
    --     WHERE s.source_db = p_source_db LIMIT 1;
    --     SET @log_id = LAST_INSERT_ID();

    -- --------------------------------------------------------
    -- STAGE 0a: mark_headings_clean
    -- --------------------------------------------------------
    SET @sql = CONCAT('TRUNCATE TABLE `', p_analytics_db, '`.mark_headings_clean');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.mark_headings_clean ',
        '(marks_headings_id, component_name, written_exam, sequence) ',
        'SELECT marks_headings_id, name, written_exam, sequence ',
        'FROM `', p_source_db, '`.marks_headings'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 0b: student_marks_raw (incremental)
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_marks_raw ',
        '(marks_id,class_id,section_id,exam_id,subject_id,student_id,',
        'exam_date,present,mark_obtained,highest_marks,',
        'reportcard_marks,reportcard_highest_marks,',
        'total_marks,highest_total_marks,grade,percent,publish,academic_yr,rn) ',
        'SELECT sm.marks_id,sm.class_id,sm.section_id,sm.exam_id,',
        'sm.subject_id,sm.student_id,sm.date,',
        'sm.present,sm.mark_obtained,sm.highest_marks,',
        'sm.reportcard_marks,sm.reportcard_highest_marks,',
        'sm.total_marks,sm.highest_total_marks,sm.grade,sm.percent,sm.publish,sm.academic_yr,',
        'ROW_NUMBER() OVER (',
        '  PARTITION BY sm.student_id,sm.subject_id,sm.exam_id,sm.academic_yr ',
        '  ORDER BY sm.marks_id DESC) ',
        'FROM `', p_source_db, '`.student_marks sm ',
        'WHERE sm.marks_id NOT IN (',
        '  SELECT marks_id FROM `', p_analytics_db, '`.student_marks_raw)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 0c: student_marks_clean (deduped, rn=1 only)
    -- --------------------------------------------------------
    SET @sql = CONCAT('TRUNCATE TABLE `', p_analytics_db, '`.student_marks_clean');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_marks_clean ',
        'SELECT * FROM `', p_analytics_db, '`.student_marks_raw WHERE rn=1'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 0d: JSON unnest → student_marks_components
    -- JSON keys are numeric strings e.g. {"1":"12","2":"8"}
    -- Path must be $."1" not $.1 — MySQL requires quoted numeric keys
    -- --------------------------------------------------------
    SET @sql = CONCAT('TRUNCATE TABLE `', p_analytics_db, '`.student_marks_components');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @unnest_sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_marks_components ',
        '(student_id,subject_id,exam_id,academic_yr,marks_headings_id,',
        'component_name,written_exam,is_present,marks_obtained,max_marks) ',
        'SELECT ',
        '  mc.student_id,mc.subject_id,mc.exam_id,mc.academic_yr,',
        '  CAST(jt.hkey AS UNSIGNED),',
        '  mh.component_name,mh.written_exam,',
        '  JSON_UNQUOTE(JSON_EXTRACT(mc.present,',
        '    CONCAT(', CHAR(39), '$."', CHAR(39), ',jt.hkey,', CHAR(39), '"', CHAR(39), '))),',
        '  CAST(JSON_UNQUOTE(JSON_EXTRACT(mc.mark_obtained,',
        '    CONCAT(', CHAR(39), '$."', CHAR(39), ',jt.hkey,', CHAR(39), '"', CHAR(39), '))) AS DECIMAL(5,2)),',
        '  CAST(JSON_UNQUOTE(JSON_EXTRACT(mc.highest_marks,',
        '    CONCAT(', CHAR(39), '$."', CHAR(39), ',jt.hkey,', CHAR(39), '"', CHAR(39), '))) AS DECIMAL(5,2))',
        ' FROM `', p_analytics_db, '`.student_marks_clean mc',
        ' JOIN JSON_TABLE(',
        '   JSON_KEYS(mc.mark_obtained),',
        '   ', CHAR(39), '$[*]', CHAR(39),
        '   COLUMNS (hkey VARCHAR(10) PATH ', CHAR(39), '$', CHAR(39), ')',
        ' ) AS jt ON TRUE',
        ' LEFT JOIN `', p_analytics_db, '`.mark_headings_clean mh',
        '   ON mh.marks_headings_id=CAST(jt.hkey AS UNSIGNED)',
        ' WHERE mc.publish=', CHAR(39), 'Y', CHAR(39)
    );
    PREPARE s FROM @unnest_sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 0e: year range temp table
    -- --------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS etl_new_years;
    SET @sql = CONCAT(
        'CREATE TEMPORARY TABLE etl_new_years AS ',
        'SELECT DISTINCT academic_yr FROM `', p_source_db, '`.student ',
        'WHERE academic_yr BETWEEN ', CHAR(39), '2019-2020', CHAR(39),
        ' AND ', CHAR(39), '2025-2026', CHAR(39)
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 1: student_clean
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_clean ',
        '(student_id,academic_yr,reg_no,class_id,section_id) ',
        'SELECT s.student_id,s.academic_yr,s.reg_no,s.class_id,s.section_id ',
        'FROM `', p_source_db, '`.student s ',
        'WHERE s.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND s.isDelete=', CHAR(39), 'N', CHAR(39), ' ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.student_clean sc ',
        '  WHERE sc.student_id=s.student_id AND sc.academic_yr=s.academic_yr)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 2: dim_student_demographics
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_student_demographics ',
        '(student_id,academic_yr,student_name,gender,dob,class_id,section_id,',
        'nationality,category,parent_id,guardian_name,guardian_mobile) ',
        'SELECT s.student_id,s.academic_yr,TRIM(s.student_name),s.gender,',
        'CASE WHEN CAST(s.dob AS CHAR)=', CHAR(39), '0000-00-00', CHAR(39), ' THEN NULL ELSE s.dob END,',
        's.class_id,s.section_id,s.nationality,s.category,',
        's.parent_id,s.guardian_name,s.guardian_mobile ',
        'FROM `', p_source_db, '`.student s ',
        'WHERE s.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND s.isDelete=', CHAR(39), 'N', CHAR(39), ' ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.dim_student_demographics d ',
        '  WHERE d.student_id=s.student_id AND d.academic_yr=s.academic_yr)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 2b: dim_teachers (full refresh)
    -- --------------------------------------------------------
    SET @sql = CONCAT('TRUNCATE TABLE `', p_analytics_db, '`.dim_teachers');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_teachers ',
        '(teacher_id,name,designation,class_id,section_id) ',
        'SELECT t.teacher_id,t.name,t.designation,t.class_id,t.section_id ',
        'FROM `', p_source_db, '`.teacher t ',
        'WHERE t.isDelete=', CHAR(39), 'N', CHAR(39)
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 2c: dim_users (full refresh)
    -- --------------------------------------------------------
    SET @sql = CONCAT('TRUNCATE TABLE `', p_analytics_db, '`.dim_users');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_users ',
        '(user_id,name,password,reg_id,role_id) ',
        'SELECT u.user_id,u.name,u.password,u.reg_id,u.role_id ',
        'FROM `', p_source_db, '`.user_master u ',
        'WHERE u.role_id IN (',
        CHAR(39), 'P', CHAR(39), ',',
        CHAR(39), 'T', CHAR(39), ',',
        CHAR(39), 'M', CHAR(39), ') ',
        'AND u.IsDelete=', CHAR(39), 'N', CHAR(39)
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 3: dim_parent
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT IGNORE INTO `', p_analytics_db, '`.dim_parent ',
        '(parent_id,father_name,mother_name) ',
        'SELECT p.parent_id,p.father_name,p.mother_name ',
        'FROM `', p_source_db, '`.parent p ',
        'WHERE NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.dim_parent dp ',
        '  WHERE dp.parent_id=p.parent_id)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 4: fact_attendance
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.fact_attendance ',
        '(student_id,academic_yr,total_school_days,absent_days,',
        'present_days,attendance_percentage) ',
        'SELECT ac.student_id,ac.academic_yr,COUNT(*),',
        'COUNT(*)-SUM(ac.attendance_status=', CHAR(39), 'P', CHAR(39), '),',
        'SUM(ac.attendance_status=', CHAR(39), 'P', CHAR(39), '),',
        'ROUND(SUM(ac.attendance_status=', CHAR(39), 'P', CHAR(39), ')/COUNT(*)*100,2) ',
        'FROM `', p_analytics_db, '`.attendance_clean ac ',
        'WHERE ac.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.fact_attendance fa ',
        '  WHERE fa.student_id=ac.student_id AND fa.academic_yr=ac.academic_yr) ',
        'GROUP BY ac.student_id,ac.academic_yr'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 5: fact_academics
    -- CBSE weighted SUM/SUM averaging
    -- Written IDs:  1,5,15,18,19,20,25,28,33,34,36
    -- Internal IDs: 2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.fact_academics ',
        '(student_id,academic_yr,avg_percent,written_avg,oral_avg,exams_taken) ',
        'SELECT smc.student_id,smc.academic_yr,',
        'ROUND(SUM(smc.marks_obtained)/NULLIF(SUM(smc.max_marks),0)*100,2),',
        'ROUND(',
        '  SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)',
        '      THEN smc.marks_obtained END)',
        '  /NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)',
        '      THEN smc.max_marks END),0)*100,2),',
        'ROUND(',
        '  SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)',
        '      THEN smc.marks_obtained END)',
        '  /NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)',
        '      THEN smc.max_marks END),0)*100,2),',
        'COUNT(DISTINCT smc.exam_id) ',
        'FROM `', p_analytics_db, '`.student_marks_components smc ',
        'WHERE smc.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND smc.is_present=', CHAR(39), 'Y', CHAR(39), ' ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.fact_academics fa ',
        '  WHERE fa.student_id=smc.student_id AND fa.academic_yr=smc.academic_yr) ',
        'GROUP BY smc.student_id,smc.academic_yr'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 5b: dim_subject
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_subject (subject_id,subject_name,subject_type) ',
        'SELECT sm_id,name,subject_type FROM `', p_source_db, '`.subject_master ',
        'WHERE name NOT IN (',
        CHAR(39), 'New subject Regular',        CHAR(39), ',',
        CHAR(39), 'new subject common code test',CHAR(39), ',',
        CHAR(39), 'SUBJECT_TEST2',               CHAR(39), ',',
        CHAR(39), 'SubjectForRCTest',             CHAR(39), ',',
        CHAR(39), 'New RC subject',              CHAR(39), ',',
        CHAR(39), 'New sub for RC',              CHAR(39), ',',
        CHAR(39), 'test',                        CHAR(39), ') ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.dim_subject ds ',
        '  WHERE ds.subject_id=sm_id) ',
        'ORDER BY sm_id'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- Science combined subject (id=24) — always refresh
    SET @sql = CONCAT(
        'DELETE FROM `', p_analytics_db, '`.dim_subject WHERE subject_id=24'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_subject (subject_id,subject_name,subject_type) ',
        'VALUES (24,', CHAR(39), 'Science', CHAR(39), ',', CHAR(39), 'Scholastic', CHAR(39), ')'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 5c: dim_class_subject_map
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.dim_class_subject_map ',
        '(class_name,subject_id,academic_yr) ',
        'SELECT class_name,subject_id,academic_yr FROM (',
        '  SELECT DISTINCT c.name AS class_name,',
        '  CASE WHEN c.name IN (',
        CHAR(39),'6',CHAR(39),',',CHAR(39),'7',CHAR(39),',',
        CHAR(39),'8',CHAR(39),',',CHAR(39),'9',CHAR(39),',',
        CHAR(39),'10',CHAR(39),') AND sub.sm_id IN (15,16,17) THEN 24 ',
        '  ELSE sub.sm_id END AS subject_id,',
        '  sub.academic_yr ',
        '  FROM `', p_source_db, '`.subject sub ',
        '  JOIN `', p_source_db, '`.class c ',
        '    ON c.class_id=sub.class_id AND c.academic_yr=sub.academic_yr ',
        '  JOIN `', p_source_db, '`.subject_master sm_src ON sm_src.sm_id=sub.sm_id ',
        '  WHERE sub.academic_yr BETWEEN ',
        CHAR(39),'2019-2020',CHAR(39),' AND ',CHAR(39),'2025-2026',CHAR(39),' ',
        '  AND sm_src.name NOT IN (',
        CHAR(39),'New subject Regular',        CHAR(39),',',
        CHAR(39),'new subject common code test',CHAR(39),',',
        CHAR(39),'SUBJECT_TEST2',               CHAR(39),',',
        CHAR(39),'SubjectForRCTest',             CHAR(39),',',
        CHAR(39),'New RC subject',              CHAR(39),',',
        CHAR(39),'New sub for RC',              CHAR(39),',',
        CHAR(39),'test',                        CHAR(39),')) AS nr ',
        'ON DUPLICATE KEY UPDATE academic_yr=nr.academic_yr'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 6a: fact_science_components
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.fact_science_components ',
        '(student_id,academic_yr,component,avg_percent,is_entered) ',
        'SELECT student_id,academic_yr,component,avg_percent,is_entered FROM (',
        '  SELECT smc.student_id,smc.academic_yr,',
        '  CASE smc.subject_id ',
        '    WHEN 15 THEN ',CHAR(39),'Physics',  CHAR(39),' ',
        '    WHEN 16 THEN ',CHAR(39),'Chemistry',CHAR(39),' ',
        '    WHEN 17 THEN ',CHAR(39),'Biology',  CHAR(39),' ',
        '  END AS component,',
        '  ROUND(SUM(smc.marks_obtained)/NULLIF(SUM(smc.max_marks),0)*100,2) AS avg_percent,',
        '  1 AS is_entered ',
        '  FROM `', p_analytics_db, '`.student_marks_components smc ',
        '  JOIN `', p_source_db, '`.student st ',
        '    ON st.student_id=smc.student_id AND st.academic_yr=smc.academic_yr ',
        '  JOIN `', p_source_db, '`.class c ',
        '    ON c.class_id=st.class_id AND c.academic_yr=st.academic_yr ',
        '  JOIN `', p_source_db, '`.subject sub ',
        '    ON sub.sm_id=smc.subject_id AND sub.academic_yr=smc.academic_yr ',
        '    AND sub.class_id=st.class_id ',
        '  WHERE smc.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        '  AND smc.is_present=',CHAR(39),'Y',CHAR(39),' ',
        '  AND st.isDelete=',CHAR(39),'N',CHAR(39),' ',
        '  AND c.name IN (',
        CHAR(39),'6',CHAR(39),',',CHAR(39),'7',CHAR(39),',',
        CHAR(39),'8',CHAR(39),',',CHAR(39),'9',CHAR(39),',',
        CHAR(39),'10',CHAR(39),') ',
        '  AND smc.subject_id IN (15,16,17) ',
        '  GROUP BY smc.student_id,smc.academic_yr,smc.subject_id',
        ') AS nr ',
        'ON DUPLICATE KEY UPDATE avg_percent=nr.avg_percent,is_entered=nr.is_entered'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- Back-fill placeholder rows for missing science components
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.fact_science_components ',
        '(student_id,academic_yr,component,avg_percent,is_entered) ',
        'SELECT e.student_id,e.academic_yr,c.component,NULL,0 ',
        'FROM (',
        '  SELECT DISTINCT student_id,academic_yr ',
        '  FROM `', p_analytics_db, '`.fact_science_components WHERE is_entered=1) e ',
        'CROSS JOIN (',
        '  SELECT ',CHAR(39),'Physics',  CHAR(39),' AS component UNION ALL ',
        '  SELECT ',CHAR(39),'Chemistry',CHAR(39),' UNION ALL ',
        '  SELECT ',CHAR(39),'Biology',  CHAR(39),') c ',
        'WHERE NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.fact_science_components fsc ',
        '  WHERE fsc.student_id=e.student_id AND fsc.academic_yr=e.academic_yr ',
        '  AND fsc.component=c.component)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 6c: fact_student_subject_performance
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.fact_student_subject_performance ',
        '(student_id,academic_yr,subject_id,avg_percent,written_avg,oral_avg,exams_taken) ',
        'SELECT smc.student_id,smc.academic_yr,',
        'CASE WHEN c.name IN (',
        CHAR(39),'6',CHAR(39),',',CHAR(39),'7',CHAR(39),',',
        CHAR(39),'8',CHAR(39),',',CHAR(39),'9',CHAR(39),',',
        CHAR(39),'10',CHAR(39),') AND smc.subject_id IN (15,16,17) THEN 24 ',
        'ELSE smc.subject_id END,',
        'ROUND(SUM(smc.marks_obtained)/NULLIF(SUM(smc.max_marks),0)*100,2),',
        'ROUND(',
        '  SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)',
        '      THEN smc.marks_obtained END)',
        '  /NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)',
        '      THEN smc.max_marks END),0)*100,2),',
        'ROUND(',
        '  SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)',
        '      THEN smc.marks_obtained END)',
        '  /NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)',
        '      THEN smc.max_marks END),0)*100,2),',
        'COUNT(DISTINCT smc.exam_id) ',
        'FROM `', p_analytics_db, '`.student_marks_components smc ',
        'JOIN `', p_source_db, '`.student st ',
        '  ON st.student_id=smc.student_id AND st.academic_yr=smc.academic_yr ',
        'JOIN `', p_source_db, '`.class c ',
        '  ON c.class_id=st.class_id AND c.academic_yr=st.academic_yr ',
        'JOIN `', p_source_db, '`.subject sub ',
        '  ON sub.sm_id=smc.subject_id AND sub.academic_yr=smc.academic_yr ',
        '  AND sub.class_id=st.class_id ',
        'WHERE smc.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND smc.is_present=',CHAR(39),'Y',CHAR(39),' ',
        'AND st.isDelete=',CHAR(39),'N',CHAR(39),' ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.fact_student_subject_performance fsp ',
        '  WHERE fsp.student_id=smc.student_id AND fsp.academic_yr=smc.academic_yr ',
        '  AND fsp.subject_id=CASE WHEN c.name IN (',
        CHAR(39),'6',CHAR(39),',',CHAR(39),'7',CHAR(39),',',
        CHAR(39),'8',CHAR(39),',',CHAR(39),'9',CHAR(39),',',
        CHAR(39),'10',CHAR(39),') AND smc.subject_id IN (15,16,17) THEN 24 ',
        '  ELSE smc.subject_id END) ',
        'GROUP BY smc.student_id,smc.academic_yr,',
        'CASE WHEN c.name IN (',
        CHAR(39),'6',CHAR(39),',',CHAR(39),'7',CHAR(39),',',
        CHAR(39),'8',CHAR(39),',',CHAR(39),'9',CHAR(39),',',
        CHAR(39),'10',CHAR(39),') AND smc.subject_id IN (15,16,17) THEN 24 ',
        'ELSE smc.subject_id END'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 7: dim_class + dim_section
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT IGNORE INTO `', p_analytics_db, '`.dim_class (class_id,class_name) ',
        'SELECT DISTINCT c.class_id,c.name FROM `', p_source_db, '`.class c ',
        'WHERE c.class_id IN (',
        '  SELECT DISTINCT s.class_id FROM `', p_source_db, '`.student s ',
        '  WHERE s.academic_yr BETWEEN ',
        CHAR(39),'2019-2020',CHAR(39),' AND ',CHAR(39),'2025-2026',CHAR(39),' ',
        '  AND s.isDelete=',CHAR(39),'N',CHAR(39),')'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'INSERT IGNORE INTO `', p_analytics_db, '`.dim_section (section_id,section_name) ',
        'SELECT DISTINCT sec.section_id,sec.name FROM `', p_source_db, '`.section sec ',
        'WHERE sec.section_id IN (',
        '  SELECT DISTINCT s.section_id FROM `', p_source_db, '`.student s ',
        '  WHERE s.academic_yr BETWEEN ',
        CHAR(39),'2019-2020',CHAR(39),' AND ',CHAR(39),'2025-2026',CHAR(39),' ',
        '  AND s.isDelete=',CHAR(39),'N',CHAR(39),')'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 8: student_master_profile
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_master_profile ',
        '(student_id,academic_yr,reg_no,class_id,section_id,',
        'avg_percent,written_avg,oral_avg,exams_taken,',
        'attendance_percentage,achievements,homework_assigned_count) ',
        'SELECT s.student_id,s.academic_yr,s.reg_no,s.class_id,s.section_id,',
        'fa.avg_percent,fa.written_avg,fa.oral_avg,fa.exams_taken,',
        'fatt.attendance_percentage,',
        'COALESCE(fach.achievement_count,0),',
        'COALESCE(fhw.homework_assigned_count,0) ',
        'FROM `', p_analytics_db, '`.student_clean s ',
        'LEFT JOIN `', p_analytics_db, '`.fact_academics fa ',
        '  ON fa.student_id=s.student_id AND fa.academic_yr=s.academic_yr ',
        'LEFT JOIN `', p_analytics_db, '`.fact_attendance fatt ',
        '  ON fatt.student_id=s.student_id AND fatt.academic_yr=s.academic_yr ',
        'LEFT JOIN `', p_analytics_db, '`.fact_achievements fach ',
        '  ON fach.student_id=s.student_id AND fach.academic_yr=s.academic_yr ',
        'LEFT JOIN `', p_analytics_db, '`.fact_homework_engagement fhw ',
        '  ON fhw.student_id=s.student_id AND fhw.academic_yr=s.academic_yr ',
        'WHERE s.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.student_master_profile sm ',
        '  WHERE sm.student_id=s.student_id AND sm.academic_yr=s.academic_yr)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 9: student_behavior_signals
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'INSERT INTO `', p_analytics_db, '`.student_behavior_signals ',
        '(student_id,academic_yr,attendance_band,learning_style,',
        'engagement_pattern,primary_strength_axis) ',
        'SELECT sm.student_id,sm.academic_yr,',
        'CASE WHEN sm.attendance_percentage>=90 THEN ',CHAR(39),'Highly Consistent',CHAR(39),' ',
        '     WHEN sm.attendance_percentage>=75 THEN ',CHAR(39),'Moderately Consistent',CHAR(39),' ',
        '     ELSE ',CHAR(39),'Irregular',CHAR(39),' END,',
        'CASE WHEN sm.written_avg>sm.oral_avg THEN ',CHAR(39),'Conceptual / Written-Oriented',CHAR(39),' ',
        '     WHEN sm.oral_avg>sm.written_avg THEN ',CHAR(39),'Experiential / Oral-Oriented',CHAR(39),' ',
        '     ELSE ',CHAR(39),'Balanced',CHAR(39),' END,',
        'CASE WHEN sm.homework_assigned_count>0 THEN ',CHAR(39),'Academically Engaged',CHAR(39),' ',
        '     ELSE ',CHAR(39),'Low Visible Engagement',CHAR(39),' END,',
        'CASE WHEN sm.avg_percent>=85 THEN ',CHAR(39),'Academic Excellence',CHAR(39),' ',
        '     WHEN sm.achievements>0  THEN ',CHAR(39),'Co-Curricular Strength',CHAR(39),' ',
        '     ELSE ',CHAR(39),'Developing Potential',CHAR(39),' END ',
        'FROM `', p_analytics_db, '`.student_master_profile sm ',
        'WHERE sm.academic_yr IN (SELECT academic_yr FROM etl_new_years) ',
        'AND NOT EXISTS (',
        '  SELECT 1 FROM `', p_analytics_db, '`.student_behavior_signals bs ',
        '  WHERE bs.student_id=sm.student_id AND bs.academic_yr=sm.academic_yr)'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- STAGE 10: views
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'CREATE OR REPLACE VIEW `', p_analytics_db, '`.student_subject_strengths AS ',
        'SELECT f.student_id,f.academic_yr,',
        'GROUP_CONCAT(DISTINCT d.subject_name ORDER BY f.avg_percent DESC SEPARATOR ',
        CHAR(39),', ',CHAR(39),') AS strong_subjects,',
        'GROUP_CONCAT(DISTINCT ',
        '  CASE WHEN d.subject_name=',CHAR(39),'Science',CHAR(39),' THEN ',
        '    CONCAT(',CHAR(39),'Science (',CHAR(39),',COALESCE((',
        '      SELECT GROUP_CONCAT(',
        '        CASE WHEN sc.is_entered=0 THEN CONCAT(sc.component,',CHAR(39),' (not assessed)',CHAR(39),') ',
        '        ELSE sc.component END ',
        '        ORDER BY sc.is_entered DESC,sc.avg_percent DESC SEPARATOR ',CHAR(39),', ',CHAR(39),') ',
        '      FROM `', p_analytics_db, '`.fact_science_components sc ',
        '      WHERE sc.student_id=f.student_id AND sc.academic_yr=f.academic_yr),',
        CHAR(39),'combined',CHAR(39),'),',CHAR(39),')',CHAR(39),') ',
        '  ELSE d.subject_name END ',
        '  ORDER BY f.avg_percent DESC SEPARATOR ',CHAR(39),', ',CHAR(39),') AS strong_subjects_ai ',
        'FROM `', p_analytics_db, '`.fact_student_subject_performance f ',
        'JOIN `', p_analytics_db, '`.dim_subject d ON d.subject_id=f.subject_id ',
        'WHERE f.avg_percent>=75 ',
        'GROUP BY f.student_id,f.academic_yr'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'CREATE OR REPLACE VIEW `', p_analytics_db, '`.student_ai_profile AS ',
        'SELECT d.student_id,',
        'CASE WHEN TRIM(d.student_name) LIKE ',CHAR(39),'% %',CHAR(39),' THEN TRIM(d.student_name) ',
        '     WHEN p.father_name IS NOT NULL AND TRIM(p.father_name)!=',CHAR(39),CHAR(39),' ',
        '          THEN CONCAT(TRIM(d.student_name),',CHAR(39),' ',CHAR(39),',',
        '               SUBSTRING_INDEX(TRIM(p.father_name),',CHAR(39),' ',CHAR(39),',1)) ',
        '     ELSE TRIM(d.student_name) END AS student_name,',
        'd.gender,d.dob,d.academic_yr,d.parent_id,',
        'dc.class_name,ds.section_name,p.father_name,p.mother_name,',
        'sm.avg_percent,sm.written_avg,sm.oral_avg,sm.exams_taken,',
        'sm.attendance_percentage,sm.achievements,sm.homework_assigned_count,',
        'ss.strong_subjects,ss.strong_subjects_ai,',
        'bs.attendance_band,bs.learning_style,bs.engagement_pattern,bs.primary_strength_axis ',
        'FROM `', p_analytics_db, '`.dim_student_demographics d ',
        'LEFT JOIN `', p_analytics_db, '`.dim_class dc ON dc.class_id=d.class_id ',
        'LEFT JOIN `', p_analytics_db, '`.dim_section ds ON ds.section_id=d.section_id ',
        'LEFT JOIN `', p_analytics_db, '`.dim_parent p ON p.parent_id=d.parent_id ',
        'LEFT JOIN `', p_analytics_db, '`.student_master_profile sm ',
        '  ON sm.student_id=d.student_id AND sm.academic_yr=d.academic_yr ',
        'LEFT JOIN `', p_analytics_db, '`.student_subject_strengths ss ',
        '  ON ss.student_id=d.student_id AND ss.academic_yr=d.academic_yr ',
        'LEFT JOIN `', p_analytics_db, '`.student_behavior_signals bs ',
        '  ON bs.student_id=d.student_id AND bs.academic_yr=d.academic_yr'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'CREATE OR REPLACE VIEW `', p_analytics_db, '`.child_selector AS ',
        'SELECT parent_id,display_name,latest_student_id,latest_academic_yr FROM (',
        '  SELECT d.parent_id,',
        '  CASE WHEN TRIM(d.student_name) LIKE ',CHAR(39),'% %',CHAR(39),' THEN TRIM(d.student_name) ',
        '       WHEN p.father_name IS NOT NULL AND TRIM(p.father_name)!=',CHAR(39),CHAR(39),' ',
        '            THEN CONCAT(TRIM(d.student_name),',CHAR(39),' ',CHAR(39),',',
        '                 SUBSTRING_INDEX(TRIM(p.father_name),',CHAR(39),' ',CHAR(39),',1)) ',
        '       ELSE TRIM(d.student_name) END AS display_name,',
        '  d.student_id AS latest_student_id,',
        '  d.academic_yr AS latest_academic_yr,',
        '  ROW_NUMBER() OVER (',
        '    PARTITION BY d.parent_id,',
        '    COALESCE(na.canonical,SUBSTRING_INDEX(TRIM(d.student_name),',CHAR(39),' ',CHAR(39),',1)) ',
        '    ORDER BY d.academic_yr DESC) AS rn ',
        '  FROM `', p_analytics_db, '`.dim_student_demographics d ',
        '  LEFT JOIN `', p_analytics_db, '`.dim_parent p ON p.parent_id=d.parent_id ',
        '  LEFT JOIN `', p_analytics_db, '`.name_aliases na ',
        '    ON na.parent_id=d.parent_id ',
        '    AND na.raw_name=SUBSTRING_INDEX(TRIM(d.student_name),',CHAR(39),' ',CHAR(39),',1)',
        ') ranked WHERE rn=1'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    -- --------------------------------------------------------
    -- Final counts + LOG: success
    -- --------------------------------------------------------
    SET @sql = CONCAT(
        'SELECT COUNT(*) INTO @rc_raw FROM `', p_analytics_db, '`.student_marks_raw'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'SELECT COUNT(*) INTO @rc_components FROM `', p_analytics_db, '`.student_marks_components'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    SET @sql = CONCAT(
        'SELECT COUNT(*) INTO @rc_facts FROM `', p_analytics_db, '`.fact_student_subject_performance'
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    -- 
    --     UPDATE u333015459_EvolvuUsrsTest.etl_master_run_log
    --     SET status='SUCCESS', run_end=NOW(),
    --         rows_raw=@rc_raw,
    --         rows_components=@rc_components,
    --     --         rows_facts=@rc_facts
    --     WHERE log_id=@log_id;

    DROP TEMPORARY TABLE IF EXISTS etl_new_years;

END$$

DELIMITER ;
