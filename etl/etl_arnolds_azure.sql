/* ============================================================
   ETL SCRIPT: student_analytics  (UNIFIED — ETL + FIXES)
   SOURCE DB  : arnolds1
   TARGET DB  : arnolds_db
   MODE       : Full Backfill + Incremental + Idempotent
   YEARS      : 2019-2020 through 2025-2026
   PURPOSE    : AI-ready student analytics — complete data extract
   RUN ORDER  : Execute top to bottom in a single session.

   CHANGE LOG :
   v2 — written_avg / oral_avg use explicit marks_headings_id
        lists instead of the written_exam Y/N flag.
   v3 — Fixed MySQL 8 deprecation warnings.
   v4 — Eliminated final 2 deprecation warnings.
   v5 — CBSE-correct weighted averaging (SUM/SUM not AVG).
   v6 — Added Step 2b: dim_teachers
        · Copies teaching staff from u333015459_arnolds.teachers into
          arnolds_db.dim_teachers so the login page
          can authenticate teachers without a direct arnolds1
          connection or separate DB credentials.

   CBSE MARKS STRUCTURE (St. Arnolds Central School, Pune):
        Internal Assessment : 20 marks
        Yearly / Term Exam  : 80 marks
        Total               : 100 marks

   WRITTEN  IDs : 1,5,15,18,19,20,25,28,33,34,36
   INTERNAL IDs : 2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41
   ============================================================ */


USE arnolds_db;


/* ============================================================
   STEP 0: DEFINE TARGET YEAR RANGE
   ============================================================ */

DROP TEMPORARY TABLE IF EXISTS new_years;

CREATE TEMPORARY TABLE new_years AS
SELECT DISTINCT academic_yr
FROM u333015459_arnolds.student
WHERE academic_yr BETWEEN '2019-2020' AND '2025-2026';


/* ============================================================
   STEP 1: STUDENT CLEAN
   ============================================================ */

INSERT INTO student_clean
  (student_id, academic_yr, reg_no, class_id, section_id)
SELECT
  s.student_id, s.academic_yr, s.reg_no, s.class_id, s.section_id
FROM u333015459_arnolds.student s
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND s.IsDelete = 'N'
  AND NOT EXISTS (
    SELECT 1 FROM student_clean sc
    WHERE sc.student_id = s.student_id AND sc.academic_yr = s.academic_yr
  );


/* ============================================================
   STEP 2: DIM — STUDENT DEMOGRAPHICS
   ============================================================ */

INSERT INTO dim_student_demographics
  (student_id, academic_yr, student_name, gender, dob,
   class_id, section_id, nationality, category,
   parent_id, guardian_name, guardian_mobile)
SELECT
  s.student_id, s.academic_yr, TRIM(s.student_name), s.gender,
  CASE WHEN CAST(s.dob AS CHAR) = '0000-00-00' THEN NULL ELSE s.dob END,
  s.class_id, s.section_id, s.nationality, s.category,
  s.parent_id, s.guardian_name, s.guardian_mobile
FROM u333015459_arnolds.student s
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND s.IsDelete = 'N'
  AND NOT EXISTS (
    SELECT 1 FROM dim_student_demographics d
    WHERE d.student_id = s.student_id AND d.academic_yr = s.academic_yr
  );


/* ============================================================
   STEP 2b: DIM — TEACHERS
   Copies all active staff from u333015459_arnolds.teachers into
   arnolds_db.dim_teachers.
   Full refresh on every ETL run — table is small and staff
   changes (new hires, departures) must be reflected immediately.
   The login page reads from here instead of connecting
   directly to u333015459_arnolds.
   ============================================================ */

CREATE TABLE IF NOT EXISTS arnolds_db.dim_teachers (
    teacher_id   INT          NOT NULL,
    name         VARCHAR(100) NOT NULL,
    designation  VARCHAR(100),
    class_id     INT,
    section_id   INT,
    PRIMARY KEY  (teacher_id)
);

TRUNCATE TABLE arnolds_db.dim_teachers;

INSERT INTO arnolds_db.dim_teachers
    (teacher_id, name, designation, class_id, section_id)
SELECT
    teacher_id,
    name,
    designation,
    class_id,
    section_id
FROM u333015459_arnolds.teacher
WHERE isDelete = 'N';


/* ============================================================
   STEP 3: DIM — PARENT
   ============================================================ */

INSERT INTO dim_parent (parent_id, father_name, mother_name)
SELECT DISTINCT p.parent_id, p.father_name, p.mother_name
FROM u333015459_arnolds.parent p
WHERE NOT EXISTS (
  SELECT 1 FROM dim_parent dp WHERE dp.parent_id = p.parent_id
);


/* ============================================================
   STEP 4: FACT — ATTENDANCE
   ============================================================ */

INSERT INTO fact_attendance
  (student_id, academic_yr, total_school_days,
   absent_days, present_days, attendance_percentage)
SELECT
  ac.student_id, ac.academic_yr,
  COUNT(*)                                                     AS total_school_days,
  COUNT(*) - SUM(ac.attendance_status = 'P')                  AS absent_days,
  SUM(ac.attendance_status = 'P')                             AS present_days,
  ROUND(SUM(ac.attendance_status = 'P') / COUNT(*) * 100, 2) AS attendance_percentage
FROM attendance_clean ac
WHERE ac.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1 FROM fact_attendance fa
    WHERE fa.student_id = ac.student_id AND fa.academic_yr = ac.academic_yr
  )
GROUP BY ac.student_id, ac.academic_yr;


/* ============================================================
   STEP 5: FACT — ACADEMICS (overall per student per year)
   SUM/SUM weighted averaging — CBSE correct.
   ============================================================ */

INSERT INTO fact_academics
  (student_id, academic_yr, avg_percent, written_avg, oral_avg, exams_taken)
SELECT
  smc.student_id, smc.academic_yr,
  ROUND(SUM(smc.marks_obtained) / NULLIF(SUM(smc.max_marks), 0) * 100, 2) AS avg_percent,
  ROUND(
    SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)
          THEN smc.marks_obtained END)
    / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36)
                   THEN smc.max_marks END), 0) * 100
  , 2) AS written_avg,
  ROUND(
    SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)
          THEN smc.marks_obtained END)
    / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41)
                   THEN smc.max_marks END), 0) * 100
  , 2) AS oral_avg,
  COUNT(DISTINCT smc.exam_id) AS exams_taken
FROM u333015459_arnolds.student_marks_components smc
WHERE smc.academic_yr IN (SELECT academic_yr FROM new_years)
  AND smc.is_present = 'Y'
  AND NOT EXISTS (
    SELECT 1 FROM fact_academics fa
    WHERE fa.student_id = smc.student_id AND fa.academic_yr = smc.academic_yr
  )
GROUP BY smc.student_id, smc.academic_yr;


/* ============================================================
   STEP 5b: DIM — SUBJECT
   ============================================================ */

INSERT INTO arnolds_db.dim_subject (subject_id, subject_name, subject_type)
SELECT sm_id, name, subject_type
FROM u333015459_arnolds.subject_master
WHERE name NOT IN (
    'New subject Regular','new subject common code test',
    'SUBJECT_TEST2','SubjectForRCTest',
    'New RC subject','New sub for RC','test'
)
  AND NOT EXISTS (
    SELECT 1 FROM arnolds_db.dim_subject ds WHERE ds.subject_id = sm_id
  )
ORDER BY sm_id;

-- Science (combined) upsert for Classes 6-10
INSERT INTO arnolds_db.dim_subject (subject_id, subject_name, subject_type)
VALUES (24, 'Science', 'Scholastic') AS new_row
ON DUPLICATE KEY UPDATE
  subject_name = new_row.subject_name,
  subject_type = new_row.subject_type;

DELETE FROM arnolds_db.dim_subject WHERE subject_id = 24;
INSERT INTO arnolds_db.dim_subject (subject_id, subject_name, subject_type)
VALUES (24, 'Science', 'Scholastic');

SET @_idx = (
    SELECT COUNT(1) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = 'arnolds_db'
      AND TABLE_NAME   = 'dim_subject'
      AND INDEX_NAME   = 'uq_dim_subject_id'
);
SET @_sql = IF(@_idx > 0,
    'DROP INDEX uq_dim_subject_id ON arnolds_db.dim_subject',
    'SELECT 1'
);
PREPARE _s FROM @_sql; EXECUTE _s; DEALLOCATE PREPARE _s;
ALTER TABLE arnolds_db.dim_subject ADD UNIQUE INDEX uq_dim_subject_id (subject_id);


/* ============================================================
   STEP 5c: DIM — CLASS SUBJECT MAP
   ============================================================ */

CREATE TABLE IF NOT EXISTS arnolds_db.dim_class_subject_map (
    class_name   VARCHAR(20)  NOT NULL,
    subject_id   INT          NOT NULL,
    academic_yr  VARCHAR(11)  NOT NULL,
    PRIMARY KEY (class_name, subject_id, academic_yr)
);

INSERT INTO arnolds_db.dim_class_subject_map (class_name, subject_id, academic_yr)
SELECT class_name, subject_id, academic_yr
FROM (
    SELECT DISTINCT
        c.name AS class_name,
        CASE WHEN c.name IN ('6','7','8','9','10') AND sub.sm_id IN (15,16,17) THEN 24
             ELSE sub.sm_id END AS subject_id,
        sub.academic_yr
    FROM u333015459_arnolds.subject sub
    JOIN u333015459_arnolds.class c ON c.class_id = sub.class_id AND c.academic_yr = sub.academic_yr
    JOIN u333015459_arnolds.subject_master sm_src ON sm_src.sm_id = sub.sm_id
    WHERE sub.academic_yr BETWEEN '2019-2020' AND '2025-2026'
      AND sm_src.name NOT IN (
          'New subject Regular','new subject common code test',
          'SUBJECT_TEST2','SubjectForRCTest','New RC subject','New sub for RC','test'
      )
) AS new_row
ON DUPLICATE KEY UPDATE academic_yr = new_row.academic_yr;


/* ============================================================
   STEP 6: FACT — SUBJECT-LEVEL PERFORMANCE
   ============================================================ */

CREATE TABLE IF NOT EXISTS fact_science_components (
    student_id   INT          NOT NULL,
    academic_yr  VARCHAR(11)  NOT NULL,
    component    VARCHAR(30)  NOT NULL,
    avg_percent  DECIMAL(5,2),
    is_entered   TINYINT      NOT NULL DEFAULT 1
        COMMENT '1 = marks recorded; 0 = never entered by school',
    PRIMARY KEY (student_id, academic_yr, component)
);

SET @_col = (
    SELECT COUNT(1) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = 'arnolds_db'
      AND TABLE_NAME   = 'fact_science_components'
      AND COLUMN_NAME  = 'is_entered'
);
SET @_add_col = IF(@_col > 0, 'SELECT 1',
    'ALTER TABLE fact_science_components ADD COLUMN is_entered TINYINT NOT NULL DEFAULT 1');
PREPARE _s FROM @_add_col; EXECUTE _s; DEALLOCATE PREPARE _s;


-- STEP 6a: Real science component averages
INSERT INTO fact_science_components (student_id, academic_yr, component, avg_percent, is_entered)
SELECT student_id, academic_yr, component, avg_percent, is_entered
FROM (
    SELECT
        smc.student_id, smc.academic_yr,
        CASE smc.subject_id WHEN 15 THEN 'Physics' WHEN 16 THEN 'Chemistry' WHEN 17 THEN 'Biology' END AS component,
        ROUND(SUM(smc.marks_obtained) / NULLIF(SUM(smc.max_marks), 0) * 100, 2) AS avg_percent,
        1 AS is_entered
    FROM u333015459_arnolds.student_marks_components smc
    JOIN u333015459_arnolds.student st ON st.student_id = smc.student_id AND st.academic_yr = smc.academic_yr
    JOIN u333015459_arnolds.class c ON c.class_id = st.class_id AND c.academic_yr = st.academic_yr
    JOIN u333015459_arnolds.subject sub ON sub.sm_id = smc.subject_id AND sub.academic_yr = smc.academic_yr AND sub.class_id = st.class_id
    WHERE smc.academic_yr IN (SELECT academic_yr FROM new_years)
      AND smc.is_present = 'Y' AND st.IsDelete = 'N'
      AND c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17)
    GROUP BY smc.student_id, smc.academic_yr, smc.subject_id
) AS new_row
ON DUPLICATE KEY UPDATE avg_percent = new_row.avg_percent, is_entered = new_row.is_entered;


-- STEP 6b: Back-fill placeholder rows for components never entered
INSERT INTO fact_science_components (student_id, academic_yr, component, avg_percent, is_entered)
SELECT existing.student_id, existing.academic_yr, comp.component, NULL, 0
FROM (SELECT DISTINCT student_id, academic_yr FROM fact_science_components WHERE is_entered = 1) existing
CROSS JOIN (SELECT 'Physics' AS component UNION ALL SELECT 'Chemistry' UNION ALL SELECT 'Biology') comp
WHERE NOT EXISTS (
    SELECT 1 FROM fact_science_components fsc
    WHERE fsc.student_id = existing.student_id
      AND fsc.academic_yr = existing.academic_yr
      AND fsc.component = comp.component
);


-- STEP 6c: Combined subject-level performance
INSERT INTO fact_student_subject_performance
  (student_id, academic_yr, subject_id, avg_percent, written_avg, oral_avg, exams_taken)
SELECT
  smc.student_id, smc.academic_yr,
  CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24
       ELSE smc.subject_id END AS subject_id,
  ROUND(SUM(smc.marks_obtained) / NULLIF(SUM(smc.max_marks), 0) * 100, 2) AS avg_percent,
  ROUND(
    SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36) THEN smc.marks_obtained END)
    / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36) THEN smc.max_marks END), 0) * 100
  , 2) AS written_avg,
  ROUND(
    SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41) THEN smc.marks_obtained END)
    / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41) THEN smc.max_marks END), 0) * 100
  , 2) AS oral_avg,
  COUNT(DISTINCT smc.exam_id) AS exams_taken
FROM u333015459_arnolds.student_marks_components smc
JOIN u333015459_arnolds.student st ON st.student_id = smc.student_id AND st.academic_yr = smc.academic_yr
JOIN u333015459_arnolds.class c ON c.class_id = st.class_id AND c.academic_yr = st.academic_yr
JOIN u333015459_arnolds.subject sub ON sub.sm_id = smc.subject_id AND sub.academic_yr = smc.academic_yr AND sub.class_id = st.class_id
WHERE smc.academic_yr IN (SELECT academic_yr FROM new_years)
  AND smc.is_present = 'Y' AND st.IsDelete = 'N'
  AND NOT EXISTS (
    SELECT 1 FROM fact_student_subject_performance fsp
    WHERE fsp.student_id = smc.student_id AND fsp.academic_yr = smc.academic_yr
      AND fsp.subject_id = CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24 ELSE smc.subject_id END
  )
GROUP BY smc.student_id, smc.academic_yr,
  CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24 ELSE smc.subject_id END;


/* ============================================================
   STEP 7: VIEW — SUBJECT STRENGTHS
   ============================================================ */

CREATE OR REPLACE VIEW student_subject_strengths AS
SELECT
  f.student_id, f.academic_yr,
  GROUP_CONCAT(DISTINCT d.subject_name ORDER BY f.avg_percent DESC SEPARATOR ', ') AS strong_subjects,
  GROUP_CONCAT(DISTINCT
    CASE WHEN d.subject_name = 'Science' THEN
      CONCAT('Science (',
        COALESCE((
          SELECT GROUP_CONCAT(
            CASE WHEN sc.is_entered = 0 THEN CONCAT(sc.component, ' (not assessed)') ELSE sc.component END
            ORDER BY sc.is_entered DESC, sc.avg_percent DESC SEPARATOR ', ')
          FROM fact_science_components sc
          WHERE sc.student_id = f.student_id AND sc.academic_yr = f.academic_yr
        ), 'combined'), ')')
    ELSE d.subject_name END
    ORDER BY f.avg_percent DESC SEPARATOR ', '
  ) AS strong_subjects_ai
FROM fact_student_subject_performance f
JOIN dim_subject d ON d.subject_id = f.subject_id
WHERE f.avg_percent >= 75
GROUP BY f.student_id, f.academic_yr;


/* ============================================================
   STEP 8: FACT — ACHIEVEMENTS
   ============================================================ */

INSERT INTO fact_achievements (student_id, academic_yr, achievement_count, achievement_list)
SELECT a.student_id, a.academic_yr, COUNT(*),
  GROUP_CONCAT(COALESCE(a.achievement, a.event) ORDER BY a.date ASC SEPARATOR ', ')
FROM u333015459_arnolds.achievements_clean a
WHERE a.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1 FROM fact_achievements fa
    WHERE fa.student_id = a.student_id AND fa.academic_yr = a.academic_yr
  )
GROUP BY a.student_id, a.academic_yr;

-- Lifetime rollup (academic_yr = 'ALL')
INSERT INTO fact_achievements (student_id, academic_yr, achievement_count, achievement_list)
SELECT student_id, academic_yr, achievement_count, achievement_list
FROM (
    SELECT student_id, 'ALL' AS academic_yr,
           SUM(achievement_count) AS achievement_count,
           GROUP_CONCAT(achievement_list ORDER BY academic_yr ASC SEPARATOR ', ') AS achievement_list
    FROM fact_achievements WHERE academic_yr != 'ALL'
    GROUP BY student_id
) AS new_row
ON DUPLICATE KEY UPDATE
  achievement_count = new_row.achievement_count,
  achievement_list  = new_row.achievement_list;


/* ============================================================
   STEP 9: FACT — HOMEWORK ENGAGEMENT
   ============================================================ */

INSERT INTO fact_homework_engagement (student_id, academic_yr, homework_assigned_count)
SELECT hc.student_id, h.academic_yr, COUNT(DISTINCT h.homework_id)
FROM u333015459_arnolds.homework h
JOIN u333015459_arnolds.homework_comments hc ON h.homework_id = hc.homework_id
WHERE h.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1 FROM fact_homework_engagement fh
    WHERE fh.student_id = hc.student_id AND fh.academic_yr = h.academic_yr
  )
GROUP BY hc.student_id, h.academic_yr;


/* ============================================================
   STEP 10: MASTER PROFILE
   ============================================================ */

INSERT INTO student_master_profile
  (student_id, academic_yr, reg_no, class_id, section_id,
   avg_percent, written_avg, oral_avg, exams_taken,
   attendance_percentage, achievements, homework_assigned_count)
SELECT
  s.student_id, s.academic_yr, s.reg_no, s.class_id, s.section_id,
  fa.avg_percent, fa.written_avg, fa.oral_avg, fa.exams_taken,
  fatt.attendance_percentage,
  COALESCE(fach.achievement_count,      0),
  COALESCE(fhw.homework_assigned_count, 0)
FROM student_clean s
LEFT JOIN fact_academics           fa   ON fa.student_id   = s.student_id AND fa.academic_yr   = s.academic_yr
LEFT JOIN fact_attendance          fatt ON fatt.student_id = s.student_id AND fatt.academic_yr = s.academic_yr
LEFT JOIN fact_achievements        fach ON fach.student_id = s.student_id AND fach.academic_yr = s.academic_yr
LEFT JOIN fact_homework_engagement fhw  ON fhw.student_id  = s.student_id AND fhw.academic_yr  = s.academic_yr
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1 FROM student_master_profile sm
    WHERE sm.student_id = s.student_id AND sm.academic_yr = s.academic_yr
  );


/* ============================================================
   STEP 11: DERIVED — STUDENT BEHAVIOR SIGNALS
   ============================================================ */

INSERT INTO student_behavior_signals
  (student_id, academic_yr, attendance_band,
   learning_style, engagement_pattern, primary_strength_axis)
SELECT
  sm.student_id, sm.academic_yr,
  CASE WHEN sm.attendance_percentage >= 90 THEN 'Highly Consistent'
       WHEN sm.attendance_percentage >= 75 THEN 'Moderately Consistent'
       ELSE 'Irregular' END,
  CASE WHEN sm.written_avg > sm.oral_avg THEN 'Conceptual / Written-Oriented'
       WHEN sm.oral_avg > sm.written_avg THEN 'Experiential / Oral-Oriented'
       ELSE 'Balanced' END,
  CASE WHEN sm.homework_assigned_count > 0 THEN 'Academically Engaged'
       ELSE 'Low Visible Engagement' END,
  CASE WHEN sm.avg_percent >= 85 THEN 'Academic Excellence'
       WHEN sm.achievements  > 0  THEN 'Co-Curricular Strength'
       ELSE 'Developing Potential' END
FROM student_master_profile sm
WHERE sm.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1 FROM student_behavior_signals bs
    WHERE bs.student_id = sm.student_id AND bs.academic_yr = sm.academic_yr
  );


/* ============================================================
   STEP 12: VIEW — STUDENT AI PROFILE
   ============================================================ */

CREATE OR REPLACE VIEW student_ai_profile AS
SELECT
  d.student_id,
  CASE WHEN TRIM(d.student_name) LIKE '% %' THEN TRIM(d.student_name)
       WHEN p.father_name IS NOT NULL AND TRIM(p.father_name) != ''
            THEN CONCAT(TRIM(d.student_name), ' ', SUBSTRING_INDEX(TRIM(p.father_name), ' ', 1))
       ELSE TRIM(d.student_name) END AS student_name,
  d.gender, d.dob, d.academic_yr, d.parent_id,
  dc.class_name, ds.section_name,
  p.father_name, p.mother_name,
  sm.avg_percent, sm.written_avg, sm.oral_avg, sm.exams_taken,
  sm.attendance_percentage, sm.achievements, sm.homework_assigned_count,
  ss.strong_subjects, ss.strong_subjects_ai,
  bs.attendance_band, bs.learning_style, bs.engagement_pattern, bs.primary_strength_axis
FROM dim_student_demographics d
LEFT JOIN dim_class              dc  ON dc.class_id   = d.class_id
LEFT JOIN dim_section            ds  ON ds.section_id = d.section_id
LEFT JOIN dim_parent             p   ON p.parent_id   = d.parent_id
LEFT JOIN student_master_profile sm  ON sm.student_id = d.student_id AND sm.academic_yr = d.academic_yr
LEFT JOIN student_subject_strengths ss ON ss.student_id = d.student_id AND ss.academic_yr = d.academic_yr
LEFT JOIN student_behavior_signals  bs ON bs.student_id = d.student_id AND bs.academic_yr = d.academic_yr;


/* ============================================================
   STEP 12b: VIEW — CHILD SELECTOR
   ============================================================ */

CREATE OR REPLACE VIEW child_selector AS
SELECT parent_id, display_name, latest_student_id, latest_academic_yr
FROM (
  SELECT
    d.parent_id,
    CASE WHEN TRIM(d.student_name) LIKE '% %' THEN TRIM(d.student_name)
         WHEN p.father_name IS NOT NULL AND TRIM(p.father_name) != ''
              THEN CONCAT(TRIM(d.student_name), ' ', SUBSTRING_INDEX(TRIM(p.father_name), ' ', 1))
         ELSE TRIM(d.student_name) END AS display_name,
    d.student_id  AS latest_student_id,
    d.academic_yr AS latest_academic_yr,
    ROW_NUMBER() OVER (
      PARTITION BY d.parent_id,
        COALESCE(na.canonical, SUBSTRING_INDEX(TRIM(d.student_name), ' ', 1))
      ORDER BY d.academic_yr DESC
    ) AS rn
  FROM dim_student_demographics d
  LEFT JOIN dim_parent   p  ON p.parent_id  = d.parent_id
  LEFT JOIN name_aliases na ON na.parent_id = d.parent_id
                            AND na.raw_name = SUBSTRING_INDEX(TRIM(d.student_name), ' ', 1)
) ranked
WHERE rn = 1;


/* ============================================================
   STEP 13: ETL RUN LOG
   ============================================================ */

INSERT INTO etl_run_log (new_years_detected, status, notes)
SELECT COUNT(*), 'SUCCESS',
  CONCAT('Backfill range 2019-2020 to 2025-2026. Years processed: ',
         GROUP_CONCAT(academic_yr ORDER BY academic_yr SEPARATOR ', '))
FROM new_years;


/* ============================================================
   STEP 14: RECONCILIATION FIXES
   ============================================================ */

-- FIX A: Ensure dim_class / dim_section exist
INSERT IGNORE INTO arnolds_db.dim_class (class_id, class_name)
SELECT DISTINCT c.class_id, c.name FROM u333015459_arnolds.class c
WHERE c.class_id IN (SELECT DISTINCT s.class_id FROM u333015459_arnolds.student s
    WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND s.IsDelete = 'N')
  AND NOT EXISTS (SELECT 1 FROM arnolds_db.dim_class dc WHERE dc.class_id = c.class_id);

INSERT IGNORE INTO arnolds_db.dim_section (section_id, section_name)
SELECT DISTINCT sec.section_id, sec.name FROM u333015459_arnolds.section sec
WHERE sec.section_id IN (SELECT DISTINCT s.section_id FROM u333015459_arnolds.student s
    WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND s.IsDelete = 'N')
  AND NOT EXISTS (SELECT 1 FROM arnolds_db.dim_section ds WHERE ds.section_id = sec.section_id);


-- FIX B: Re-insert any students still missing
INSERT INTO arnolds_db.student_clean (student_id, academic_yr, reg_no, class_id, section_id)
SELECT s.student_id, s.academic_yr, s.reg_no, s.class_id, s.section_id
FROM u333015459_arnolds.student s
WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND s.IsDelete = 'N'
  AND NOT EXISTS (SELECT 1 FROM arnolds_db.student_clean sc
    WHERE sc.student_id = s.student_id AND sc.academic_yr = s.academic_yr);

INSERT INTO arnolds_db.dim_student_demographics
    (student_id, academic_yr, student_name, gender, dob,
     class_id, section_id, nationality, category, parent_id, guardian_name, guardian_mobile)
SELECT s.student_id, s.academic_yr, TRIM(s.student_name), s.gender,
    CASE WHEN CAST(s.dob AS CHAR) = '0000-00-00' THEN NULL ELSE s.dob END,
    s.class_id, s.section_id, s.nationality, s.category,
    s.parent_id, s.guardian_name, s.guardian_mobile
FROM u333015459_arnolds.student s
WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND s.IsDelete = 'N'
  AND NOT EXISTS (SELECT 1 FROM arnolds_db.dim_student_demographics d
    WHERE d.student_id = s.student_id AND d.academic_yr = s.academic_yr);

INSERT INTO arnolds_db.student_master_profile
    (student_id, academic_yr, reg_no, class_id, section_id,
     avg_percent, written_avg, oral_avg, exams_taken,
     attendance_percentage, achievements, homework_assigned_count)
SELECT s.student_id, s.academic_yr, s.reg_no, s.class_id, s.section_id,
    fa.avg_percent, fa.written_avg, fa.oral_avg, fa.exams_taken,
    fatt.attendance_percentage,
    COALESCE(fach.achievement_count, 0), COALESCE(fhw.homework_assigned_count, 0)
FROM arnolds_db.student_clean s
LEFT JOIN arnolds_db.fact_academics           fa   ON fa.student_id   = s.student_id AND fa.academic_yr   = s.academic_yr
LEFT JOIN arnolds_db.fact_attendance          fatt ON fatt.student_id = s.student_id AND fatt.academic_yr = s.academic_yr
LEFT JOIN arnolds_db.fact_achievements        fach ON fach.student_id = s.student_id AND fach.academic_yr = s.academic_yr
LEFT JOIN arnolds_db.fact_homework_engagement fhw  ON fhw.student_id  = s.student_id AND fhw.academic_yr  = s.academic_yr
WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026'
  AND NOT EXISTS (SELECT 1 FROM arnolds_db.student_master_profile sm
    WHERE sm.student_id = s.student_id AND sm.academic_yr = s.academic_yr);


-- FIX C: Re-insert any marks still missing
INSERT INTO arnolds_db.fact_student_subject_performance
    (student_id, academic_yr, subject_id, avg_percent, written_avg, oral_avg, exams_taken)
SELECT smc.student_id, smc.academic_yr,
    CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24 ELSE smc.subject_id END,
    ROUND(SUM(smc.marks_obtained) / NULLIF(SUM(smc.max_marks), 0) * 100, 2),
    ROUND(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36) THEN smc.marks_obtained END)
          / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36) THEN smc.max_marks END), 0) * 100, 2),
    ROUND(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41) THEN smc.marks_obtained END)
          / NULLIF(SUM(CASE WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41) THEN smc.max_marks END), 0) * 100, 2),
    COUNT(DISTINCT smc.exam_id)
FROM u333015459_arnolds.student_marks_components smc
JOIN u333015459_arnolds.student st ON st.student_id = smc.student_id AND st.academic_yr = smc.academic_yr AND st.IsDelete = 'N'
JOIN u333015459_arnolds.class c ON c.class_id = st.class_id AND c.academic_yr = st.academic_yr
JOIN u333015459_arnolds.subject sub ON sub.sm_id = smc.subject_id AND sub.academic_yr = smc.academic_yr AND sub.class_id = st.class_id
WHERE smc.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND smc.is_present = 'Y'
  AND NOT EXISTS (
    SELECT 1 FROM arnolds_db.fact_student_subject_performance fsp
    WHERE fsp.student_id = smc.student_id AND fsp.academic_yr = smc.academic_yr
      AND fsp.subject_id = CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24 ELSE smc.subject_id END
  )
GROUP BY smc.student_id, smc.academic_yr,
    CASE WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24 ELSE smc.subject_id END;


-- FIX D: Insert any missing parent records
INSERT IGNORE INTO arnolds_db.dim_parent (parent_id, father_name, mother_name)
SELECT parent_id, father_name, mother_name FROM u333015459_arnolds.parent
WHERE NOT EXISTS (SELECT 1 FROM arnolds_db.dim_parent dp WHERE dp.parent_id = u333015459_arnolds.parent.parent_id);


-- FIX E: Ghost student preview (review before uncommenting deletes)
SELECT sm.student_id, sm.academic_yr, d.student_name
FROM arnolds_db.student_master_profile sm
LEFT JOIN arnolds_db.dim_student_demographics d ON d.student_id = sm.student_id AND d.academic_yr = sm.academic_yr
LEFT JOIN u333015459_arnolds.student s ON s.student_id = sm.student_id AND s.academic_yr = sm.academic_yr
WHERE sm.academic_yr BETWEEN '2019-2020' AND '2025-2026'
  AND (s.student_id IS NULL OR s.IsDelete = 'Y');

/*
DELETE sm FROM arnolds_db.student_master_profile sm
LEFT JOIN u333015459_arnolds.student s ON s.student_id = sm.student_id AND s.academic_yr = sm.academic_yr
WHERE sm.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND (s.student_id IS NULL OR s.IsDelete = 'Y');

DELETE sbs FROM arnolds_db.student_behavior_signals sbs
LEFT JOIN u333015459_arnolds.student s ON s.student_id = sbs.student_id AND s.academic_yr = sbs.academic_yr
WHERE sbs.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND (s.student_id IS NULL OR s.IsDelete = 'Y');
*/


/* ============================================================
   STEP 15: FINAL VERIFICATION
   ============================================================ */

-- 1. Row counts per year
SELECT
    src.academic_yr,
    src.total_source                               AS arnolds1_students,
    COALESCE(smp.in_master,     0)                 AS in_master_profile,
    COALESCE(att.in_attendance, 0)                 AS have_attendance,
    COALESCE(mrk.have_marks,    0)                 AS have_marks,
    src.total_source - COALESCE(smp.in_master, 0)  AS missing_from_master,
    CASE WHEN src.total_source = COALESCE(smp.in_master, 0)
          AND src.total_source = COALESCE(att.in_attendance, 0) THEN '✅ Complete'
         WHEN COALESCE(smp.in_master, 0) = 0 THEN '❌ ETL not run'
         ELSE '⚠️  Partial' END AS etl_status
FROM (
    SELECT academic_yr, COUNT(DISTINCT student_id) AS total_source
    FROM u333015459_arnolds.student
    WHERE academic_yr BETWEEN '2019-2020' AND '2025-2026' AND IsDelete = 'N'
    GROUP BY academic_yr
) src
LEFT JOIN (SELECT academic_yr, COUNT(DISTINCT student_id) AS in_master FROM arnolds_db.student_master_profile GROUP BY academic_yr) smp ON smp.academic_yr = src.academic_yr
LEFT JOIN (SELECT academic_yr, COUNT(DISTINCT student_id) AS in_attendance FROM arnolds_db.fact_attendance GROUP BY academic_yr) att ON att.academic_yr = src.academic_yr
LEFT JOIN (SELECT academic_yr, COUNT(DISTINCT student_id) AS have_marks FROM arnolds_db.fact_student_subject_performance GROUP BY academic_yr) mrk ON mrk.academic_yr = src.academic_yr
ORDER BY src.academic_yr;

-- 2. dim_teachers verification
SELECT
    COUNT(*) AS total_teachers_loaded,
    SUM(CASE WHEN designation LIKE '%Principal%' THEN 1 ELSE 0 END) AS principals,
    SUM(CASE WHEN designation LIKE '%TGT%'       THEN 1 ELSE 0 END) AS tgt_teachers,
    SUM(CASE WHEN designation LIKE '%PGT%'       THEN 1 ELSE 0 END) AS pgt_teachers,
    SUM(CASE WHEN designation LIKE '%PRT%'       THEN 1 ELSE 0 END) AS prt_teachers
FROM arnolds_db.dim_teachers;

-- 3. Students still missing from master (should be 0 rows)
SELECT s.student_id, s.academic_yr, s.student_name
FROM u333015459_arnolds.student s
WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026' AND s.IsDelete = 'N'
  AND NOT EXISTS (SELECT 1 FROM student_master_profile sm
    WHERE sm.student_id = s.student_id AND sm.academic_yr = s.academic_yr)
ORDER BY s.academic_yr, s.student_id;

-- 4. Test/bloat subjects (should be 0 rows)
SELECT subject_id, subject_name FROM arnolds_db.dim_subject
WHERE subject_name IN ('New subject Regular','new subject common code test',
    'SUBJECT_TEST2','SubjectForRCTest','New RC subject','New sub for RC','test');

-- 5. subject_id 24 check (should be exactly 1 row)
SELECT * FROM dim_subject WHERE subject_id = 24;

-- 6. Science components completeness
SELECT academic_yr,
    COUNT(DISTINCT student_id) AS students,
    SUM(CASE WHEN component_count != 3 THEN 1 ELSE 0 END) AS incomplete
FROM (SELECT student_id, academic_yr, COUNT(*) AS component_count
      FROM fact_science_components GROUP BY student_id, academic_yr) sub
GROUP BY academic_yr ORDER BY academic_yr;

-- 7. Unclassified marks_headings (should be 0 UNCLASSIFIED rows)
SELECT DISTINCT smc.marks_headings_id, mh.name,
  CASE WHEN smc.marks_headings_id IN (1,5,15,18,19,20,25,28,33,34,36) THEN 'Written'
       WHEN smc.marks_headings_id IN (2,3,4,6,7,11,21,22,23,24,26,27,30,31,32,37,38,39,40,41) THEN 'Internal'
       ELSE 'UNCLASSIFIED ⚠️' END AS classification
FROM u333015459_arnolds.student_marks_components smc
JOIN u333015459_arnolds.marks_headings mh ON mh.marks_headings_id = smc.marks_headings_id
WHERE smc.academic_yr BETWEEN '2019-2020' AND '2025-2026'
ORDER BY classification DESC, smc.marks_headings_id;

/* ===================== END OF SCRIPT ======================== */