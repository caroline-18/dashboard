/* ============================================================
   ETL SCRIPT: student_analytics
   SOURCE DB  : arnolds1
   TARGET DB  : student_analytics
   MODE       : Full Backfill + Incremental + Idempotent
   YEARS      : 2019-2020 through 2025-2026
   PURPOSE    : AI-ready student analytics
   ============================================================
   CHANGE LOG:
   - Fixed: new_years filter now covers ALL years 2019-2026
             instead of only years greater than the current max,
             which caused students from prior years to be skipped.
   - Fixed: Removed isActive='Y' filter from backfill scope so
             historically inactive students are still captured.
   - Fixed: fact_student_subject_performance now also respects
             the academic_yr IN (new_years) filter for consistency.
   - Added:  ETL run log captures year range and row counts.
   ============================================================ */

USE student_analytics;

/* ============================================================
   STEP 0: DEFINE TARGET YEARS
   ============================================================ */

DROP TEMPORARY TABLE IF EXISTS new_years;

CREATE TEMPORARY TABLE new_years AS
SELECT DISTINCT academic_yr
FROM arnolds1.student
WHERE academic_yr BETWEEN '2019-2020' AND '2025-2026';


/* ============================================================
   STEP 1: STUDENT CLEAN
   ============================================================ */

INSERT INTO student_clean
(student_id, academic_yr, reg_no, class_id, section_id)
SELECT
    s.student_id,
    s.academic_yr,
    s.reg_no,
    s.class_id,
    s.section_id
FROM arnolds1.student s
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND s.IsDelete = 'N'
  AND NOT EXISTS (
      SELECT 1
      FROM student_clean sc
      WHERE sc.student_id = s.student_id
        AND sc.academic_yr = s.academic_yr
  );


/* ============================================================
   STEP 2: STUDENT DEMOGRAPHICS
   ============================================================ */

INSERT INTO dim_student_demographics
(student_id, academic_yr, student_name, gender, dob,
 class_id, section_id, nationality, category,
 parent_id, guardian_name, guardian_mobile)
SELECT
    s.student_id,
    s.academic_yr,
    s.student_name,
    s.gender,
    CASE
        WHEN CAST(s.dob AS CHAR) = '0000-00-00' THEN NULL
        ELSE s.dob
    END,
    s.class_id,
    s.section_id,
    s.nationality,
    s.category,
    s.parent_id,
    s.guardian_name,
    COALESCE(s.guardian_mobile, '')
FROM arnolds1.student s
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND s.IsDelete = 'N'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_student_demographics d
      WHERE d.student_id = s.student_id
        AND d.academic_yr = s.academic_yr
  );


/* ============================================================
   STEP 3: PARENT DIMENSION
   ============================================================ */

INSERT INTO dim_parent
(parent_id, father_name, mother_name,
 father_contact, mother_contact)
SELECT DISTINCT
    p.parent_id,
    p.father_name,
    p.mother_name,
    p.f_mobile,
    p.m_mobile
FROM arnolds1.parent p
WHERE p.IsDelete = 'N'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_parent dp
      WHERE dp.parent_id = p.parent_id
  );


/* ============================================================
   STEP 4: ATTENDANCE FACT
   ============================================================ */

INSERT INTO fact_attendance
(student_id, academic_yr, total_school_days,
 absent_days, present_days, attendance_percentage)
SELECT
    ac.student_id,
    ac.academic_yr,
    COUNT(*),
    COUNT(*) - SUM(ac.attendance_status = 'P'),
    SUM(ac.attendance_status = 'P'),
    ROUND(SUM(ac.attendance_status = 'P') / COUNT(*) * 100, 2)
FROM attendance_clean ac
WHERE ac.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
      SELECT 1
      FROM fact_attendance fa
      WHERE fa.student_id = ac.student_id
        AND fa.academic_yr = ac.academic_yr
  )
GROUP BY ac.student_id, ac.academic_yr;


/* ============================================================
   STEP 5: ACADEMICS FACT
   ============================================================ */

INSERT INTO fact_academics
(student_id, academic_yr, avg_percent, written_avg, oral_avg, exams_taken)
SELECT
    md.student_id,
    mm.academic_yr,

    /* OVERALL % (weighted) */
    ROUND(
        SUM(md.marks_obtained) /
        NULLIF(SUM(md.highest_marks),0) * 100
    ,2) AS overall_percent,

    /* WRITTEN % */
    ROUND(
        SUM(CASE
            WHEN mh.marks_headings_name = 'Written'
            THEN md.marks_obtained
        END) /
        NULLIF(SUM(CASE
            WHEN mh.marks_headings_name = 'Written'
            THEN md.highest_marks
        END),0) * 100
    ,2) AS written_percent,

    /* INTERNAL % (Oral + Practical + others) */
    ROUND(
        SUM(CASE
            WHEN mh.marks_headings_name != 'Written'
            THEN md.marks_obtained
        END) /
        NULLIF(SUM(CASE
            WHEN mh.marks_headings_name != 'Written'
            THEN md.highest_marks
        END),0) * 100
    ,2) AS internal_percent,

    COUNT(DISTINCT mm.exam_id) AS exams_taken

FROM mark_master mm
JOIN mark_detail md
  ON md.mark_id = mm.mark_id
JOIN marks_headings mh
  ON mh.marks_headings_id = mm.marks_headings_id

WHERE mm.academic_yr IN (SELECT academic_yr FROM new_years)
  AND md.present = 'Y'

GROUP BY
    md.student_id,
    mm.academic_yr
ON DUPLICATE KEY UPDATE
    avg_percent   = VALUES(avg_percent),
    written_avg   = VALUES(written_avg),
    oral_avg      = VALUES(oral_avg),
    exams_taken   = VALUES(exams_taken);



/* ============================================================
   STEP 6: SUBJECT PERFORMANCE
   ============================================================ */

INSERT INTO fact_student_subject_performance
(student_id, academic_yr, subject_id,
 avg_percent, written_avg, oral_avg, exams_taken)
SELECT
    smc.student_id,
    smc.academic_yr,
    smc.subject_id,
    ROUND(AVG(smc.marks_obtained / NULLIF(smc.max_marks,0) * 100),2),
    ROUND(AVG(CASE WHEN smc.written_exam='Y'
         THEN smc.marks_obtained / NULLIF(smc.max_marks,0) * 100 END),2),
    ROUND(AVG(CASE WHEN smc.written_exam='N'
         THEN smc.marks_obtained / NULLIF(smc.max_marks,0) * 100 END),2),
    COUNT(DISTINCT smc.exam_id)
FROM student_marks_components smc
WHERE smc.academic_yr IN (SELECT academic_yr FROM new_years)
  AND smc.is_present='Y'
  AND NOT EXISTS (
      SELECT 1
      FROM fact_student_subject_performance fsp
      WHERE fsp.student_id = smc.student_id
        AND fsp.academic_yr = smc.academic_yr
        AND fsp.subject_id = smc.subject_id
  )
GROUP BY smc.student_id, smc.academic_yr, smc.subject_id;


/* ============================================================
   STEP 7: SUBJECT STRENGTHS VIEW
   ============================================================ */

INSERT INTO fact_student_subject_performance
(student_id, academic_yr, subject_id,
 avg_percent, written_avg, oral_avg, exams_taken)
SELECT
    md.student_id,
    mm.academic_yr,
    mm.subject_id,

    ROUND(
        SUM(md.marks_obtained) /
        NULLIF(SUM(md.highest_marks),0) * 100
    ,2),

    ROUND(
        SUM(CASE
            WHEN mh.marks_headings_name = 'Written'
            THEN md.marks_obtained
        END) /
        NULLIF(SUM(CASE
            WHEN mh.marks_headings_name = 'Written'
            THEN md.highest_marks
        END),0) * 100
    ,2),

    ROUND(
        SUM(CASE
            WHEN mh.marks_headings_name != 'Written'
            THEN md.marks_obtained
        END) /
        NULLIF(SUM(CASE
            WHEN mh.marks_headings_name != 'Written'
            THEN md.highest_marks
        END),0) * 100
    ,2),

    COUNT(DISTINCT mm.exam_id)

FROM mark_master mm
JOIN mark_detail md
  ON md.mark_id = mm.mark_id
JOIN marks_headings mh
  ON mh.marks_headings_id = mm.marks_headings_id

WHERE mm.academic_yr IN (SELECT academic_yr FROM new_years)
  AND md.present = 'Y'

GROUP BY
    md.student_id,
    mm.academic_yr,
    mm.subject_id
ON DUPLICATE KEY UPDATE
    avg_percent = VALUES(avg_percent),
    written_avg = VALUES(written_avg),
    oral_avg    = VALUES(oral_avg),
    exams_taken = VALUES(exams_taken);



/* ============================================================
   STEP 8: ACHIEVEMENTS FACT
   ============================================================ */

INSERT INTO fact_achievements
(student_id, academic_yr, achievement_count, achievement_list)
SELECT
    a.student_id,
    'ALL',                          -- sentinel value to flag lifetime row
    COUNT(*),
    GROUP_CONCAT(
        DISTINCT CONCAT(
            a.academic_yr, ': ',    -- prefix each achievement with its year
            a.event,
            CASE
                WHEN a.position IS NOT NULL AND a.position != ''
                THEN CONCAT(' - ', a.position)
                ELSE ''
            END
        )
        ORDER BY a.academic_yr, a.event
        SEPARATOR ', '
    )
FROM arnolds1.achievements_clean a
GROUP BY a.student_id
ON DUPLICATE KEY UPDATE
    achievement_count = VALUES(achievement_count),
    achievement_list  = VALUES(achievement_list);
    
/* ============================================================
   STEP 9: MASTER PROFILE
   ============================================================ */

-- Step 9a: Drop and recreate with proper primary key
DROP TABLE IF EXISTS student_master_profile;

CREATE TABLE student_master_profile (
    student_id              INT             NOT NULL,
    academic_yr             VARCHAR(11)     NOT NULL,
    reg_no                  VARCHAR(10),
    class_id                INT             NOT NULL,
    section_id              INT             NOT NULL,
    avg_percent             DECIMAL(11,2),
    written_avg             DECIMAL(11,2),
    oral_avg                DECIMAL(11,2),
    exams_taken             BIGINT          DEFAULT 0,
    attendance_percentage   DECIMAL(27,2),
    achievements            BIGINT          NOT NULL DEFAULT 0,
    achievement_list        TEXT,
    homework_assigned_count BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (student_id, academic_yr)
);

-- Step 9b: Insert clean data with lifetime achievements
INSERT INTO student_master_profile
(student_id, academic_yr, reg_no, class_id, section_id,
 avg_percent, written_avg, oral_avg, exams_taken,
 attendance_percentage, achievements,
 achievement_list, homework_assigned_count)
SELECT
    s.student_id,
    s.academic_yr,
    s.reg_no,
    s.class_id,
    s.section_id,
    fa.avg_percent,
    fa.written_avg,
    fa.oral_avg,
    fa.exams_taken,
    fatt.attendance_percentage,
    COALESCE(fach.achievement_count, 0),
    COALESCE(fach.achievement_list, ''),
    COALESCE(fhw.homework_assigned_count, 0)
FROM student_clean s
LEFT JOIN fact_academics fa
    ON fa.student_id = s.student_id
   AND fa.academic_yr = s.academic_yr
LEFT JOIN fact_attendance fatt
    ON fatt.student_id = s.student_id
   AND fatt.academic_yr = s.academic_yr
LEFT JOIN fact_achievements fach
    ON fach.student_id  = s.student_id
   AND fach.academic_yr = 'ALL'
LEFT JOIN fact_homework_engagement fhw
    ON fhw.student_id = s.student_id
   AND fhw.academic_yr = s.academic_yr
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
ON DUPLICATE KEY UPDATE
    avg_percent             = VALUES(avg_percent),
    written_avg             = VALUES(written_avg),
    oral_avg                = VALUES(oral_avg),
    exams_taken             = VALUES(exams_taken),
    attendance_percentage   = VALUES(attendance_percentage),
    achievements            = VALUES(achievements),
    achievement_list        = VALUES(achievement_list),
    homework_assigned_count = VALUES(homework_assigned_count);

-- Step 9c: Verify
SELECT student_id, academic_yr, achievements, achievement_list
FROM student_master_profile
WHERE student_id = 8608;

-- Step 9d: Confirm no duplicates
SELECT student_id, academic_yr, COUNT(*) AS cnt
FROM student_master_profile
GROUP BY student_id, academic_yr
HAVING cnt > 1;
/* ============================================================
   MULTI-YEAR TREND VIEW
   ============================================================ */

CREATE OR REPLACE VIEW student_academic_trend AS
SELECT
    student_id,
    academic_yr,
    avg_percent,
    LAG(avg_percent) OVER (
        PARTITION BY student_id
        ORDER BY academic_yr
    ) prev_year_percent,
    ROUND(
        avg_percent -
        LAG(avg_percent) OVER (
            PARTITION BY student_id
            ORDER BY academic_yr
        ),2) year_growth,
    CASE
        WHEN LAG(avg_percent) OVER (
            PARTITION BY student_id
            ORDER BY academic_yr
        ) IS NULL THEN 'Baseline Year'
        WHEN avg_percent >
             LAG(avg_percent) OVER (
                 PARTITION BY student_id
                 ORDER BY academic_yr
             ) THEN 'Improving'
        WHEN avg_percent <
             LAG(avg_percent) OVER (
                 PARTITION BY student_id
                 ORDER BY academic_yr
             ) THEN 'Declining'
        ELSE 'Stable'
    END performance_trend
FROM student_master_profile;


/* ============================================================
   CAREER CONFIDENCE VIEW
   ============================================================ */

CREATE OR REPLACE VIEW student_career_profile AS
SELECT
    sm.student_id,
    sm.academic_yr,
    (
        COALESCE(sm.avg_percent,0)*0.30 +
        COALESCE(sm.attendance_percentage,0)*0.20 +
        (CASE
            WHEN sat.performance_trend='Improving' THEN 20
            WHEN sat.performance_trend='Stable' THEN 10
            ELSE 0
         END)*0.20 +
        (CASE
            WHEN ss.strong_subjects IS NOT NULL
                 AND ss.strong_subjects!=''
            THEN 100 ELSE 0 END)*0.15 +
        (CASE
            WHEN sm.achievements>0
            THEN 100 ELSE 0 END)*0.15
    ) career_confidence_index,
    sat.performance_trend,
    sat.year_growth
FROM student_master_profile sm
LEFT JOIN student_academic_trend sat
    ON sat.student_id=sm.student_id
   AND sat.academic_yr=sm.academic_yr
LEFT JOIN student_subject_strengths ss
    ON ss.student_id=sm.student_id
   AND ss.academic_yr=sm.academic_yr;


/* ============================================================
   FINAL AI PROFILE VIEW
   ============================================================ */

CREATE OR REPLACE VIEW student_ai_profile AS
SELECT
    d.*,

    /* Parent Information */
    p.father_name,
    p.mother_name,
    p.father_contact,
    p.mother_contact,

    /* Academic Metrics (YEAR-SPECIFIC) */
    sm.avg_percent,
    sm.written_avg,
    sm.oral_avg,
    sm.exams_taken,
    sm.attendance_percentage,
    sm.homework_assigned_count,

    /* 🔥 Lifetime Achievements (ALL YEARS) */
    COALESCE(la.total_achievements, 0) AS achievements,
    COALESCE(la.total_achievement_list, '') AS achievement_list,

    /* Subject Strengths (YEAR-SPECIFIC) */
    ss.strong_subjects,

    /* Career Profile (YEAR-SPECIFIC) */
    cp.career_confidence_index,
    cp.performance_trend,
    cp.year_growth

FROM dim_student_demographics d

/* Parent */
LEFT JOIN dim_parent p
    ON p.parent_id = d.parent_id

/* Master profile (year-specific academics) */
LEFT JOIN student_master_profile sm
    ON sm.student_id = d.student_id
   AND sm.academic_yr = d.academic_yr

/* Subject strengths (year-specific) */
LEFT JOIN student_subject_strengths ss
    ON ss.student_id = d.student_id
   AND ss.academic_yr = d.academic_yr

/* 🔥 Lifetime Achievements Aggregation */
LEFT JOIN (
    SELECT
        student_id,
        SUM(achievement_count) AS total_achievements,
        GROUP_CONCAT(
            DISTINCT achievement_list
            SEPARATOR ', '
        ) AS total_achievement_list
    FROM fact_achievements
    GROUP BY student_id
) la
    ON la.student_id = d.student_id

/* Career Profile (year-specific) */
LEFT JOIN student_career_profile cp
    ON cp.student_id = d.student_id
   AND cp.academic_yr = d.academic_yr;


/* ============================================================
   STEP 13: ETL RUN LOG
   ============================================================ */

INSERT INTO etl_run_log
  (new_years_detected, status, notes)
SELECT
  COUNT(*),
  'SUCCESS',
  CONCAT(
    'Backfill range 2019-2020 to 2025-2026. Years processed: ',
    GROUP_CONCAT(academic_yr ORDER BY academic_yr SEPARATOR ', ')
  )
FROM new_years;

/* ============================================================
   VERIFICATION QUERIES
   Run these after the ETL to confirm completeness.
   ============================================================ */

-- 1. Row counts per year in the master profile
SELECT
  academic_yr,
  COUNT(*) AS student_count
FROM student_master_profile
WHERE academic_yr BETWEEN '2019-2020' AND '2025-2026'
GROUP BY academic_yr
ORDER BY academic_yr;

-- 2. Students in source that are MISSING from the master profile
--    (should return 0 rows after a successful run)
SELECT
  s.student_id,
  s.academic_yr,
  s.student_name,
  s.parent_id
FROM arnolds1.student s
WHERE s.academic_yr BETWEEN '2019-2020' AND '2025-2026'
  AND s.IsDelete = 'N'
  AND NOT EXISTS (
    SELECT 1
    FROM student_master_profile sm
    WHERE sm.student_id  = s.student_id
      AND sm.academic_yr = s.academic_yr
  )
ORDER BY s.academic_yr, s.student_id;

-- 3. Students with no parent_id (data quality check)
SELECT
  d.student_id,
  d.student_name,
  d.academic_yr,
  d.parent_id
FROM dim_student_demographics d
WHERE d.academic_yr BETWEEN '2019-2020' AND '2025-2026'
  AND (d.parent_id IS NULL OR d.parent_id = '')
ORDER BY d.academic_yr, d.student_id;

/* ===================== END OF ETL =========================== */