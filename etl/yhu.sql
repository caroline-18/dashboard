/* ============================================================
   ETL SCRIPT: student_analytics (INCREMENTAL)
   SOURCE DB  : arnolds1
   TARGET DB  : student_analytics
   MODE       : Incremental + Idempotent
   ============================================================ */

USE student_analytics;

/* ------------------------------------------------------------
   1. Detect NEW academic years
   ------------------------------------------------------------ */
DROP TEMPORARY TABLE IF EXISTS new_years;

CREATE TEMPORARY TABLE new_years AS
SELECT DISTINCT academic_yr
FROM arnolds1.student
WHERE academic_yr >
  (SELECT COALESCE(MAX(academic_yr), '0000-0000')
   FROM student_master_profile);


/* ------------------------------------------------------------
   2. Incremental STUDENT CLEAN
   ------------------------------------------------------------ */
INSERT INTO student_clean
(
  student_id,
  academic_yr,
  reg_no,
  class_id,
  section_id
)
SELECT
  s.student_id,
  s.academic_yr,
  s.reg_no,
  s.class_id,
  s.section_id
FROM arnolds1.student s
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND s.IsDelete = 'N'
  AND s.isActive = 'Y'
  AND NOT EXISTS (
    SELECT 1
    FROM student_clean sc
    WHERE sc.student_id = s.student_id
      AND sc.academic_yr = s.academic_yr
  );


/* ------------------------------------------------------------
   3. Incremental FACT: ATTENDANCE
   ------------------------------------------------------------ */
INSERT INTO fact_attendance
(
  student_id,
  academic_yr,
  total_school_days,
  absent_days,
  present_days,
  attendance_percentage
)
SELECT
  ac.student_id,
  ac.academic_yr,
  COUNT(*) AS total_school_days,
  COUNT(*) - SUM(CASE WHEN ac.attendance_status = 'P' THEN 1 ELSE 0 END) AS absent_days,
  SUM(CASE WHEN ac.attendance_status = 'P' THEN 1 ELSE 0 END) AS present_days,
  ROUND(
    (SUM(CASE WHEN ac.attendance_status = 'P' THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    2
  ) AS attendance_percentage
FROM attendance_clean ac
WHERE ac.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1
    FROM fact_attendance fa
    WHERE fa.student_id = ac.student_id
      AND fa.academic_yr = ac.academic_yr
  )
GROUP BY ac.student_id, ac.academic_yr;


/* ------------------------------------------------------------
   4. Incremental FACT: ACADEMICS
   ------------------------------------------------------------ */
INSERT INTO fact_academics
(
  student_id,
  academic_yr,
  avg_percent,
  written_avg,
  oral_avg,
  exams_taken
)
SELECT
  smc.student_id,
  smc.academic_yr,

  ROUND(
    AVG((smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100),
    2
  ) AS avg_percent,

  ROUND(
    AVG(
      CASE
        WHEN smc.written_exam = 'Y'
        THEN (smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100
      END
    ),
    2
  ) AS written_avg,

  ROUND(
    AVG(
      CASE
        WHEN smc.written_exam = 'N'
        THEN (smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100
      END
    ),
    2
  ) AS oral_avg,

  COUNT(DISTINCT smc.exam_id) AS exams_taken

FROM student_marks_components smc
WHERE smc.academic_yr IN (SELECT academic_yr FROM new_years)
  AND smc.is_present = 'Y'
  AND NOT EXISTS (
    SELECT 1
    FROM fact_academics fa
    WHERE fa.student_id = smc.student_id
      AND fa.academic_yr = smc.academic_yr
  )
GROUP BY smc.student_id, smc.academic_yr;


/* ------------------------------------------------------------
   5. Incremental FACT: ACHIEVEMENTS
   ------------------------------------------------------------ */
INSERT INTO fact_achievements
(
  student_id,
  academic_yr,
  achievement_count
)
SELECT
  a.student_id,
  a.academic_yr,
  COUNT(*) AS achievement_count
FROM arnolds1.achievements_clean a
WHERE a.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1
    FROM fact_achievements fa
    WHERE fa.student_id = a.student_id
      AND fa.academic_yr = a.academic_yr
  )
GROUP BY a.student_id, a.academic_yr;


/* ------------------------------------------------------------
   6. Incremental FACT: HOMEWORK ENGAGEMENT
   ------------------------------------------------------------ */
INSERT INTO fact_homework_engagement
(
  student_id,
  academic_yr,
  homework_assigned_count
)
SELECT
  hc.student_id,
  h.academic_yr,
  COUNT(DISTINCT h.homework_id) AS homework_assigned_count
FROM arnolds1.homework h
JOIN arnolds1.homework_comments hc
  ON h.homework_id = hc.homework_id
WHERE h.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1
    FROM fact_homework_engagement fh
    WHERE fh.student_id = hc.student_id
      AND fh.academic_yr = h.academic_yr
  )
GROUP BY hc.student_id, h.academic_yr;

/* ------------------------------------------------------------
   6. Incremental FACT: Subject-Level 
   ------------------------------------------------------------ */
   
INSERT INTO fact_student_subject_performance
(
  student_id,
  academic_yr,
  subject_id,
  avg_percent,
  written_avg,
  oral_avg,
  exams_taken
)
SELECT
  smc.student_id,
  smc.academic_yr,
  smc.subject_id,

  ROUND(
    AVG((smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100),
    2
  ) AS avg_percent,

  ROUND(
    AVG(
      CASE WHEN smc.written_exam = 'Y'
      THEN (smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100 END
    ),
    2
  ) AS written_avg,

  ROUND(
    AVG(
      CASE WHEN smc.written_exam = 'N'
      THEN (smc.marks_obtained / NULLIF(smc.max_marks, 0)) * 100 END
    ),
    2
  ) AS oral_avg,

  COUNT(DISTINCT smc.exam_id) AS exams_taken

FROM student_marks_components smc
WHERE smc.is_present = 'Y'
  AND NOT EXISTS (
    SELECT 1
    FROM fact_student_subject_performance fsp
    WHERE fsp.student_id = smc.student_id
      AND fsp.academic_yr = smc.academic_yr
      AND fsp.subject_id = smc.subject_id
  )
GROUP BY smc.student_id, smc.academic_yr, smc.subject_id;



/* ------------------------------------------------------------
   7. Incremental MASTER PROFILE
   ------------------------------------------------------------ */
INSERT INTO student_master_profile
(
  student_id,
  academic_yr,
  reg_no,
  class_id,
  section_id,
  avg_percent,
  written_avg,
  oral_avg,
  exams_taken,
  attendance_percentage,
  achievements,
  homework_engagement_count
)
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

  COALESCE(fach.achievement_count, 0) AS achievements,
  COALESCE(fhw.homework_assigned_count, 0) AS homework_engagement_count

FROM student_clean s
LEFT JOIN fact_academics fa
  ON fa.student_id = s.student_id
 AND fa.academic_yr = s.academic_yr
LEFT JOIN fact_attendance fatt
  ON fatt.student_id = s.student_id
 AND fatt.academic_yr = s.academic_yr
LEFT JOIN fact_achievements fach
  ON fach.student_id = s.student_id
 AND fach.academic_yr = s.academic_yr
LEFT JOIN fact_homework_engagement fhw
  ON fhw.student_id = s.student_id
 AND fhw.academic_yr = s.academic_yr
WHERE s.academic_yr IN (SELECT academic_yr FROM new_years)
  AND NOT EXISTS (
    SELECT 1
    FROM student_master_profile sm
    WHERE sm.student_id = s.student_id
      AND sm.academic_yr = s.academic_yr
  );
  

INSERT INTO etl_run_log (new_years_detected, status)
SELECT COUNT(*), 'SUCCESS' FROM new_years;


/* ===================== END OF ETL =========================== */
