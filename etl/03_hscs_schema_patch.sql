-- ================================================================
-- HSCS Analytics Schema Patch
-- Creates missing tables in hscs_analytics to match ETL expectations
-- Safe to run multiple times (CREATE TABLE IF NOT EXISTS throughout)
-- ================================================================

USE hscs_analytics;

-- --------------------------------------------------------
-- Missing staging tables for marks pipeline
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS mark_headings_clean (
    marks_headings_id INT          NOT NULL PRIMARY KEY,
    component_name    VARCHAR(50)  NOT NULL,
    written_exam      VARCHAR(1)   NOT NULL,
    sequence          INT          NOT NULL
);

CREATE TABLE IF NOT EXISTS student_marks_raw (
    marks_id                  INT            NOT NULL,
    class_id                  INT            NOT NULL,
    section_id                INT            NOT NULL,
    exam_id                   INT            NOT NULL,
    subject_id                VARCHAR(100)   NOT NULL,
    student_id                INT            NOT NULL,
    exam_date                 DATE,
    present                   VARCHAR(200)   NOT NULL,
    mark_obtained             VARCHAR(200)   NOT NULL,
    highest_marks             VARCHAR(200)   NOT NULL,
    reportcard_marks          VARCHAR(300),
    reportcard_highest_marks  VARCHAR(300),
    total_marks               INT,
    highest_total_marks       INT,
    grade                     VARCHAR(200)   NOT NULL,
    percent                   DECIMAL(5,2),
    publish                   CHAR(1)        NOT NULL DEFAULT 'N',
    academic_yr               VARCHAR(11)    NOT NULL,
    rn                        BIGINT UNSIGNED NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS student_marks_clean (
    marks_id                  INT            NOT NULL,
    class_id                  INT            NOT NULL,
    section_id                INT            NOT NULL,
    exam_id                   INT            NOT NULL,
    subject_id                VARCHAR(100)   NOT NULL,
    student_id                INT            NOT NULL,
    exam_date                 DATE,
    present                   VARCHAR(200)   NOT NULL,
    mark_obtained             VARCHAR(200)   NOT NULL,
    highest_marks             VARCHAR(200)   NOT NULL,
    reportcard_marks          VARCHAR(300),
    reportcard_highest_marks  VARCHAR(300),
    total_marks               INT,
    highest_total_marks       INT,
    grade                     VARCHAR(200)   NOT NULL,
    percent                   DECIMAL(5,2),
    publish                   CHAR(1)        NOT NULL DEFAULT 'N',
    academic_yr               VARCHAR(11)    NOT NULL,
    rn                        BIGINT UNSIGNED NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS student_marks_components (
    student_id        INT            NOT NULL,
    subject_id        VARCHAR(100)   NOT NULL,
    exam_id           INT            NOT NULL,
    academic_yr       VARCHAR(11)    NOT NULL,
    marks_headings_id BIGINT UNSIGNED,
    component_name    VARCHAR(50),
    written_exam      VARCHAR(1),
    is_present        LONGTEXT,
    marks_obtained    DECIMAL(5,2),
    max_marks         DECIMAL(5,2)
);

-- --------------------------------------------------------
-- Missing dimension tables
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_teachers (
    teacher_id  INT          NOT NULL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    designation VARCHAR(100),
    class_id    INT,
    section_id  INT
);

CREATE TABLE IF NOT EXISTS dim_users (
    user_id  VARCHAR(50)  NOT NULL PRIMARY KEY COMMENT 'Login username',
    name     VARCHAR(200) NOT NULL,
    password VARCHAR(20)  NOT NULL,
    reg_id   INT          NOT NULL,
    role_id  CHAR(1)      NOT NULL COMMENT 'P=Parent T=Teacher M=Principal'
);

-- --------------------------------------------------------
-- Missing fact tables
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_science_components (
    student_id   INT          NOT NULL,
    academic_yr  VARCHAR(11)  NOT NULL,
    component    VARCHAR(30)  NOT NULL,
    avg_percent  DECIMAL(5,2),
    is_entered   TINYINT      NOT NULL DEFAULT 1,
    PRIMARY KEY (student_id, academic_yr, component)
);

CREATE TABLE IF NOT EXISTS fact_academics_dedup (
    student_id   INT             NOT NULL,
    academic_yr  VARCHAR(11)     NOT NULL,
    avg_percent  DECIMAL(15,6),
    written_avg  DECIMAL(15,6),
    oral_avg     DECIMAL(15,6),
    exams_taken  BIGINT
);

-- --------------------------------------------------------
-- Fix dim_student_demographics — add missing columns
-- that our ETL inserts but hscs schema doesn't have
-- --------------------------------------------------------

SELECT 1;

-- --------------------------------------------------------
-- ETL support tables (if missing)
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS attendance_clean (
    student_id        INT         NOT NULL,
    academic_yr       VARCHAR(11) NOT NULL,
    attendance_date   DATE        NOT NULL,
    attendance_status CHAR(1)     NOT NULL,
    INDEX idx_student_yr (student_id, academic_yr)
);

CREATE TABLE IF NOT EXISTS name_aliases (
    id         INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    parent_id  INT          NOT NULL,
    raw_name   VARCHAR(100) NOT NULL,
    canonical  VARCHAR(100) NOT NULL,
    INDEX idx_parent (parent_id)
);

CREATE TABLE IF NOT EXISTS fact_achievements (
    student_id        INT          NOT NULL,
    academic_yr       VARCHAR(11)  NOT NULL,
    achievement_count INT          NOT NULL DEFAULT 0,
    achievement_list  TEXT,
    PRIMARY KEY (student_id, academic_yr)
);

CREATE TABLE IF NOT EXISTS fact_homework_engagement (
    student_id              INT         NOT NULL,
    academic_yr             VARCHAR(11) NOT NULL,
    homework_assigned_count INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (student_id, academic_yr)
);