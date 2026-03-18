-- ================================================================
-- Run against: u333015459_EvolvuUsrsTest
-- Purpose: school registry + master ETL run log
-- ================================================================

USE u333015459_EvolvuUsrsTest;

CREATE TABLE IF NOT EXISTS school (
    school_id        INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name             VARCHAR(255) NOT NULL,
    short_name       VARCHAR(50),
    source_db        VARCHAR(100) NOT NULL  COMMENT 'e.g. arnolds_live',
    analytics_db     VARCHAR(100) NOT NULL  COMMENT 'e.g. arnolds1_analytics',
    default_password VARCHAR(100),
    is_active        TINYINT(1)   NOT NULL DEFAULT 1,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One row per school per ETL run
CREATE TABLE IF NOT EXISTS etl_master_run_log (
    log_id        BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    school_id     INT         NOT NULL,
    school_name   VARCHAR(255),
    source_db     VARCHAR(100),
    analytics_db  VARCHAR(100),
    run_start     DATETIME    NOT NULL,
    run_end       DATETIME,
    status        VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_raw      INT         DEFAULT 0,
    rows_components INT       DEFAULT 0,
    rows_facts    INT         DEFAULT 0,
    error_msg     TEXT,
    INDEX idx_school (school_id),
    INDEX idx_run_start (run_start)
);

-- Seed known schools
INSERT INTO school
    (school_id, name, short_name, source_db, analytics_db, default_password, is_active)
VALUES
    (1, 'Evolvu Smart School',        'SACS', 'arnolds_live', 'arnolds1_analytics', 'arnolds', 1),
    (7, 'Holy Spirit Convent School', 'HSCS', 'hscs_live',    'hscs_analytics',     'hscs',    1)
ON DUPLICATE KEY UPDATE
    source_db    = VALUES(source_db),
    analytics_db = VALUES(analytics_db),
    is_active    = VALUES(is_active);