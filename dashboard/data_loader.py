# ============================================================
#  data_loader.py  —  ACEVENTURA_AI  (Django version)
#  ETL : etl/etl_arnolds1_analytics_incremental.sql
#  DB  : arnolds1  →  arnolds1_analytics
#  All reads go through this module only.
# ============================================================

import threading
_thread_locals = threading.local()

def set_active_school_db(alias: str):
    """Called at login to set the school DB for this thread/request."""
    _thread_locals.school_db_alias = alias

def get_active_school_db() -> str:
    return getattr(_thread_locals, "school_db_alias", None)

import logging
import warnings
from functools import wraps
from typing import Optional

import mysql.connector
import pandas as pd
from django.conf import settings
from django.core.cache import cache

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
)
pd.set_option("future.no_silent_downcasting", True)

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s  —  %(message)s",
)
log = logging.getLogger("data_loader")

def _get_school_db_names() -> tuple[str, str]:
    alias = get_active_school_db() or "arnolds_db"
    db_cfg = settings.DATABASES.get(alias, {})
    analytics_db = db_cfg.get("NAME", "arnolds1_analytics")
    # Read source DB from settings instead of guessing by stripping "_analytics"
    source_db = db_cfg.get("SOURCE_DB", analytics_db.replace("_analytics", ""))
    return source_db, analytics_db

# ============================================================
#  DB CONFIG  —  reads from Django settings.py
# ============================================================

# def _get_db_config() -> dict:
#     db = settings.DATABASES["arnolds_db"]
#     return {
#         "host":     db["HOST"],
#         "user":     db["USER"],
#         "password": db["PASSWORD"],
#         "database": db["NAME"],
#         "port":     int(db.get("PORT", 3306)),
#     }


# ============================================================
#  CONNECTION + QUERY HELPER
# ============================================================

def _get_connection():
    from django.conf import settings

    alias = get_active_school_db()
    if not alias:
        # fallback to first school DB for backward-compat during dev
        alias = "arnolds_db"

    db_cfg = settings.DATABASES.get(alias)
    if not db_cfg:
        raise RuntimeError(f"No DB config found for alias '{alias}'")

    return mysql.connector.connect(
        host=db_cfg["HOST"],
        port=int(db_cfg.get("PORT", 3306)),
        user=db_cfg["USER"],
        password=db_cfg["PASSWORD"],
        database=db_cfg["NAME"],
        charset="utf8mb4",
    )


def _read_sql(query: str, params=None) -> pd.DataFrame:
    """Execute SELECT, return DataFrame."""
    if params:
        params = tuple(
            p.item() if hasattr(p, "item") else p
            for p in params
        )
    conn = _get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(query, params or ())
        rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        conn.close()


# ============================================================
#  VALIDATION CONSTANTS
# ============================================================

_REQUIRED_TABLES = {
    "student_clean":                    ["student_id", "academic_yr"],
    "dim_student_demographics":         ["student_id", "academic_yr", "student_name", "parent_id"],
    "dim_class":                        ["class_id", "class_name"],
    "dim_section":                      ["section_id", "section_name"],
    "dim_parent":                       ["parent_id", "father_name", "mother_name"],
    "dim_subject":                      ["subject_id", "subject_name"],
    "dim_users":                        ["user_id", "name", "password", "reg_id", "role_id"],
    "dim_teachers":                     ["teacher_id", "name", "designation", "class_id", "section_id"],
    "student_master_profile":           ["student_id", "academic_yr", "avg_percent"],
    "fact_attendance":                  ["student_id", "academic_yr", "attendance_percentage"],
    "fact_academics":                   ["student_id", "academic_yr", "avg_percent"],
    "fact_achievements":                ["student_id", "academic_yr", "achievement_count"],
    "fact_homework_engagement":         ["student_id", "academic_yr", "homework_assigned_count"],
    "fact_student_subject_performance": ["student_id", "academic_yr", "subject_id", "avg_percent"],
    "fact_science_components":          ["student_id", "academic_yr", "component", "is_entered"],
    "student_behavior_signals":         ["student_id", "academic_yr", "attendance_band"],
}

_REQUIRED_VIEWS = [
    "student_subject_strengths",
    "student_ai_profile",
    "child_selector",
]

_VALID_YEARS = [
    "2019-2020", "2020-2021", "2021-2022",
    "2022-2023", "2023-2024", "2024-2025", "2025-2026",
]

_BLOAT_SUBJECTS = [
    "New subject Regular", "new subject common code test",
    "SUBJECT_TEST2", "SubjectForRCTest",
    "New RC subject", "New sub for RC", "test",
]

# Role codes as defined in the ETL (arnolds1.roles → dim_users.role_id)
ROLE_PARENT    = "P"
ROLE_TEACHER   = "T"
ROLE_PRINCIPAL = "M"

_VALID_ROLES = {ROLE_PARENT, ROLE_TEACHER, ROLE_PRINCIPAL}


# ============================================================
#  SCHEMA VALIDATION
# ============================================================

def validate_schema() -> dict:
    errors, warnings_list, info = [], [], []
    conn = None
    try:
        conn = _get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT TABLE_NAME FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
        """)
        existing_tables = {r["TABLE_NAME"] for r in cur.fetchall()}
        for t in _REQUIRED_TABLES:
            if t not in existing_tables:
                errors.append(f"Missing table: {t}")

        cur.execute("""
            SELECT TABLE_NAME FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
        """)
        existing_views = {r["TABLE_NAME"] for r in cur.fetchall()}
        for v in _REQUIRED_VIEWS:
            if v not in existing_views:
                errors.append(f"Missing view: {v}")

        cur.execute("""
            SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
        """)
        cols_by_table: dict[str, set] = {}
        for r in cur.fetchall():
            cols_by_table.setdefault(r["TABLE_NAME"], set()).add(r["COLUMN_NAME"])

        for table, req_cols in _REQUIRED_TABLES.items():
            if table not in existing_tables:
                continue
            have = cols_by_table.get(table, set())
            for col in req_cols:
                if col not in have:
                    errors.append(f"Missing column: {table}.{col}")

        if "dim_subject" in existing_tables:
            ph = ", ".join(["%s"] * len(_BLOAT_SUBJECTS))
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM dim_subject WHERE subject_name IN ({ph})",
                _BLOAT_SUBJECTS,
            )
            cnt = cur.fetchone()["cnt"]
            if cnt > 0:
                errors.append(f"dim_subject: {cnt} test/bloat subject(s) found. Re-run ETL Step 5b.")

        if "dim_subject" in existing_tables:
            cur.execute("SELECT COUNT(*) AS cnt FROM dim_subject WHERE subject_id = 24")
            cnt = cur.fetchone()["cnt"]
            if cnt == 0:
                errors.append("dim_subject: subject_id 24 (Science) is missing.")
            elif cnt > 1:
                errors.append(f"dim_subject: subject_id 24 has {cnt} duplicates. Re-run ETL Step 5b.")
            else:
                info.append("dim_subject: subject_id 24 (Science) — 1 row ✅")

        if "dim_users" in existing_tables:
            cur.execute("""
                SELECT role_id, COUNT(*) AS cnt
                FROM dim_users
                GROUP BY role_id
                ORDER BY role_id
            """)
            for row in cur.fetchall():
                role_label = {"P": "Parent", "T": "Teacher", "M": "Principal"}.get(row["role_id"], row["role_id"])
                info.append(f"dim_users: {row['cnt']:,} {role_label} account(s) ✅")

        if "dim_teachers" in existing_tables:
            cur.execute("SELECT COUNT(*) AS cnt FROM dim_teachers")
            cnt = cur.fetchone()["cnt"]
            if cnt == 0:
                warnings_list.append("dim_teachers is empty. Re-run ETL Step 2b.")
            else:
                info.append(f"dim_teachers: {cnt:,} teacher record(s) ✅")

        if "etl_run_log" in existing_tables:
            cur.execute("SELECT COUNT(*) AS cnt FROM etl_run_log WHERE status = 'SUCCESS'")
            cnt = cur.fetchone()["cnt"]
            if cnt == 0:
                errors.append("etl_run_log: no SUCCESS entries. ETL may not have completed.")
            else:
                etl_cols = cols_by_table.get("etl_run_log", set())
                ts_col   = next(
                    (c for c in ["run_ts", "created_at", "run_date", "timestamp"] if c in etl_cols),
                    None,
                )
                select_str = f"{ts_col}, notes" if ts_col else "notes"
                order_str  = ts_col if ts_col else "notes"
                cur.execute(
                    f"SELECT {select_str} FROM etl_run_log "
                    f"WHERE status = 'SUCCESS' ORDER BY {order_str} DESC LIMIT 1"
                )
                row = cur.fetchone()
                ts  = row.get(ts_col, "unknown") if ts_col else "unknown"
                info.append(f"Last ETL run: {ts} — {row.get('notes', '')}")
        else:
            warnings_list.append("etl_run_log not found — cannot confirm ETL history.")

        if "student_master_profile" in existing_tables:
            cur.execute("SELECT COUNT(DISTINCT student_id) AS cnt FROM student_master_profile")
            cnt = cur.fetchone()["cnt"]
            if cnt == 0:
                errors.append("student_master_profile is empty. ETL has not loaded any data.")
            else:
                info.append(f"student_master_profile: {cnt:,} unique students ✅")

        if "fact_science_components" in existing_tables:
            if "is_entered" not in cols_by_table.get("fact_science_components", set()):
                errors.append("fact_science_components.is_entered missing. Re-run ETL Step 6.")

        if "student_master_profile" in existing_tables:
            cur.execute("""
                SELECT academic_yr, COUNT(DISTINCT student_id) AS cnt
                FROM student_master_profile
                WHERE academic_yr != 'ALL'
                GROUP BY academic_yr ORDER BY academic_yr
            """)
            for row in cur.fetchall():
                yr, cnt = row["academic_yr"], row["cnt"]
                if cnt == 0:
                    warnings_list.append(f"{yr}: 0 students in master profile.")
                else:
                    info.append(f"{yr}: {cnt:,} students ✅")

    except mysql.connector.Error as e:
        errors.append(f"Database connection failed: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings_list, "info": info}


# ============================================================
#  INPUT VALIDATORS
# ============================================================

def validate_year(academic_yr: str) -> bool:
    return academic_yr in _VALID_YEARS


def validate_student_id(student_id) -> bool:
    try:
        return int(student_id) > 0
    except (TypeError, ValueError):
        return False


def validate_user_id(user_id) -> bool:
    """user_id is a VARCHAR(50) login credential — must be a non-empty string."""
    return bool(user_id and str(user_id).strip())


# ============================================================
#  SAFE LOAD DECORATOR  —  no st.error, returns empty DataFrame
# ============================================================

def _safe_load(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                log.warning("%s returned empty result (args=%s)", func.__name__, args)
            return result
        except mysql.connector.Error as e:
            log.error("%s — DB error: %s", func.__name__, e)
            return pd.DataFrame()
        except ValueError as e:
            log.error("%s — validation: %s", func.__name__, e)
            return pd.DataFrame()
        except Exception as e:
            log.error("%s — unexpected: %s", func.__name__, e, exc_info=True)
            return pd.DataFrame()
    return wrapper


# ============================================================
#  DJANGO CACHE HELPER  —  replaces @st.cache_data(ttl=300)
# ============================================================

CACHE_TTL = 300  # seconds


def _cached(key: str, loader_fn, *args):
    """
    Generic cache wrapper using Django's cache framework.
    Replace st.cache_data with this pattern in each loader.
    """
    result = cache.get(key)
    if result is None:
        result = loader_fn(*args)
        cache.set(key, result, CACHE_TTL)
    return result


# ============================================================
#  LOGIN / AUTHENTICATION  —  reads from dim_users (ETL Step 2c)
#
#  dim_users is a full TRUNCATE + INSERT on every ETL run so
#  password resets and IsDelete changes from arnolds1.user_master
#  are always reflected.
#
#  Role codes (role_id):
#    P  →  Parent      (child profile view)
#    T  →  Teacher     (class & student management)
#    M  →  Principal   (school-wide overview)
#
#  authenticate_user()  — primary login check; returns user dict or None
#  load_user_by_id()    — re-fetches a user after session resume
# ============================================================

def authenticate_user(user_id: str, password: str) -> Optional[dict]:
    """
    Validate login credentials against dim_users.

    Looks up user_id (case-insensitive) and compares the plain-text
    password stored by the ETL from arnolds1.user_master.

    Returns a dict with keys:
        user_id, name, role_id, role_name, reg_id
    or None on failure (wrong credentials / user not found).

    NOTE: Passwords in dim_users are plain-text VARCHAR(20) as stored
    by the source school ERP.  No hashing is applied here — mirror the
    source exactly.  If you introduce hashing, update the ETL INSERT
    and this comparison together.
    """
    if not validate_user_id(user_id):
        log.warning("authenticate_user: blank user_id supplied")
        return None
    if not password:
        log.warning("authenticate_user: blank password for user_id=%s", user_id)
        return None

    try:
        df = _read_sql(
            """
            SELECT user_id, name, password, reg_id, role_id
            FROM dim_users
            WHERE LOWER(user_id) = LOWER(%s)
            LIMIT 1
            """,
            (str(user_id).strip(),),
        )
    except Exception as e:
        log.error("authenticate_user — DB error: %s", e)
        return None

    if df.empty:
        log.info("authenticate_user: user_id='%s' not found", user_id)
        return None

    row = df.iloc[0]

    # Plain-text comparison — matches ETL source (arnolds1.user_master.password)
    if str(row["password"]) != str(password):
        log.info("authenticate_user: wrong password for user_id='%s'", user_id)
        return None

    role_id = str(row["role_id"]).strip().upper()
    if role_id not in _VALID_ROLES:
        log.warning(
            "authenticate_user: user_id='%s' has unexpected role_id='%s'",
            user_id, role_id,
        )
        return None

    role_name_map = {ROLE_PARENT: "Parent", ROLE_TEACHER: "Teacher", ROLE_PRINCIPAL: "Principal"}

    user = {
        "user_id":   str(row["user_id"]),
        "name":      str(row["name"]),
        "role_id":   role_id,
        "role_name": role_name_map[role_id],
        "reg_id":    int(row["reg_id"]),   # parent_id for P, teacher_id for T/M
    }
    log.info(
        "authenticate_user: SUCCESS user_id='%s' role=%s reg_id=%d",
        user["user_id"], user["role_id"], user["reg_id"],
    )
    return user


def load_user_by_id(user_id: str) -> Optional[dict]:
    """
    Re-fetch a user record from dim_users by user_id alone.

    Used to rebuild session state after a page reload — the caller
    already authenticated; this just refreshes the user dict in case
    the ETL has re-run and updated the name or role since login.

    Returns the same dict shape as authenticate_user(), or None if
    the user_id no longer exists in dim_users (e.g. account deleted
    and ETL re-run).
    """
    if not validate_user_id(user_id):
        log.warning("load_user_by_id: blank user_id supplied")
        return None

    try:
        df = _read_sql(
            """
            SELECT user_id, name, reg_id, role_id
            FROM dim_users
            WHERE LOWER(user_id) = LOWER(%s)
            LIMIT 1
            """,
            (str(user_id).strip(),),
        )
    except Exception as e:
        log.error("load_user_by_id — DB error: %s", e)
        return None

    if df.empty:
        log.info("load_user_by_id: user_id='%s' not found", user_id)
        return None

    row     = df.iloc[0]
    role_id = str(row["role_id"]).strip().upper()
    if role_id not in _VALID_ROLES:
        log.warning("load_user_by_id: unexpected role_id='%s' for user_id='%s'", role_id, user_id)
        return None

    role_name_map = {ROLE_PARENT: "Parent", ROLE_TEACHER: "Teacher", ROLE_PRINCIPAL: "Principal"}
    return {
        "user_id":   str(row["user_id"]),
        "name":      str(row["name"]),
        "role_id":   role_id,
        "role_name": role_name_map[role_id],
        "reg_id":    int(row["reg_id"]),
    }


# ============================================================
#  LOAD FUNCTIONS
# ============================================================

@_safe_load
def _load_student_profile_raw(academic_yr: Optional[str] = None) -> pd.DataFrame:
    if academic_yr is not None and not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr '{academic_yr}'.")

    year_clause = "AND d.academic_yr = %s" if academic_yr else ""
    params      = (academic_yr,) if academic_yr else None

    query = f"""
        SELECT
            d.student_id,
            CASE
                WHEN TRIM(d.student_name) LIKE '%% %%'
                    THEN TRIM(d.student_name)
                WHEN p.father_name IS NOT NULL AND TRIM(p.father_name) != ''
                    THEN CONCAT(TRIM(d.student_name), ' ',
                                SUBSTRING_INDEX(TRIM(p.father_name), ' ', 1))
                ELSE TRIM(d.student_name)
            END                              AS student_name,
            d.gender, d.dob, d.parent_id,
            d.guardian_name, d.guardian_mobile, d.academic_yr,
            dc.class_name, ds.section_name,
            p.father_name, p.mother_name, p.father_contact, p.mother_contact,
            sm.avg_percent, sm.written_avg, sm.oral_avg, sm.exams_taken,
            sm.attendance_percentage, sm.homework_assigned_count,
            ss.strong_subjects, ss.strong_subjects_ai,
            bs.attendance_band, bs.learning_style,
            bs.engagement_pattern, bs.primary_strength_axis,
            fa_year.achievement_count        AS year_achievement_count,
            fa_year.achievement_list         AS year_achievement_list,
            fa_all.achievement_count         AS achievement_count,
            fa_all.achievement_list          AS achievement_list
        FROM dim_student_demographics d
        LEFT JOIN dim_class              dc  ON dc.class_id    = d.class_id
        LEFT JOIN dim_section            ds  ON ds.section_id  = d.section_id
        LEFT JOIN dim_parent             p   ON p.parent_id    = d.parent_id
        LEFT JOIN student_master_profile sm  ON sm.student_id  = d.student_id
                                           AND sm.academic_yr  = d.academic_yr
        LEFT JOIN student_subject_strengths ss
                                            ON ss.student_id  = d.student_id
                                           AND ss.academic_yr  = d.academic_yr
        LEFT JOIN student_behavior_signals  bs
                                            ON bs.student_id  = d.student_id
                                           AND bs.academic_yr  = d.academic_yr
        LEFT JOIN fact_achievements fa_year ON fa_year.student_id  = d.student_id
                                          AND fa_year.academic_yr  = d.academic_yr
        LEFT JOIN fact_achievements fa_all  ON fa_all.student_id   = d.student_id
                                          AND fa_all.academic_yr   = 'ALL'
        WHERE d.student_name IS NOT NULL
          AND TRIM(d.student_name) != ''
          AND d.student_name NOT REGEXP '^[0-9]+$'
          {year_clause}
        ORDER BY d.academic_yr DESC, d.student_name ASC
    """
    df = _read_sql(query, params)
    if not df.empty:
        for col in ("year_achievement_count", "achievement_count"):
            df[col] = df[col].fillna(0).infer_objects(copy=False).astype(int)
        for col in ("year_achievement_list", "achievement_list"):
            df[col] = df[col].fillna("")
    log.info("load_student_profile: %d rows (year=%s)", len(df), academic_yr or "ALL")
    return df


def load_student_profile(academic_yr: Optional[str] = None) -> pd.DataFrame:
    key = f"student_profile_{academic_yr or 'ALL'}"
    return _cached(key, _load_student_profile_raw, academic_yr)


@_safe_load
def _load_child_selector_raw(parent_id: int) -> pd.DataFrame:
    if not validate_student_id(parent_id):
        raise ValueError(f"Invalid parent_id: {parent_id}")
    df = _read_sql(
        "SELECT parent_id, display_name, latest_student_id, latest_academic_yr "
        "FROM child_selector WHERE parent_id = %s ORDER BY display_name ASC",
        (parent_id,),
    )
    log.info("load_child_selector: parent_id=%d → %d child(ren)", parent_id, len(df))
    return df


def load_child_selector(parent_id: int) -> pd.DataFrame:
    return _cached(f"child_selector_{parent_id}", _load_child_selector_raw, parent_id)


@_safe_load
def _load_student_subjects_raw(student_id: int, academic_yr: str) -> pd.DataFrame:
    if not validate_student_id(student_id):
        raise ValueError(f"Invalid student_id: {student_id}")
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")

    df = _read_sql("""
        SELECT ds.subject_name, fsp.avg_percent, fsp.written_avg, fsp.oral_avg
        FROM fact_student_subject_performance fsp
        JOIN dim_subject ds ON ds.subject_id = fsp.subject_id
        WHERE fsp.student_id = %s AND fsp.academic_yr = %s
          AND fsp.avg_percent IS NOT NULL
        ORDER BY fsp.avg_percent DESC
    """, (student_id, academic_yr))

    df_sci = _read_sql("""
        SELECT component, avg_percent, COALESCE(is_entered, 1) AS is_entered
        FROM fact_science_components
        WHERE student_id = %s AND academic_yr = %s
        ORDER BY is_entered DESC, avg_percent DESC
    """, (student_id, academic_yr))

    df["science_components"] = None
    if not df.empty and not df_sci.empty:
        parts = [
            f"{r['component']} (not assessed)" if int(r["is_entered"]) == 0
            else r["component"]
            for _, r in df_sci.iterrows()
        ]
        df.loc[df["subject_name"] == "Science", "science_components"] = ", ".join(parts)

    log.info("load_student_subjects: student_id=%d year=%s → %d subject(s)", student_id, academic_yr, len(df))
    return df


def load_student_subjects(student_id: int, academic_yr: str) -> pd.DataFrame:
    return _cached(f"student_subjects_{student_id}_{academic_yr}", _load_student_subjects_raw, student_id, academic_yr)


@_safe_load
def _load_subject_performance_raw(student_id: int, academic_yr: str) -> pd.DataFrame:
    if not validate_student_id(student_id):
        raise ValueError(f"Invalid student_id: {student_id}")
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")

    df = _read_sql("""
        SELECT
            ds.subject_name,
            fsp.avg_percent,
            fsp.written_avg,
            fsp.oral_avg,
            cls.class_avg_percent
        FROM fact_student_subject_performance fsp
        JOIN dim_subject ds ON ds.subject_id = fsp.subject_id
        LEFT JOIN (
            SELECT
                fspc.subject_id,
                ROUND(AVG(fspc.avg_percent), 2) AS class_avg_percent
            FROM fact_student_subject_performance fspc
            JOIN student_master_profile smp
                ON  smp.student_id  = fspc.student_id
                AND smp.academic_yr = fspc.academic_yr
            WHERE smp.class_id = (
                      SELECT class_id FROM student_master_profile
                      WHERE student_id = %s AND academic_yr = %s LIMIT 1
                  )
              AND smp.section_id = (
                      SELECT section_id FROM student_master_profile
                      WHERE student_id = %s AND academic_yr = %s LIMIT 1
                  )
              AND fspc.academic_yr = %s
            GROUP BY fspc.subject_id
        ) cls ON cls.subject_id = fsp.subject_id
        WHERE fsp.student_id  = %s
          AND fsp.academic_yr = %s
        ORDER BY fsp.avg_percent DESC
    """, (student_id, academic_yr, student_id, academic_yr, academic_yr, student_id, academic_yr))

    log.info("load_subject_performance: student_id=%d year=%s → %d subject(s)", student_id, academic_yr, len(df))
    return df


def load_subject_performance(student_id: int, academic_yr: str) -> pd.DataFrame:
    return _cached(f"subject_perf_{student_id}_{academic_yr}", _load_subject_performance_raw, student_id, academic_yr)


@_safe_load
def _load_subject_performance_all_raw(academic_yr: str) -> pd.DataFrame:
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")
    df = _read_sql("""
        SELECT fsp.student_id, fsp.academic_yr, ds.subject_name,
               fsp.avg_percent, fsp.written_avg, fsp.oral_avg, fsp.exams_taken
        FROM fact_student_subject_performance fsp
        JOIN dim_subject ds ON ds.subject_id = fsp.subject_id
        WHERE fsp.academic_yr = %s
        ORDER BY fsp.student_id, ds.subject_name
    """, (academic_yr,))
    log.info("load_subject_performance_all: year=%s → %d rows", academic_yr, len(df))
    return df


def load_subject_performance_all(academic_yr: str) -> pd.DataFrame:
    return _cached(f"subject_perf_all_{academic_yr}", _load_subject_performance_all_raw, academic_yr)


@_safe_load
def _load_exam_breakdown_raw(student_id: int, academic_yr: str) -> pd.DataFrame:
    if not validate_student_id(student_id):
        raise ValueError(f"Invalid student_id: {student_id}")
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")

    source_db, analytics_db = _get_school_db_names()   # ← dynamic

    df = _read_sql(f"""
        SELECT
            mh.name                         AS exam_type,
            e.exam_id                       AS exam_id,
            e.name                          AS exam_name,
            CASE
                WHEN c.name IN ('6','7','8','9','10')
                 AND smc.subject_id IN (15, 16, 17)
                THEN CASE smc.subject_id
                        WHEN 15 THEN 'Physics'
                        WHEN 16 THEN 'Chemistry'
                        WHEN 17 THEN 'Biology'
                     END
                ELSE ds.subject_name
            END                             AS subject_name,
            SUM(smc.marks_obtained)         AS marks_obtained,
            SUM(smc.max_marks)              AS max_marks
        FROM `{source_db}`.student_marks_components smc
        JOIN `{source_db}`.marks_headings mh
            ON mh.marks_headings_id = smc.marks_headings_id
        JOIN `{source_db}`.exam e
            ON  e.exam_id     = smc.exam_id
            AND e.academic_yr = smc.academic_yr
        JOIN `{source_db}`.student st
            ON  st.student_id  = smc.student_id
            AND st.academic_yr = smc.academic_yr
        JOIN `{source_db}`.class c
            ON  c.class_id    = st.class_id
            AND c.academic_yr = st.academic_yr
        JOIN `{source_db}`.subject_master sm2
            ON sm2.sm_id = smc.subject_id
        JOIN `{analytics_db}`.dim_subject ds
            ON ds.subject_id = CASE
                WHEN c.name IN ('6','7','8','9','10') AND smc.subject_id IN (15,16,17) THEN 24
                ELSE smc.subject_id
               END
        WHERE smc.student_id  = %s
          AND smc.academic_yr = %s
          AND smc.is_present  = 'Y'
          AND st.IsDelete     = 'N'
        GROUP BY mh.name, e.exam_id, e.name, c.name, smc.subject_id, ds.subject_name
        ORDER BY mh.name, e.name, subject_name
    """, (student_id, academic_yr))

    log.info("load_exam_breakdown: student_id=%d year=%s → %d rows", student_id, academic_yr, len(df))
    return df

def load_exam_breakdown(student_id: int, academic_yr: str) -> pd.DataFrame:
    return _cached(f"exam_breakdown_{student_id}_{academic_yr}", _load_exam_breakdown_raw, student_id, academic_yr)

@_safe_load
def _load_available_years_raw() -> list:
    df = _read_sql(
        "SELECT DISTINCT academic_yr FROM student_master_profile "
        "WHERE academic_yr != 'ALL' ORDER BY academic_yr DESC"
    )
    years = df["academic_yr"].tolist() if not df.empty else []
    log.info("load_available_years: %s", years)
    return years


def load_available_years() -> list:
    return _cached("available_years", _load_available_years_raw)


@_safe_load
def _load_available_classes_raw(academic_yr: str) -> list:
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")
    df = _read_sql(
        "SELECT DISTINCT dc.class_name "
        "FROM dim_student_demographics d "
        "JOIN dim_class dc ON dc.class_id = d.class_id "
        "WHERE d.academic_yr = %s ORDER BY dc.class_name",
        (academic_yr,),
    )
    return df["class_name"].tolist() if not df.empty else []


def load_available_classes(academic_yr: str) -> list:
    return _cached(f"available_classes_{academic_yr}", _load_available_classes_raw, academic_yr)


@_safe_load
def _load_class_summary_raw(academic_yr: str, class_name: str) -> pd.DataFrame:
    if not validate_year(academic_yr):
        raise ValueError(f"Invalid academic_yr: {academic_yr}")
    if not str(class_name).strip():
        raise ValueError("class_name cannot be empty")

    df = _read_sql("""
        SELECT
            d.student_id,
            CASE
                WHEN TRIM(d.student_name) LIKE '%% %%'
                    THEN TRIM(d.student_name)
                WHEN p.father_name IS NOT NULL AND TRIM(p.father_name) != ''
                    THEN CONCAT(TRIM(d.student_name), ' ',
                                SUBSTRING_INDEX(TRIM(p.father_name), ' ', 1))
                ELSE TRIM(d.student_name)
            END                          AS student_name,
            ds.section_name,
            sm.avg_percent, sm.attendance_percentage,
            sm.achievements, sm.homework_assigned_count,
            bs.attendance_band, bs.learning_style, bs.primary_strength_axis,
            ss.strong_subjects
        FROM dim_student_demographics d
        JOIN  dim_class dc ON dc.class_id = d.class_id AND dc.class_name = %s
        LEFT JOIN dim_section            ds  ON ds.section_id  = d.section_id
        LEFT JOIN dim_parent             p   ON p.parent_id    = d.parent_id
        LEFT JOIN student_master_profile sm  ON sm.student_id  = d.student_id
                                           AND sm.academic_yr  = d.academic_yr
        LEFT JOIN student_behavior_signals bs ON bs.student_id  = d.student_id
                                            AND bs.academic_yr  = d.academic_yr
        LEFT JOIN student_subject_strengths ss ON ss.student_id  = d.student_id
                                             AND ss.academic_yr  = d.academic_yr
        WHERE d.academic_yr = %s
          AND d.student_name IS NOT NULL AND TRIM(d.student_name) != ''
        ORDER BY ds.section_name, student_name
    """, (class_name, academic_yr))

    log.info("load_class_summary: class=%s year=%s → %d students", class_name, academic_yr, len(df))
    return df


def load_class_summary(academic_yr: str, class_name: str) -> pd.DataFrame:
    return _cached(f"class_summary_{academic_yr}_{class_name}", _load_class_summary_raw, academic_yr, class_name)


# ============================================================
#  WRITE FUNCTIONS  —  unchanged from Streamlit version
# ============================================================

def save_extracurricular_achievement(payload: dict) -> bool:
    sql = """
        INSERT INTO extracurricular_achievements (
            student_id, title, category, achievement_date, level,
            position, description, certificate_filename,
            certificate_data, certificate_mime, submitted_by_parent_id
        ) VALUES (
            %(student_id)s, %(title)s, %(category)s, %(achievement_date)s, %(level)s,
            %(position)s, %(description)s, %(certificate_filename)s,
            %(certificate_data)s, %(certificate_mime)s, %(submitted_by_parent_id)s
        )
    """
    conn = cur = None
    try:
        conn = _get_connection()
        cur  = conn.cursor()
        cur.execute(sql, payload)
        conn.commit()
        log.info("save_extracurricular_achievement: saved for student_id=%s", payload.get("student_id"))
        return True
    except mysql.connector.Error as e:
        log.error("save_extracurricular_achievement — DB error: %s", e)
        return False
    except Exception as e:
        log.error("save_extracurricular_achievement — unexpected: %s", e, exc_info=True)
        return False
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def update_extracurricular_achievement(payload: dict) -> bool:
    record_id = payload.get("id")
    if not record_id or int(record_id) <= 0:
        log.warning("update_extracurricular_achievement: invalid id=%s", record_id)
        return False

    conn = cur = None
    try:
        conn = _get_connection()
        cur  = conn.cursor()
        if payload.get("certificate_data") is not None:
            cur.execute("""
                UPDATE extracurricular_achievements SET
                    title                = %(title)s,
                    category             = %(category)s,
                    achievement_date     = %(achievement_date)s,
                    level                = %(level)s,
                    position             = %(position)s,
                    description          = %(description)s,
                    certificate_filename = %(certificate_filename)s,
                    certificate_data     = %(certificate_data)s,
                    certificate_mime     = %(certificate_mime)s
                WHERE id = %(id)s
            """, payload)
        else:
            cur.execute("""
                UPDATE extracurricular_achievements SET
                    title            = %(title)s,
                    category         = %(category)s,
                    achievement_date = %(achievement_date)s,
                    level            = %(level)s,
                    position         = %(position)s,
                    description      = %(description)s
                WHERE id = %(id)s
            """, payload)
        conn.commit()
        if cur.rowcount == 0:
            log.warning("update_extracurricular_achievement: id=%d not found", record_id)
            return False
        log.info("update_extracurricular_achievement: updated id=%d", record_id)
        return True
    except mysql.connector.Error as e:
        log.error("update_extracurricular_achievement — DB error: %s", e)
        return False
    except Exception as e:
        log.error("update_extracurricular_achievement — unexpected: %s", e, exc_info=True)
        return False
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def delete_extracurricular_achievement(record_id: int) -> bool:
    if not record_id or int(record_id) <= 0:
        log.warning("delete_extracurricular_achievement: invalid record_id=%s", record_id)
        return False

    conn = cur = None
    try:
        conn = _get_connection()
        cur  = conn.cursor()
        cur.execute("DELETE FROM extracurricular_achievements WHERE id = %s", (int(record_id),))
        conn.commit()
        if cur.rowcount == 0:
            log.warning("delete_extracurricular_achievement: record_id=%d not found", record_id)
            return False
        log.info("delete_extracurricular_achievement: deleted record_id=%d", record_id)
        return True
    except mysql.connector.Error as e:
        log.error("delete_extracurricular_achievement — DB error: %s", e)
        return False
    except Exception as e:
        log.error("delete_extracurricular_achievement — unexpected: %s", e, exc_info=True)
        return False
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass