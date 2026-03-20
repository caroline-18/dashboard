# dashboard/school_registry.py
"""
Multi-school login support.

Workflow:
  1. get_school_for_user(user_id)  →  queries master DB, returns school row
  2. get_or_register_school_db(school_id, school_row)  →  ensures Django
     knows about this school's DB and returns the alias string
  3. data_loader._get_connection() reads the alias from the session
"""

import logging
import mysql.connector
from django.conf import settings
from django import db as django_db

log = logging.getLogger("school_registry")

# ── Role tables in the master DB ──────────────────────────────────
# Order matters: check most-specific first
_ROLE_TABLES = [
    "teacher_users_schoolwise",
    "staff_users_schoolwise",
    "management_users_schoolwise",
    "feesoftware_users_schoolwise",
    "security_users_schoolwise",
    "user_schoolwise",          # parents / students (largest table)
]


def _master_conn():
    """Open a raw connection to the master Evolvu users DB."""
    cfg = settings.MASTER_DB_CONFIG
    return mysql.connector.connect(
        host=cfg["HOST"],
        port=int(cfg.get("PORT", 3306)),
        user=cfg["USER"],
        password=cfg["PASSWORD"],
        database=cfg["NAME"],
        charset="utf8mb4",
    )


def get_school_for_user(user_id: str):
    if getattr(__import__("django.conf", fromlist=["settings"]).settings, "DEV_SCHOOL_BYPASS", False):
        log.info("DEV_SCHOOL_BYPASS: routing user '%s' → school_id=1", user_id)
        return 1, "user_schoolwise"
    """
    Search every active school's dim_users table for the user_id.
    Returns (school_id, role_table) or (None, None) if not found.
    """
    conn = None
    try:
        conn = _master_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT school_id, analytics_db FROM `school` WHERE is_active = 1"
        )
        schools = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        log.error("Master DB lookup failed: %s", e)
        return None, None

    master = settings.MASTER_DB_CONFIG
    for school in schools:
        try:
            sc = mysql.connector.connect(
                host=master["HOST"],
                port=int(master.get("PORT", 3306)),
                user=master["USER"],
                password=master["PASSWORD"],
                database=school["analytics_db"],
                charset="utf8mb4",
            )
            cur = sc.cursor(dictionary=True)
            cur.execute(
                "SELECT user_id FROM dim_users WHERE LOWER(user_id) = LOWER(%s) LIMIT 1",
                (user_id,),
            )
            row = cur.fetchone()
            cur.close()
            sc.close()
            if row:
                log.info("User '%s' found in school_id=%s (%s)",
                         user_id, school["school_id"], school["analytics_db"])
                return int(school["school_id"]), "dim_users"
        except Exception as e:
            log.debug("Could not search school_id=%s: %s", school["school_id"], e)

    return None, None


def get_school_row(school_id: int):
    """
    Fetch the school record from master DB `school` table.
    Returns dict with keys: school_id, name, short_name, url,
    project_url, laravel_project_url, default_password etc.
    """
    conn = None
    try:
        conn = _master_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM `school` WHERE school_id = %s LIMIT 1",
            (school_id,),
        )
        return cur.fetchone()
    except Exception as e:
        log.error("school row lookup failed for school_id=%s: %s", school_id, e)
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_or_register_school_db(school_id: int) -> str:
    """
    Return the Django DB alias for this school_id, registering it
    dynamically if not already present.
    """
    # 1 — Check hard-coded alias map first
    alias_map = getattr(settings, "SCHOOL_DB_ALIAS_MAP", {})
    if school_id in alias_map:
        alias = alias_map[school_id]
        if alias in settings.DATABASES:
            return alias

    # 2 — Check if already registered dynamically
    alias = f"school_{school_id}_db"
    if alias in settings.DATABASES:
        return alias

    # 3 — Fetch school row to get the correct analytics_db name
    school = get_school_row(school_id)
    if not school:
        raise ValueError(f"Unknown school_id: {school_id}")

    # 4 — Use analytics_db directly from the school table
    db_name = school["analytics_db"]  # e.g. "arnolds1_analytics"

    master = settings.MASTER_DB_CONFIG
    new_db_config = {
        "ENGINE":   "django.db.backends.mysql",
        "NAME":     db_name,
        "USER":     master["USER"],
        "PASSWORD": master["PASSWORD"],
        "HOST":     master["HOST"],
        "PORT":     master.get("PORT", "3306"),
        "OPTIONS":  {"charset": "utf8mb4"},
    }

    # 5 — Register dynamically
    settings.DATABASES[alias] = new_db_config
    django_db.connections.databases[alias] = new_db_config

    log.info("Registered DB alias '%s' → %s (school_id=%s)", alias, db_name, school_id)
    return alias