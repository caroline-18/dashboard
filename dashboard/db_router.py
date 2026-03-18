# dashboard/db_router.py

class SchoolDatabaseRouter:
    def _is_school_db(self, db):
        from django.conf import settings
        # Any DB that isn't 'default' and isn't sqlite is a school DB
        return (
            db != "default"
            and settings.DATABASES.get(db, {}).get("ENGINE", "")
            != "django.db.backends.sqlite3"
        )

    def db_for_read(self, model, **hints):
        return None

    def db_for_write(self, model, **hints):
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if self._is_school_db(db):
            return False
        return True