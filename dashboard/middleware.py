# dashboard/middleware.py

class SchoolDBMiddleware:
    """
    Restores the active school DB alias from the session on every request,
    so _get_connection() always points to the right school.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        alias = request.session.get("school_db_alias")
        if alias:
            from dashboard.data_loader import set_active_school_db
            from dashboard.school_registry import get_or_register_school_db
            from django.conf import settings
            # Re-register if the process restarted and lost the dynamic alias
            if alias not in settings.DATABASES:
                school_id = request.session.get("school_id")
                if school_id:
                    try:
                        get_or_register_school_db(int(school_id))
                    except Exception:
                        pass
            set_active_school_db(alias)
        return self.get_response(request)