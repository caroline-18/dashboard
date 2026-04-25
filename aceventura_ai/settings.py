from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("SECRET_KEY")
DEBUG = os.getenv("DEBUG", "False") == "True"
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "dashboard",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "dashboard.middleware.SchoolDBMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "aceventura_ai.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "dashboard" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "aceventura_ai.wsgi.application"

MASTER_DB_CONFIG = {
    "ENGINE":   "django.db.backends.mysql",
    "NAME":     os.getenv("MASTER_DB_NAME"),
    "USER":     os.getenv("MASTER_DB_USER"),
    "PASSWORD": os.getenv("MASTER_DB_PASSWORD"),
    "HOST":     os.getenv("MASTER_DB_HOST"),
    "PORT":     os.getenv("MASTER_DB_PORT", "3306"),
    "OPTIONS":  {"charset": "utf8mb4"},
}

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
    "arnolds_db": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.getenv("ARNOLDS_DB_NAME"),
        "USER": os.getenv("ARNOLDS_DB_USER"),
        "PASSWORD": os.getenv("ARNOLDS_DB_PASSWORD"),
        "HOST": os.getenv("ARNOLDS_DB_HOST", "localhost"),
        "PORT": os.getenv("ARNOLDS_DB_PORT", "3306"),
        "SOURCE_DB": os.getenv("ARNOLDS_SOURCE_DB"),
        "OPTIONS": {"charset": "utf8mb4"},
    },
    
    "arnolds_live": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.getenv("ARNOLDS_LIVE_DB_NAME"),
        "USER": os.getenv("ARNOLDS_LIVE_DB_USER"),
        "PASSWORD": os.getenv("ARNOLDS_LIVE_DB_PASSWORD"),
        "HOST": os.getenv("ARNOLDS_LIVE_DB_HOST"),
        "PORT": os.getenv("ARNOLDS_LIVE_DB_PORT", "3306"),
        "OPTIONS": {"charset": "utf8mb4"},
    },
    "hscs_db": {
        "ENGINE":   "django.db.backends.mysql",
        "NAME":     os.getenv("HSCS_DB_NAME"),
        "USER":     os.getenv("HSCS_DB_USER"),
        "PASSWORD": os.getenv("HSCS_DB_PASSWORD"),
        "HOST":     os.getenv("HSCS_DB_HOST", "localhost"),
        "PORT":     os.getenv("HSCS_DB_PORT", "3306"),
        "OPTIONS":  {"charset": "utf8mb4"},
    },
}

SCHOOL_DB_ALIAS_MAP = {
    1: "arnolds_db",
    7: "hscs_db",
}

DATABASE_ROUTERS = ["dashboard.db_router.SchoolDatabaseRouter"]

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "aceventura-cache",
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Manila"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]

LOGIN_URL           = "/login/"
LOGIN_REDIRECT_URL  = "/student/"
LOGOUT_REDIRECT_URL = "/login/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

TEACHER_ACCESS_KEY   = "teacher"
PRINCIPAL_ACCESS_KEY = "principal"
DEV_SCHOOL_BYPASS    = os.getenv("DEV_SCHOOL_BYPASS", "False") == "True"
CSRF_TRUSTED_ORIGINS = os.getenv("CSRF_TRUSTED_ORIGINS", "").split(",")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
