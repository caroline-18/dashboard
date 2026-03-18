from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = 'django-insecure-h)$8^=w9f8e#h#xh__6c5tt$9)eepd^iw$8=b)+1bs=ph$kuv7'

DEBUG = True

ALLOWED_HOSTS = []

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'dashboard',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'dashboard.middleware.SchoolDBMiddleware',        # ← RIGHT after SessionMiddleware
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'aceventura_ai.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'dashboard' / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'aceventura_ai.wsgi.application'

MASTER_DB_CONFIG = {
    'ENGINE':   'django.db.backends.mysql',
    'NAME':     'u333015459_EvolvuUsrsTest',
    'USER':     'root',
    'PASSWORD': 'Kevin@1702',
    'HOST':     'localhost',
    'PORT':     '3306',
    'OPTIONS':  {'charset': 'utf8mb4'},
}

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    },
    # School DBs are registered dynamically at login time via
    # dashboard.school_registry.register_school_db()
    # Hard-code known schools here as fallback / dev convenience:
    'arnolds_db': {
        'ENGINE':   'django.db.backends.mysql',
        'NAME':     'arnolds1_analytics',
        "SOURCE_DB": "arnolds_live",
        'USER':     'root',
        'PASSWORD': 'Kevin@1702',
        'HOST':     'localhost',
        'PORT':     '3306',
        'OPTIONS':  {'charset': 'utf8mb4'},
    },
    'hscs_db': {
        'ENGINE':   'django.db.backends.mysql',
        'NAME':     'hscs1_analytics',
        'USER':     'root',
        'PASSWORD': 'Kevin@1702',
        'HOST':     'localhost',
        'PORT':     '3306',
        'OPTIONS':  {'charset': 'utf8mb4'},
    },
}

SCHOOL_DB_ALIAS_MAP = {
    1: 'arnolds_db',
    7: 'hscs_db',
    # Add more as you onboard schools
}

DATABASE_ROUTERS = ['dashboard.db_router.SchoolDatabaseRouter']

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'aceventura-cache',
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Manila'
USE_I18N = True
USE_TZ = True

STATIC_URL = '/static/'
STATICFILES_DIRS = [BASE_DIR / 'static']

LOGIN_URL           = '/login/'
LOGIN_REDIRECT_URL  = '/student/'
LOGOUT_REDIRECT_URL = '/login/'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

TEACHER_ACCESS_KEY   = "teacher"
PRINCIPAL_ACCESS_KEY = "principal"