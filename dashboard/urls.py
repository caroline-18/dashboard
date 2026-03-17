from django.urls import path
from . import views

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path("",                     views.home,               name="home"),
    path("student/",             views.student_view,       name="student_view"),
    path("class/",               views.class_view,         name="class_view"),   # ← added
    path("login/",               views.login_view,         name="login"),
    path("logout/",              views.logout_view,        name="logout"),
    path('api/eca/save/',   views.api_eca_save,   name='api_eca_save'),
    path('api/eca/update/', views.api_eca_update, name='api_eca_update'),
    path('api/eca/delete/', views.api_eca_delete, name='api_eca_delete'),
    path("api/career-analysis/", views.api_career_analysis, name="api_career_analysis"),
    path('api/achievement/add/',        views.api_achievement_add,    name='api_achievement_add'),
    path('api/achievement/edit/',       views.api_achievement_edit,   name='api_achievement_edit'),
    path('api/achievement/delete/',     views.api_achievement_delete, name='api_achievement_delete'),
    path('api/achievement/cert/<int:record_id>/', views.api_achievement_cert, name='api_achievement_cert'),
]