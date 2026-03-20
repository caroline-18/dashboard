import json
import logging
from collections import defaultdict
import base64

import pandas as pd
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils.html import format_html
from django.views.decorators.http import require_POST

from .data_loader import (
    _get_connection,
    load_student_profile,
    load_child_selector,
    load_student_subjects,
    load_exam_breakdown,
    load_subject_performance,
    load_class_summary,
    save_extracurricular_achievement,
    update_extracurricular_achievement,
    delete_extracurricular_achievement,
    authenticate_user,
    ROLE_PARENT,
    ROLE_TEACHER,
    ROLE_PRINCIPAL,
)

log = logging.getLogger("views")

def api_achievement_add(request):
    return api_eca_save(request)
 
def api_achievement_edit(request):
    return api_eca_update(request)
 
def api_achievement_delete(request):
    return api_eca_delete(request)

def api_achievement_cert(request, record_id: int):
    if not request.session.get("role"):
        return JsonResponse({"error": "Unauthorised"}, status=401)
    try:
        from .data_loader import _get_connection
        conn = _get_connection()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT certificate_data, certificate_filename, certificate_mime
            FROM   extracurricular_achievements
            WHERE  id = %s
        """, (int(record_id),))
        row = cur.fetchone()
        cur.close(); conn.close()
 
        if not row or not row["certificate_data"]:
            return JsonResponse({"error": "No certificate found."}, status=404)
 
        return JsonResponse({
            "ok":       True,
            "data":     row["certificate_data"],   # already a base64 string
            "mime":     row["certificate_mime"] or "application/octet-stream",
            "filename": row["certificate_filename"] or "certificate",
        })
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)
    
# ============================================================
#  CONSTANTS
# ============================================================

_CLASS_ORDER = ["nursery", "lkg", "ukg"] + [str(i) for i in range(1, 13)]

from .data_loader import _VALID_YEARS  # noqa: E402

_SA = {
    "mathematics":                   "Maths",
    "health & physical education":   "Physical Education",
    "health and physical education": "Physical Education",
    "social science":                "SST",
    "environmental studies":         "EVS",
    "moral science":                 "G.K",
    "value education":               "V.Ed",
}

_S, _CS, _SO, _OP = "Scholastic", "Co-Scholastic", "Social", "Optional"

SUBJECT_MASTER = {
    # ── Scholastic ──────────────────────────────────────────────────
    "english":                   (_S,  "📖", "#3b82f6"),
    "hindi":                     (_S,  "📖", "#3b82f6"),
    "marathi":                   (_S,  "📖", "#3b82f6"),
    "sanskrit":                  (_S,  "📜", "#3b82f6"),
    "maths":                     (_S,  "🔢", "#3b82f6"),
    "mathematics":               (_S,  "🔢", "#3b82f6"),
    "mathematics regular":       (_S,  "🔢", "#3b82f6"),
    "applied mathematics":       (_S,  "📐", "#3b82f6"),
    "science":                   (_S,  "🔬", "#3b82f6"),
    "physics":                   (_S,  "⚛️", "#3b82f6"),
    "chemistry":                 (_S,  "⚗️", "#3b82f6"),
    "biology":                   (_S,  "🧬", "#3b82f6"),
    "computer":                  (_S,  "💻", "#3b82f6"),
    "computer science":          (_S,  "💻", "#3b82f6"),
    "computer applications":     (_S,  "💻", "#3b82f6"),
    "artificial intelligence":   (_S,  "🤖", "#3b82f6"),
    "history":                   (_S,  "🏛️", "#3b82f6"),
    "civics":                    (_S,  "⚖️", "#3b82f6"),
    "geography":                 (_S,  "🗺️", "#3b82f6"),
    "political science":         (_S,  "🏛️", "#3b82f6"),
    "economics":                 (_S,  "📊", "#3b82f6"),
    "accountancy":               (_S,  "🧾", "#3b82f6"),
    "business studies":          (_S,  "💼", "#3b82f6"),
    "psychology":                (_S,  "🧠", "#3b82f6"),
    "social science":            (_S,  "🌍", "#3b82f6"),
    "sst":                       (_S,  "🌍", "#3b82f6"),
    "evs":                       (_S,  "🌿", "#3b82f6"),
    "gk":                        (_S,  "💡", "#3b82f6"),
    # ── Co-Scholastic ───────────────────────────────────────────────
    "p.e.":                      (_CS, "🏃", "#00c9a7"),
    "pt":                        (_CS, "🏃", "#00c9a7"),
    "dance":                     (_CS, "💃", "#00c9a7"),
    "art & craft":               (_CS, "🖌️", "#00c9a7"),
    "art/craft":                 (_CS, "🖌️", "#00c9a7"),
    "art & drawing":             (_CS, "🖌️", "#00c9a7"),
    "art education":             (_CS, "🖌️", "#00c9a7"),
    "music":                     (_CS, "🎵", "#00c9a7"),
    "physical education":        (_CS, "🏃", "#00c9a7"),
    "health and fitness":        (_CS, "💪", "#00c9a7"),
    "health & physical education": (_CS, "🏃", "#00c9a7"),
    "games":                     (_CS, "🎮", "#00c9a7"),
    "work education":            (_CS, "🔧", "#00c9a7"),
    "work educations (or pre-vocational education)": (_CS, "🔧", "#00c9a7"),
    "general studies":           (_CS, "📚", "#00c9a7"),
    "rhymes":                    (_CS, "🎵", "#00c9a7"),
    "colouring":                 (_CS, "🖍️", "#00c9a7"),
    "story":                     (_CS, "📖", "#00c9a7"),
    # ── Social ──────────────────────────────────────────────────────
    "library":                   (_SO, "📚", "#b794f6"),
    "mpt":                       (_SO, "🤝", "#b794f6"),
    "cca":                       (_SO, "🎭", "#b794f6"),
    # ── Optional ────────────────────────────────────────────────────
    "computer science (opt)":    (_OP, "💻", "#f59e0b"),
    "psychology (opt)":          (_OP, "🧠", "#f59e0b"),
    "applied mathematics (opt)": (_OP, "📐", "#f59e0b"),
}

_REMEDIAL_FOCUS = {
    "english":          ["Reading comprehension", "Grammar & punctuation", "Essay writing"],
    "hindi":            ["Reading comprehension", "Vyakaran (Grammar)", "Lekhan (Writing)"],
    "maths":            ["Core concept revision", "Problem-solving practice", "Formula recall"],
    "mathematics":      ["Core concept revision", "Problem-solving practice", "Formula recall"],
    "science":          ["Diagram labelling", "Concept revision", "Experiment notes"],
    "physics":          ["Formula derivation", "Numerical practice", "Conceptual clarity"],
    "chemistry":        ["Equation balancing", "Periodic table recall", "Reaction types"],
    "biology":          ["Diagram labelling", "Process revision", "Key term recall"],
    "computer science": ["Algorithm practice", "Code debugging", "Theory concepts"],
    "history":          ["Timeline practice", "Event cause & effect", "Key figure revision"],
    "geography":        ["Map practice", "Climate & landform revision", "Diagram work"],
    "economics":        ["Graph interpretation", "Key term revision", "Numerical practice"],
    "accountancy":      ["Journal entry practice", "Balance sheet", "Trial balance"],
    "social science":   ["Timeline practice", "Map practice", "Key term revision"],
}

_REMEDIAL_ACTIONS = {
    "english":          "Schedule weekly guided reading sessions with teacher",
    "hindi":            "Daily 15-min reading + weekly writing practice",
    "maths":            "Daily timed practice worksheets; seek teacher help for weak topics",
    "mathematics":      "Daily timed practice worksheets; seek teacher help for weak topics",
    "science":          "Diagram-focused revision + lab notebook review",
    "physics":          "Daily numerical practice; teacher support for derivations",
    "chemistry":        "Flashcard reactions + periodic table drill",
    "biology":          "Diagram drawing daily + concept note revision",
    "computer science": "Coding exercises daily + algorithm revision",
    "history":          "Timeline creation + chapter-wise summary notes",
    "geography":        "Daily map practice + diagram labelling exercises",
    "economics":        "Graph practice + numerical problem sets daily",
    "accountancy":      "Daily journal entry practice + teacher-led error correction",
    "social science":   "Map + timeline practice combined with key term revision",
}

_CAT_ABBR = {
    "Scholastic":    "SC",
    "Co-Scholastic": "CS",
    "Social":        "SO",
    "Optional":      "OP",
}

_EXAM_DISPLAY = {
    "term 1":                    "Term 1",
    "term 2":                    "Term 2",
    "periodic test 1":           "Periodic Test 1",
    "periodic test 2":           "Periodic Test 2",
    "periodic test 2 (ix n x)":  "Periodic Test 2",
    "periodic test 3":           "Periodic Test 3",
    "periodic test 3 (ix)":      "Periodic Test 3",
    "periodic test 1 (xi)":      "Periodic Test 1",
    "unit test 2":               "Unit Test 2",
    "unit test 3":               "Unit Test 3",
    "unit test 3 (xi)":          "Unit Test 3",
    "final exam":                "Final Exam",
    "pre-board 1":               "Pre-Board 1",
    "pre-board 2":               "Pre-Board 2",
}

def _normalise_exam_name(raw: str) -> str:
    return _EXAM_DISPLAY.get(str(raw).strip().lower(), str(raw).strip().title())


_COMPONENT_SORT = {
    "periodic test":        1,
    "term":                 2,
    "written":              2,
    "pre-board":            2,
    "subject enrichment":   3,
    "multiple assessment":  4,
    "portfolio":            4,
    "oral":                 3,
    "activity":             3,
    "practical":            2,
    "internal":             3,
    "reading":              2,
    "recitation":           3,
    "reading & recitation": 2,
    "conversation":         4,
    "rhymes":               3,
    "i can recognize":      1,
    "i can say":            2,
    "i can write(pattern)": 3,
    "i can say and recognise": 1,
}

def _component_sort_key(label: str) -> int:
    return _COMPONENT_SORT.get(str(label).strip().lower(), 99)


_SCHOLASTIC_SUBJECTS = {
    "english", "hindi", "marathi", "sanskrit",
    "maths", "mathematics", "mathematics regular", "applied mathematics",
    "science", "physics", "chemistry", "biology",
    "computer", "computer science", "computer applications", "artificial intelligence",
    "history", "civics", "geography", "political science",
    "economics", "accountancy", "business studies", "psychology",
    "social science", "sst", "evs", "gk",
}

def _is_scholastic(subject_name: str) -> bool:
    return subject_name.strip().lower() in _SCHOLASTIC_SUBJECTS


# ============================================================
#  SHARED HELPERS
# ============================================================

def _class_rank(cn):
    cn = str(cn).strip().lower()
    for p in ("class ", "grade ", "std ", "standard "):
        if cn.startswith(p):
            cn = cn[len(p):].strip()
            break
    try:
        return _CLASS_ORDER.index(cn)
    except Exception:
        return len(_CLASS_ORDER)


def sort_classes(cl):
    return sorted(cl, key=_class_rank)


def safe(v, fb="—"):
    if v is None or str(v).strip().lower() in ("none", "nan", ""):
        return fb
    return str(v).strip()


def fnum(v):
    try:
        f = float(v)
        if pd.notna(f):
            return f
    except Exception:
        pass
    return None


def inum(v):
    try:
        f = float(v)
        if pd.notna(f):
            return int(f)
    except Exception:
        pass
    return 0


def initials(name):
    p = str(name).strip().split()
    return (p[0][0] + (p[-1][0] if len(p) > 1 else "")).upper()


def performance_tier(pct):
    if pct is None:
        return None
    if pct >= 85:
        return ("⭐ Top Performer",  "#EAF3DE", "#27500A")
    elif pct >= 65:
        return ("📈 On Track",       "#E6F1FB", "#0C447C")
    elif pct >= 50:
        return ("💪 Needs Support",  "#FAEEDA", "#633806")
    else:
        return ("🔴 Focus Area",     "#FCEBEB", "#791F1F")


def clean_subjects(raw):
    if not raw or str(raw).strip().lower() in ("", "none", "nan", "—"):
        return ""
    seen, cl = [], []
    for s in str(raw).split(","):
        s = s.strip()
        if not s:
            continue
        c = _SA.get(s.lower(), s)
        k = c.lower()
        if k not in seen:
            seen.append(k)
            cl.append(c)
    return ", ".join(cl)


def get_subject_meta(sn):
    return SUBJECT_MASTER.get(sn.strip().lower(), (_S, "📘", "#3b82f6"))


def remedial_focus(sn):
    return _REMEDIAL_FOCUS.get(
        sn.strip().lower(),
        ["Concept revision", "Practice exercises", "Clarify doubts with teacher"],
    )


def remedial_action(sn):
    return _REMEDIAL_ACTIONS.get(
        sn.strip().lower(),
        "Attend extra support sessions and complete all assigned practice work",
    )


def priority_label(score):
    if score < 35:
        return ("Needs Urgent Help",  "rgba(239,68,68,0.14)",  "#dc2626")
    return     ("Needs Some Support", "rgba(251,146,60,0.14)", "#ea580c")


def is_senior_class(class_name):
    cn = str(class_name).strip().lower()
    for p in ("standard", "class", "grade", "std"):
        if cn.startswith(p):
            cn = cn[len(p):].strip()
            break
    for s in ("th", "st", "nd", "rd"):
        if cn.endswith(s):
            cn = cn[:-len(s)].strip()
            break
    _rom = {"ix": "9", "x": "10", "xi": "11", "xii": "12"}
    if cn in _rom:
        cn = _rom[cn]
    return cn in ("9", "10", "11", "12")


def build_remedial_subjects(full_subjects_df):
    remedial = []
    if full_subjects_df is None or full_subjects_df.empty:
        return remedial

    sc = next(
        (c for c in ("avg_percent", "subject_avg", "score", "percentage")
         if c in full_subjects_df.columns),
        None,
    )
    if not sc:
        return remedial

    rd = full_subjects_df.copy().reset_index(drop=True)
    numeric_scores = pd.to_numeric(rd[sc], errors="coerce")
    below_50   = (numeric_scores < 50) & numeric_scores.notna()
    scholastic = rd["subject_name"].apply(lambda s: get_subject_meta(str(s))[0] == _S)
    rd = rd[below_50 & scholastic].copy().reset_index(drop=True)
    if rd.empty:
        return remedial

    rd["_score"] = pd.to_numeric(rd[sc], errors="coerce")
    rd = rd.sort_values("_score").reset_index(drop=True)

    for _, rr in rd.iterrows():
        sn = str(rr["subject_name"])
        sv = float(rr["_score"])
        pl, pb, pc = priority_label(sv)
        remedial.append({
            "name":     sn,
            "score":    sv,
            "icon":     get_subject_meta(sn)[1],
            "priority": pl,
            "pr_bg":    pb,
            "pr_color": pc,
            "focus":    remedial_focus(sn),
            "action":   remedial_action(sn),
        })
    return remedial


def build_subject_groups(subjects_clean):
    groups = defaultdict(list)
    for subj in subjects_clean.split(", "):
        subj = subj.strip()
        if not subj:
            continue
        cat, icon, color = get_subject_meta(subj)
        groups[cat].append({"name": subj, "icon": icon, "color": color})
    return dict(groups)


# ============================================================
#  BUILD EXAM DATA
# ============================================================

def build_exam_data(student_id, acad_yr):
    exam_data       = {}
    available_exams = ["all"]

    try:
        if student_id is not None:
            df_exams = load_exam_breakdown(
                student_id=int(student_id), academic_yr=acad_yr
            )
            if df_exams is not None and not df_exams.empty:

                df_exams["_exam_display"] = df_exams["exam_name"].apply(_normalise_exam_name)

                _exam_order = {}
                for raw_exam in df_exams["exam_name"].unique():
                    norm = _normalise_exam_name(raw_exam)
                    if norm not in _exam_order:
                        _exam_order[norm] = len(_exam_order)

                for exam_display, exam_grp in df_exams.groupby("_exam_display", sort=False):
                    rows = []
                    for subj_name, subj_grp in exam_grp.groupby("subject_name", sort=False):
                        sn = str(subj_name)
                        cat, icon, color = get_subject_meta(sn)
                        cat_abbr = _CAT_ABBR.get(cat, "SC")

                        components = []
                        grand_obtained = 0.0
                        grand_max      = 0.0

                        for _, cr in subj_grp.iterrows():
                            lbl  = str(cr.get("exam_type", "")).strip()
                            mo   = cr.get("marks_obtained")
                            mm   = cr.get("max_marks")
                            mo_f = float(mo) if mo is not None and pd.notna(mo) else None
                            mm_f = float(mm) if mm is not None and pd.notna(mm) else None

                            if mo_f is not None:
                                grand_obtained += mo_f
                            if mm_f is not None:
                                grand_max += mm_f

                            components.append({
                                "label":      lbl,
                                "obtained":   round(mo_f, 1) if mo_f is not None else None,
                                "highest":    round(mm_f, 1) if mm_f is not None else None,
                                "sort_order": _component_sort_key(lbl),
                            })

                        components.sort(key=lambda c: c["sort_order"])

                        pct = round(grand_obtained / grand_max * 100, 1) if grand_max > 0 else 0

                        rows.append({
                            "name":         sn,
                            "icon":         icon,
                            "color":        color,
                            "category":     cat,
                            "catAbbr":      cat_abbr,
                            "isScholastic": _is_scholastic(sn),
                            "components":   components,
                            "total":        round(grand_obtained, 1),
                            "maxMarks":     round(grand_max, 1),
                            "passMark":     None,
                            "grade":        "",
                            "avg":          pct,
                        })

                    _cat_order = {"Scholastic": 0, "Co-Scholastic": 1, "Social": 2, "Optional": 3}
                    rows.sort(key=lambda r: (_cat_order.get(r["category"], 4), r["name"]))

                    if rows:
                        exam_data[exam_display] = rows
                        if exam_display not in available_exams:
                            available_exams.append(exam_display)

                real_exams = [e for e in available_exams if e != "all"]
                real_exams.sort(key=lambda e: _exam_order.get(e, 99))
                available_exams = ["all"] + real_exams

    except Exception as e:
        log.warning("Could not load exam breakdown for student_id=%s: %s", student_id, e)

    return exam_data, available_exams


# ============================================================
#  CLASS VIEW — PRIVATE HELPERS
# ============================================================

def _att_dist_counts(series):
    s = series.dropna()
    return [
        int((s < 60).sum()),
        int(((s >= 60) & (s < 75)).sum()),
        int(((s >= 75) & (s < 85)).sum()),
        int(((s >= 85) & (s < 90)).sum()),
        int((s >= 90).sum()),
    ]


def _acad_dist_counts(series):
    s      = series.dropna()
    bins   = list(range(0, 101, 10))
    labels = [f"{b}–{b+10}" for b in bins[:-1]]
    cut    = pd.cut(s, bins=bins, labels=labels, right=False, include_lowest=True)
    return cut.value_counts().reindex(labels, fill_value=0).tolist()


def _acad_dist_colors(series):
    return [
        "#00c9a7" if b >= 70 else ("#5b9aff" if b >= 40 else "#f87171")
        for b in range(0, 100, 10)
    ]


def _signal_items(series, color_map):
    counts = series.dropna().value_counts()
    total  = counts.sum()
    return [
        {
            "label": lbl,
            "count": int(cnt),
            "pct":   round(cnt / total * 100) if total else 0,
            "color": color_map.get(lbl, "#94a3b8"),
        }
        for lbl, cnt in counts.items()
    ]


def _load_subject_marks_cv(academic_yr, class_name):
    subjects_list, subject_marks = [], {}
    conn = None
    try:
        conn = _get_connection()
        cur  = conn.cursor(dictionary=True)

        cur.execute("""
            SELECT DISTINCT ds.subject_id, ds.subject_name
            FROM dim_class_subject_map csm
            JOIN dim_subject ds ON ds.subject_id = csm.subject_id
            WHERE csm.academic_yr = %s AND csm.class_name = %s
            ORDER BY ds.subject_name
        """, (academic_yr, class_name))
        all_subjects = cur.fetchall()

        cur.execute("""
            SELECT
                fsp.student_id,
                CASE
                    WHEN TRIM(d.student_name) LIKE '%% %%'
                        THEN TRIM(d.student_name)
                    WHEN p.father_name IS NOT NULL AND TRIM(p.father_name) != ''
                        THEN CONCAT(TRIM(d.student_name), ' ',
                                    SUBSTRING_INDEX(TRIM(p.father_name), ' ', 1))
                    ELSE TRIM(d.student_name)
                END AS student_name,
                fsp.subject_id,
                fsp.avg_percent AS subject_avg,
                fsp.written_avg,
                fsp.oral_avg
            FROM fact_student_subject_performance fsp
            JOIN dim_student_demographics d
                ON d.student_id = fsp.student_id AND d.academic_yr = fsp.academic_yr
            LEFT JOIN dim_parent p ON p.parent_id = d.parent_id
            WHERE fsp.academic_yr = %s
              AND fsp.subject_id IN (
                  SELECT subject_id FROM dim_class_subject_map
                  WHERE class_name = %s AND academic_yr = %s
              )
              AND d.academic_yr = %s
              AND d.student_name IS NOT NULL
              AND TRIM(d.student_name) != ''
        """, (academic_yr, class_name, academic_yr, academic_yr))
        all_marks = cur.fetchall()

        mbys = {}
        for row in all_marks:
            sid = row["subject_id"]
            mbys.setdefault(sid, []).append({
                "student_name": row["student_name"],
                "subject_avg":  float(row["subject_avg"])  if row["subject_avg"]  is not None else None,
                "written_avg":  float(row["written_avg"])  if row["written_avg"]  is not None else None,
                "oral_avg":     float(row["oral_avg"])     if row["oral_avg"]     is not None else None,
            })

        swm = set(mbys.keys())
        for subj in all_subjects:
            sid = subj["subject_id"]
            subjects_list.append({
                "subject_id":   sid,
                "subject_name": subj["subject_name"],
                "has_marks":    sid in swm,
            })
            subject_marks[str(sid)] = mbys.get(sid, [])

        cur.close()
    except Exception as e:
        log.warning("class_view subject load error: %s", e)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return subjects_list, subject_marks


# ============================================================
#  AI INSIGHT
# ============================================================

def get_ai_insight(student_dict, history_df=None):
    try:
        from ai.student_gemini_analysis import generate_dashboard_insight
        return generate_dashboard_insight(
            current_student=student_dict,
            history_df=history_df if history_df is not None else pd.DataFrame(),
        )
    except Exception as e:
        log.warning("AI insight unavailable: %s", e)
        return None


# ============================================================
#  VIEWS
# ============================================================

def home(request):
    if not request.session.get("role"):
        return redirect("login")
    return redirect("student_view")


def student_view(request):
    if not request.session.get("role"):
        return redirect("login")

    role     = request.session.get("role", "admin")
    adm_type = request.session.get("admin_type", "")
    is_teacher   = (role == "admin" and adm_type == "teacher")
    is_principal = (role == "admin" and adm_type == "principal")

    df_all = load_student_profile()
    if df_all is None or df_all.empty:
        return render(request, "student_view.html", {"error": "no_data"})

    for col in ("academic_yr", "class_name", "section_name", "student_name"):
        if col in df_all.columns:
            df_all[col] = df_all[col].astype(str).str.strip()

    if role == "parent":
        parent_id = request.session.get("parent_id")
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError):
            pass
        df = df_all[df_all["parent_id"] == parent_id].copy() \
            if "parent_id" in df_all.columns else df_all.copy()
    else:
        df = df_all.copy()
        # ── TEACHER FILTER: restrict to assigned class/section only ──
        if is_teacher:
            teacher_class_id = request.session.get("teacher_class_id")
            teacher_sec_id   = request.session.get("teacher_sec_id")
            if teacher_class_id and "class_id" in df.columns:
                df = df[df["class_id"].astype(str) == str(teacher_class_id)]
            if teacher_sec_id and "section_id" in df.columns:
                df = df[df["section_id"].astype(str) == str(teacher_sec_id)]

    if df.empty:
        return render(request, "student_view.html", {"error": "no_student"})

    children_list     = []
    selected_child_id = ""
    student_years     = []
    selected_year     = ""
    all_years         = []
    all_classes, all_divs, all_students = [], [], []
    selected_class, selected_div, selected_name = "All Classes", "All Sections", ""

    # ── ADMIN BRANCH ──────────────────────────────────────────────
    if role == "admin":
        all_years     = sorted(df["academic_yr"].dropna().unique().tolist(), reverse=True)
        selected_year = request.GET.get("year", all_years[0] if all_years else "")

        df_yr = df[df["academic_yr"] == selected_year] if selected_year else df

        all_classes    = sort_classes(df_yr["class_name"].dropna().unique().tolist())
        selected_class = request.GET.get("class", "All Classes")
        df_cls         = df_yr if selected_class == "All Classes" \
                         else df_yr[df_yr["class_name"] == selected_class]

        all_divs     = sorted(df_cls["section_name"].dropna().unique().tolist())
        selected_div = request.GET.get("section", "All Sections")
        df_sec       = df_cls if selected_div == "All Sections" \
                       else df_cls[df_cls["section_name"] == selected_div]

        all_students  = sorted(df_sec["student_name"].dropna().unique().tolist())
        requested_name = request.GET.get("student", "")
        # Reset to first student if class or section changed and old name no longer valid
        selected_name = requested_name if requested_name in all_students else (all_students[0] if all_students else "")

        student_rows = df_sec[df_sec["student_name"] == selected_name]
        if student_rows.empty:
            return render(request, "student_view.html", {
                "error": "no_student",
                "role": role,
                "filters": {
                    "years": all_years, "selected_year": selected_year,
                    "classes": all_classes, "selected_class": selected_class,
                    "divs": all_divs, "selected_div": selected_div,
                    "students": all_students, "selected_name": selected_name,
                },
            })
        student = student_rows.iloc[0]

    # ── PARENT BRANCH ─────────────────────────────────────────────
    else:
        children_df = load_child_selector(request.session.get("parent_id"))
        if children_df is None or children_df.empty:
            return render(request, "student_view.html", {"error": "no_student"})

        default_child_id  = str(children_df.iloc[0]["latest_student_id"])
        raw_child = request.GET.get("child", default_child_id)
        valid_ids = children_df["latest_student_id"].astype(str).tolist()
        selected_child_id = raw_child if raw_child in valid_ids else default_child_id

        child_row = children_df[
            children_df["latest_student_id"].astype(str) == selected_child_id
        ]
        if child_row.empty:
            child_row         = children_df.iloc[[0]]
            selected_child_id = str(child_row.iloc[0]["latest_student_id"])

        child_display = child_row.iloc[0]["display_name"]
        child_first   = child_display.split()[0].upper()

        pid     = request.session.get("parent_id")
        pid_str = str(pid)
        if "parent_id" in df_all.columns:
            student_all_years = df_all[
                (df_all["parent_id"].astype(str) == pid_str) &
                (df_all["student_name"].str.upper().str.startswith(child_first, na=False))
            ].copy()
        else:
            student_all_years = df_all[
                df_all["student_name"].str.upper().str.startswith(child_first, na=False)
            ].copy()

        student_years = sorted(
            student_all_years["academic_yr"].dropna().unique().tolist(), reverse=True
        )

        requested_year = request.GET.get("year", "")
        if requested_year and requested_year in student_years:
            selected_year = requested_year
        else:
            selected_year = student_years[0] if student_years else ""

        student_row = student_all_years[student_all_years["academic_yr"] == selected_year]
        if student_row.empty:
            return render(request, "student_view.html", {"error": "no_data", "role": role})

        student       = student_row.iloc[0]
        children_list = children_df[["display_name", "latest_student_id"]].to_dict("records")

    # ── RESOLVE STUDENT VALUES ────────────────────────────────────
    name            = safe(student.get("student_name"), "Student").upper()
    cls             = safe(student.get("class_name"))
    division        = safe(student.get("section_name"))
    acad_yr         = safe(student.get("academic_yr"))
    overall         = fnum(student.get("avg_percent"))
    attendance      = fnum(student.get("attendance_percentage"))
    written         = fnum(student.get("written_avg"))
    oral            = fnum(student.get("oral_avg"))
    achievements    = inum(student.get("achievement_count"))
    achievement_list_raw = safe(student.get("achievement_list"), "")
    hw_count        = inum(student.get("homework_assigned_count"))
    att_band        = safe(student.get("attendance_band"))
    learn_style     = safe(student.get("learning_style"))
    engage          = safe(student.get("engagement_pattern"))
    strength        = safe(student.get("primary_strength_axis"), "")
    father          = safe(student.get("father_name"))
    mother          = safe(student.get("mother_name"))
    father_contact  = safe(student.get("father_contact"))
    mother_contact  = safe(student.get("mother_contact"))
    guardian_name   = safe(student.get("guardian_name"))
    guardian_mobile = safe(student.get("guardian_mobile"))
    parent_id_v     = safe(student.get("parent_id"))
    student_id      = student.get("student_id")
    avatar          = initials(name)

    # ── Subjects ──────────────────────────────────────────────────
    subjects_clean = clean_subjects(safe(student.get("strong_subjects"), ""))
    subjects_ai    = safe(student.get("strong_subjects_ai"), "")

    full_subjects_df = pd.DataFrame()
    if student_id is not None:
        try:
            full_subjects_df = load_student_subjects(
                student_id=int(student_id), academic_yr=acad_yr
            )
        except Exception as e:
            log.warning("Could not load subjects for student_id=%s: %s", student_id, e)

    if not subjects_clean and not full_subjects_df.empty:
        if "avg_percent" in full_subjects_df.columns:
            _scores = pd.to_numeric(full_subjects_df["avg_percent"], errors="coerce")
            _top    = full_subjects_df[_scores >= 75]
        else:
            _top = pd.DataFrame()
        _src = _top if not _top.empty else full_subjects_df.head(5)
        if "subject_name" in _src.columns:
            subjects_clean = clean_subjects(", ".join(_src["subject_name"].tolist()))
            subjects_ai    = ", ".join(_src["subject_name"].tolist())

    subject_groups    = build_subject_groups(subjects_clean) if subjects_clean else {}
    remedial_subjects = build_remedial_subjects(full_subjects_df)

    subjects_list = []
    if not full_subjects_df.empty and "avg_percent" in full_subjects_df.columns:
        for _, sr in full_subjects_df.iterrows():
            sn    = str(sr.get("subject_name", ""))
            sp    = float(pd.to_numeric(sr.get("avg_percent", 0), errors="coerce") or 0)
            icon  = get_subject_meta(sn)[1]
            color = "#00c9a7" if sp >= 75 else ("#f59e0b" if sp >= 50 else "#ef4444")
            subjects_list.append({
                "name":         sn,
                "icon":         icon,
                "score":        sp,
                "color":        color,
                "isScholastic": _is_scholastic(sn),
                "avg":          sp,
            })

    # ── Exam data ─────────────────────────────────────────────────
    exam_data, available_exams = build_exam_data(student_id, acad_yr)

    if "all" not in exam_data or not exam_data["all"]:
        exam_data["all"] = subjects_list

    # ── Classmates scatter ────────────────────────────────────────
    classmates_json = []
    required_cols   = {"academic_yr", "class_name", "section_name"}
    if required_cols.issubset(set(df_all.columns)):
        df_classmates = df_all[
            (df_all["academic_yr"]  == str(student.get("academic_yr", "")).strip()) &
            (df_all["class_name"]   == str(student.get("class_name",  "")).strip()) &
            (df_all["section_name"] == str(student.get("section_name","")).strip())
        ].copy()
        for _, cr in df_classmates.iterrows():
            s = fnum(cr.get("avg_percent"))
            a = fnum(cr.get("attendance_percentage"))
            classmates_json.append({
                "name":       safe(cr.get("student_name")),
                "score":      round(s, 1) if s is not None else 0,
                "attendance": round(a, 1) if a is not None else 0,
                "is_me":      str(cr.get("student_id")) == str(student_id),
            })

    # ── Multi-year progress ───────────────────────────────────────
    progress_data = []
    snk  = student.get("student_name")
    spid = str(student.get("parent_id", ""))
    if all(c in df_all.columns for c in ("student_name", "parent_id", "academic_yr")):
        history_df = df_all[
            (df_all["student_name"] == snk) &
            (df_all["parent_id"].astype(str) == spid)
        ].copy()
        for _, yr_row in history_df.sort_values("academic_yr").iterrows():
            sc = fnum(yr_row.get("avg_percent"))
            progress_data.append({
                "year":  safe(yr_row.get("academic_yr")),
                "score": round(sc, 1) if sc is not None else 0,
            })
    else:
        history_df = pd.DataFrame()

    # ── AI Insight ────────────────────────────────────────────────
    ai_text = get_ai_insight(
        student_dict={**student.to_dict(), "strong_subjects": subjects_ai or subjects_clean},
        history_df=history_df if not history_df.empty else pd.DataFrame(),
    )
    if not ai_text:
        o_str = f"{overall:.1f}%" if overall is not None else "N/A"
        a_str = f"{attendance:.1f}%" if attendance is not None else "N/A"
        ai_text = (
            f"{name.title()} has an overall average of {o_str} and attends "
            f"{a_str} of school days. "
            "Once more data is recorded, a personalised AI summary will appear here."
        )

    # ── Performance tier ──────────────────────────────────────────
    tier_raw = performance_tier(overall)
    tier = (
        {"label": tier_raw[0], "bg": tier_raw[1], "color": tier_raw[2]}
        if tier_raw else None
    )

    # ── Achievement list ──────────────────────────────────────────
    achievement_list_json = json.dumps(
        [a.strip() for a in achievement_list_raw.split(",") if a.strip()]
    )

    # ── BUILD CONTEXT ─────────────────────────────────────────────
    context = {
        "name":       name,
        "avatar":     avatar,
        "cls":        cls,
        "division":   division,
        "acad_yr":    acad_yr,
        "student_id": student_id,

        "overall":       overall,
        "overall_disp":  f"{overall:.1f}%" if overall is not None else "N/A",
        "overall_pct":   min(int(overall), 100) if overall is not None else 0,
        "attendance":    attendance,
        "att_disp":      f"{attendance:.1f}%" if attendance is not None else "N/A",
        "att_pct":       min(int(attendance), 100) if attendance is not None else 0,
        "written":       written,
        "oral":          oral,

        "achievements":          achievements,
        "achievementListRaw":    json.dumps(achievement_list_raw if achievement_list_raw else ""),
        "achievement_list_json": achievement_list_json,
        "hw_count":              hw_count,
        "att_band":              att_band,
        "learn_style":           learn_style,
        "engage":                engage,
        "strength":              strength,

        "father":          father,
        "mother":          mother,
        "father_contact":  father_contact,
        "mother_contact":  mother_contact,
        "guardian_name":   guardian_name,
        "guardian_mobile": guardian_mobile,
        "parent_id_v":     parent_id_v,

        "subject_groups":    json.dumps(subject_groups),
        "subjects_list":     json.dumps(subjects_list),
        "remedial_subjects": json.dumps(remedial_subjects),
        "remedial_count":    len(remedial_subjects),

        "exam_data":       json.dumps(exam_data),
        "available_exams": json.dumps(available_exams),

        "classmates_json": json.dumps(classmates_json),
        "progress_data":   json.dumps(progress_data),
        "has_multi_year":  len(progress_data) > 1,

        "ai_text":   ai_text,
        "is_senior": is_senior_class(cls),

        "role":         role,
        "is_teacher":   is_teacher,
        "is_principal": is_principal,
        "tier":         tier,

        "teacher_name":  request.session.get("teacher_name", ""),
        "teacher_desig": request.session.get("teacher_desig", ""),
    }

    if role == "admin":
        context["filters"] = {
            "years":          all_years,
            "selected_year":  selected_year,
            "classes":        all_classes,
            "selected_class": selected_class,
            "divs":           all_divs,
            "selected_div":   selected_div,
            "students":       all_students,
            "selected_name":  selected_name,
        }
    else:
        context.update({
            "children_list":  children_list,
            "selected_child": selected_child_id,
            "student_years":  student_years,
            "selected_year":  selected_year,
        })
    achievement_records = []
    if student_id is not None:
        try:
            from .data_loader import _get_connection
            _conn = _get_connection()
            _cur  = _conn.cursor(dictionary=True)
            _cur.execute("""
                SELECT id, title, category, achievement_date, level, position,
                       description, certificate_filename, certificate_mime,
                       CASE WHEN certificate_data IS NOT NULL AND certificate_data != ''
                            THEN 1 ELSE 0 END AS has_cert,
                       submitted_at
                FROM   extracurricular_achievements
                WHERE  student_id = %s
                ORDER  BY achievement_date DESC
            """, (int(student_id),))
            for r in _cur.fetchall():
                achievement_records.append({
                    "id":                   r["id"],
                    "title":                r["title"],
                    "category":             r["category"],
                    "achievement_date":     str(r["achievement_date"]),
                    "level":                r["level"],
                    "position":             r["position"] or "",
                    "description":          r["description"] or "",
                    "certificate_filename": r["certificate_filename"] or "",
                    "certificate_mime":     r["certificate_mime"] or "",
                    "has_cert":             bool(r["has_cert"]),
                    "submitted_at":         str(r["submitted_at"]) if r["submitted_at"] else "",
                })
            _cur.close(); _conn.close()
        except Exception as _e:
            log.warning("Could not load ECA records for student_id=%s: %s", student_id, _e)
 
    # Add to context (place just before the return render(...) line)
    context["achievement_records_json"] = json.dumps(achievement_records)

    return render(request, "student_view.html", context)


# ============================================================
#  CLASS VIEW
# ============================================================

def class_view(request):
    role = request.session.get("role")
    if not role:
        return redirect("login")
    if role != "admin":
        return redirect("student_view")

    adm_type     = request.session.get("admin_type", "")
    is_teacher   = (adm_type == "teacher")
    is_principal = (adm_type == "principal")

    df_all = load_student_profile()
    if df_all is None or df_all.empty:
        return render(request, "class_view.html", {"error": "No data available."})

    df_all["avg_percent"]           = pd.to_numeric(df_all["avg_percent"],           errors="coerce")
    df_all["attendance_percentage"] = pd.to_numeric(df_all["attendance_percentage"], errors="coerce")

    all_years   = sorted(df_all["academic_yr"].dropna().unique().tolist(), reverse=True)
    all_classes = sort_classes(df_all["class_name"].dropna().unique().tolist())
    # ── TEACHER FILTER: restrict to assigned class/section only ──
    if is_teacher:
        teacher_class_id = request.session.get("teacher_class_id")
        if teacher_class_id and "class_id" in df_all.columns:
            df_all = df_all[df_all["class_id"].astype(str) == str(teacher_class_id)]
            all_classes = sort_classes(df_all["class_name"].dropna().unique().tolist())

    selected_year     = request.GET.get("year",       all_years[0]   if all_years   else "")
    selected_class    = request.GET.get("class_name", all_classes[0] if all_classes else "")
    selected_division = request.GET.get("division",   "all")

    df_yr  = df_all[df_all["academic_yr"] == selected_year]
    df_cls = df_yr[df_yr["class_name"]    == selected_class]
    all_divisions = sorted(df_cls["section_name"].dropna().unique().tolist())

    df = df_cls.copy() if selected_division == "all" \
         else df_cls[df_cls["section_name"] == selected_division].copy()

    if df.empty:
        return render(request, "class_view.html", {
            "error":             "No students found for the selected filters.",
            "all_years":         all_years,
            "all_classes":       all_classes,
            "all_divisions":     all_divisions,
            "selected_year":     selected_year,
            "selected_class":    selected_class,
            "selected_division": selected_division,
            "is_teacher":        is_teacher,
            "is_principal":      is_principal,
        })

    n_students   = len(df)
    avg_acad_raw = float(df["avg_percent"].mean())
    avg_att_raw  = float(df["attendance_percentage"].mean())
    count_top    = int((df["avg_percent"] >= 75).sum())

    at_risk_both_df = df[(df["attendance_percentage"] < 75) & (df["avg_percent"] < 40)]
    at_risk_att_df  = df[(df["attendance_percentage"] < 75) & (df["avg_percent"] >= 40)]
    at_risk_acad_df = df[(df["avg_percent"] < 40)           & (df["attendance_percentage"] >= 75)]

    MEDALS       = ["🥇", "🥈", "🥉", "4th", "5th"]
    MEDAL_COLORS = ["#FFD700", "#C0C0C0", "#CD7F32", "#94a3b8", "#94a3b8"]

    top_df = (df.dropna(subset=["avg_percent"])
                .sort_values("avg_percent", ascending=False)
                .query("avg_percent > 80")
                .head(5)
                .reset_index(drop=True))
    top_performers = [
        {
            "student_name": row.get("student_name", ""),
            "reg_no":       str(row.get("reg_no", "")) if pd.notna(row.get("reg_no")) else "",
            "avg_percent":  round(float(row["avg_percent"]), 1),
            "medal":        MEDALS[i] if i < 5 else str(i + 1),
            "medal_color":  MEDAL_COLORS[i] if i < 5 else "#94a3b8",
        }
        for i, row in top_df.iterrows()
    ]
    needs_support = [
        {
            "student_name": row.get("student_name", ""),
            "reg_no":       str(row.get("reg_no", "")) if pd.notna(row.get("reg_no")) else "",
            "avg_percent":  round(float(row["avg_percent"]), 1),
        }
        for _, row in (
            df.dropna(subset=["avg_percent"])
              .query("avg_percent < 35")
              .sort_values("avg_percent")
              .head(10)
              .iterrows()
        )
    ]

    acad_med = float(df["avg_percent"].median())
    att_med  = float(df["attendance_percentage"].median())

    def _zone(a, b):
        if a >= acad_med and b >= att_med: return "Thriving"
        if a >= acad_med:                  return "Academically Strong"
        if b >= att_med:                   return "Present & Growing"
        return "Needs Support"

    scatter_rows   = df.dropna(subset=["avg_percent", "attendance_percentage"])
    scatter_points = [
        {
            "x":     round(float(r["attendance_percentage"]), 1),
            "y":     round(float(r["avg_percent"]), 1),
            "label": str(r.get("student_name", "")),
            "zone":  _zone(float(r["avg_percent"]), float(r["attendance_percentage"])),
        }
        for _, r in scatter_rows.iterrows()
    ]
    zc = {"Thriving": 0, "Academically Strong": 0, "Present & Growing": 0, "Needs Support": 0}
    for p in scatter_points:
        zc[p["zone"]] += 1

    def zpct(k):
        return round(zc[k] / n_students * 100) if n_students else 0

    att_pattern    = _signal_items(
        df["attendance_band"]    if "attendance_band"    in df.columns else pd.Series(dtype=str),
        {"Highly Consistent": "#00c9a7", "Moderately Consistent": "#5b9aff", "Irregular": "#f87171"},
    )
    learning_style = _signal_items(
        df["learning_style"]     if "learning_style"     in df.columns else pd.Series(dtype=str),
        {"Conceptual / Written-Oriented": "#5b9aff",
         "Experiential / Oral-Oriented":  "#b794f6",
         "Balanced":                       "#00c9a7"},
    )
    engagement_pat = _signal_items(
        df["engagement_pattern"] if "engagement_pattern" in df.columns else pd.Series(dtype=str),
        {"Academically Engaged": "#00c9a7", "Low Visible Engagement": "#f87171"},
    )

    def _risk_list(df_r):
        return [
            {
                "student_name":          str(r.get("student_name", "")),
                "reg_no":                str(r.get("reg_no", "")) if pd.notna(r.get("reg_no")) else "",
                "avg_percent":           round(float(r["avg_percent"]), 1),
                "attendance_percentage": round(float(r["attendance_percentage"]), 1),
            }
            for _, r in df_r.sort_values("avg_percent").iterrows()
        ]

    thriving_pct = zc["Thriving"] / n_students * 100 if n_students else 0
    att_good_pct = len(df[df["attendance_percentage"] >= 85]) / n_students * 100 if n_students else 0
    risk_pct     = len(at_risk_both_df) / n_students * 100 if n_students else 0

    if risk_pct > 20:
        insight_emoji = "⚠️"
        insight_html  = format_html(
            "<b>{}%</b> of students are showing both low attendance "
            "and low academic scores — early intervention is recommended.",
            round(risk_pct),
        )
    elif thriving_pct >= 70:
        insight_emoji = "🌟"
        insight_html  = format_html(
            "<b>{}%</b> of students are academically on track with "
            "good attendance. The class is performing well overall.",
            round(thriving_pct),
        )
    elif att_good_pct < 50:
        insight_emoji = "📅"
        insight_html  = format_html(
            "Only <b>{}%</b> of students have attendance above 85%. "
            "Improving attendance could boost academic outcomes.",
            round(att_good_pct),
        )
    else:
        insight_emoji = "📊"
        insight_html  = format_html(
            "Class average is <b>{}</b>% academic, <b>{}</b>% attendance. "
            "{} student(s) need priority support.",
            f"{avg_acad_raw:.1f}",
            f"{avg_att_raw:.1f}",
            len(at_risk_both_df),
        )

    subjects_list_cv, subject_marks = _load_subject_marks_cv(selected_year, selected_class)

    return render(request, "class_view.html", {
        "all_years":         all_years,
        "all_classes":       all_classes,
        "all_divisions":     all_divisions,
        "selected_year":     selected_year,
        "selected_class":    selected_class,
        "selected_division": selected_division,

        "n_students":         n_students,
        "avg_academic":       avg_acad_raw,
        "avg_attendance":     avg_att_raw,
        "count_top":          count_top,
        "count_at_risk_both": len(at_risk_both_df),
        "count_at_risk_att":  len(at_risk_att_df),
        "count_at_risk_acad": len(at_risk_acad_df),

        "top_performers": top_performers,
        "needs_support":  needs_support,

        "acad_dist_counts": json.dumps(_acad_dist_counts(df["avg_percent"])),
        "acad_dist_colors": json.dumps(_acad_dist_colors(df["avg_percent"])),
        "att_dist_counts":  json.dumps(_att_dist_counts(df["attendance_percentage"])),

        "scatter_points":         json.dumps(scatter_points),
        "zone_thriving":          zc["Thriving"],
        "zone_thriving_pct":      zpct("Thriving"),
        "zone_acad_strong":       zc["Academically Strong"],
        "zone_acad_strong_pct":   zpct("Academically Strong"),
        "zone_present":           zc["Present & Growing"],
        "zone_present_pct":       zpct("Present & Growing"),
        "zone_needs_support":     zc["Needs Support"],
        "zone_needs_support_pct": zpct("Needs Support"),

        "att_pattern_json":   json.dumps(att_pattern),
        "learn_style_json":   json.dumps(learning_style),
        "engagement_json":    json.dumps(engagement_pat),
        "attendance_pattern": att_pattern,
        "learning_style":     learning_style,
        "engagement_pattern": engagement_pat,

        "at_risk_both":  _risk_list(at_risk_both_df),
        "at_risk_att":   _risk_list(at_risk_att_df),
        "at_risk_acad":  _risk_list(at_risk_acad_df),

        "insight_emoji": insight_emoji,
        "insight_html":  insight_html,

        "subjects_list":      subjects_list_cv,
        "subject_marks_json": json.dumps({str(k): v for k, v in subject_marks.items()}),

        "is_teacher":   is_teacher,
        "is_principal": is_principal,
        "teacher_id":   request.session.get("teacher_id", ""),
    })


# ============================================================
#  CAREER ANALYSIS API
# ============================================================

@require_POST
def api_career_analysis(request):
    if not request.session.get("role"):
        return JsonResponse({"error": "Unauthorised"}, status=401)

    try:
        body = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    career_goal = body.get("career_goal", "").strip()
    student_id  = body.get("student_id")
    acad_yr     = body.get("acad_yr", "")

    if not career_goal:
        return JsonResponse({"error": "No career goal provided"}, status=400)

    full_subjects_df = pd.DataFrame()
    if student_id:
        try:
            full_subjects_df = load_student_subjects(int(student_id), acad_yr)
        except Exception:
            pass

    achievement_list_raw = body.get("achievement_list", "")
    parsed_achievements = _parse_achievements(achievement_list_raw)

    result = _generate_career_analysis(
        career_goal=career_goal,
        class_name=body.get("class_name", ""),
        overall_pct=float(body.get("overall_pct") or 0),
        full_subjects_df=full_subjects_df,
        student_name=body.get("student_name", ""),
        achievements=parsed_achievements,
    )

    confidence = []
    if not full_subjects_df.empty:
        sc = next((c for c in ("avg_percent", "score") if c in full_subjects_df.columns), None)
        if sc:
            for _, r in full_subjects_df.iterrows():
                s = pd.to_numeric(r[sc], errors="coerce")
                if pd.notna(s):
                    confidence.append({
                        "subject": str(r["subject_name"]),
                        "score": round(float(s), 1),
                        "level": "strong" if s >= 70 else ("developing" if s >= 50 else "weak")
                    })

    ach_signals = [
        {"subject": a["category"], "score": None, "level": "achievement",
         "signal": a["career_signal"], "title": a["title"]}
        for a in parsed_achievements
    ]

    return JsonResponse({"result": result, "confidence": confidence, "achievement_signals": ach_signals})


# ============================================================
#  SUBJECT → CAREER GROUND-TRUTH MAP
# ============================================================

_SUBJECT_CAREER_MAP = {
    "physics": [
        ("Engineering (Mechanical / Electrical / Civil)", "Physics is the foundation of all engineering disciplines"),
        ("Astronomy & Astrophysics", "Physics is the core science behind space and planetary study"),
        ("Architecture", "Physics (structural mechanics) is essential for architectural design"),
    ],
    "chemistry": [
        ("Medicine & Pharmacy", "Chemistry underpins pharmacology and medical diagnostics"),
        ("Chemical Engineering", "Chemistry is directly applied in industrial process design"),
        ("Biotechnology & Research", "Chemistry is essential for lab-based biological research"),
    ],
    "biology": [
        ("Medicine & Healthcare (MBBS / Nursing)", "Biology is the primary subject for medical careers"),
        ("Biotechnology & Genetics", "Biology drives research in genetic engineering and life sciences"),
        ("Environmental Science", "Biology supports ecology and conservation management"),
    ],
    "biotechnology": [
        ("Biotechnology & Pharmaceuticals", "Direct subject alignment with the biotech industry roles"),
        ("Genetic Research", "Biotechnology is the foundation of molecular research"),
    ],
    "science": [
        ("Medicine & Healthcare", "Science is the gateway to medical and health science careers"),
        ("Engineering", "Science fundamentals underpin all engineering streams"),
        ("Environmental Science", "Science supports ecology and environmental management"),
    ],
    "zoology": [
        ("Veterinary Science", "Zoology directly supports animal medicine careers"),
        ("Wildlife Biology & Conservation", "Zoology aligns with ecological field research"),
    ],
    "botany": [
        ("Agriculture & Plant Science", "Botany supports crop science and agri-tech"),
        ("Environmental Conservation", "Botany is core to plant ecology and conservation"),
    ],
    "mathematics": [
        ("Data Science & Analytics", "Mathematics is the backbone of data science and statistics"),
        ("Finance & Actuarial Science", "Strong Maths is required for banking and financial modelling"),
        ("Engineering", "Mathematics is essential across all engineering streams"),
    ],
    "maths": [
        ("Data Science & Analytics", "Mathematics is the backbone of data science and statistics"),
        ("Finance & Actuarial Science", "Strong Maths is required for banking and financial modelling"),
        ("Engineering", "Mathematics is essential across all engineering streams"),
    ],
    "applied mathematics": [
        ("Data Science & Analytics", "Applied Maths directly supports statistical and analytical roles"),
        ("Finance & Actuarial Science", "Applied Mathematics is core to financial modelling"),
    ],
    "statistics": [
        ("Data Science & Analytics", "Statistics is the primary tool in machine learning and analysis"),
        ("Economics & Policy Research", "Statistics is essential for economic modelling"),
    ],
    "computer science": [
        ("Software Engineering & Development", "Computer Science directly prepares students for coding roles"),
        ("Artificial Intelligence & Machine Learning", "CS is the gateway to AI/ML in the tech industry"),
        ("Cybersecurity", "Computer Science covers the fundamentals of network security"),
    ],
    "computer": [
        ("Software Engineering & Development", "Computer skills map directly to development careers"),
        ("IT Management & Systems Administration", "Computer knowledge supports enterprise system roles"),
    ],
    "computer applications": [
        ("Software Development", "Computer Applications covers practical programming and systems work"),
        ("IT Support & Systems", "Computer Applications prepares students for IT operations roles"),
    ],
    "information technology": [
        ("Software Development", "IT skills map directly to application and web development"),
        ("IT Management & Systems Administration", "IT supports networking and enterprise system roles"),
    ],
    "artificial intelligence": [
        ("AI/ML Engineering", "Direct subject alignment with the AI industry"),
        ("Data Science", "AI coursework overlaps strongly with data science"),
    ],
    "english": [
        ("Journalism & Media", "Strong English is essential for writing and broadcasting careers"),
        ("Law", "Legal careers require precise written and verbal communication"),
        ("Content & Communications", "English supports copywriting, PR, and content strategy"),
    ],
    "hindi": [
        ("Journalism & Regional Media", "Hindi fluency supports regional news and publishing careers"),
        ("Education & Teaching", "Hindi is a valued subject for teaching careers across India"),
    ],
    "marathi": [
        ("Regional Journalism & Publishing", "Marathi supports regional media and literary careers"),
        ("Education & Teaching (Marathi medium)", "Marathi qualifies students for teaching roles"),
    ],
    "sanskrit": [
        ("Linguistics & Classical Studies", "Sanskrit is directly relevant to academic language study"),
        ("Education & Teaching (Humanities)", "Sanskrit qualifies students for humanities teaching roles"),
        ("Philosophy & Religious Studies", "Sanskrit is foundational for Indian philosophy and theology"),
    ],
    "economics": [
        ("Finance & Banking", "Economics is directly aligned with banking and financial services"),
        ("Civil Services & Public Policy (IAS/IPS)", "Economics is a core subject for administrative roles"),
        ("Business Management (MBA)", "Economics provides analytical grounding for business careers"),
    ],
    "history": [
        ("Civil Services & Public Administration", "History is a valued optional for IAS exams"),
        ("Archaeology & Heritage Management", "History is the foundation for archaeological careers"),
        ("Law", "Historical and constitutional knowledge supports legal studies"),
    ],
    "geography": [
        ("Urban Planning & Civil Engineering", "Geography supports spatial planning and GIS careers"),
        ("Environmental Science & Conservation", "Geography is core to climate and ecology careers"),
        ("Geology & Earth Sciences", "Physical Geography is closely related to geological study"),
    ],
    "political science": [
        ("Civil Services (IAS/IPS/IFS)", "Political Science is the most directly aligned subject for admin careers"),
        ("Law & Governance", "Political Science supports legal and policy careers"),
        ("Journalism & Policy Analysis", "Political Science is essential for political reporting"),
    ],
    "civics": [
        ("Civil Services", "Civics is foundational for public administration careers"),
        ("Law & Governance", "Civics knowledge directly supports legal study"),
    ],
    "social science": [
        ("Civil Services & Public Administration", "Social Science is core to administrative and policy careers"),
        ("Journalism & Research", "Social Science supports qualitative research and reporting"),
    ],
    "psychology": [
        ("Clinical Psychology & Counselling", "Psychology is the direct pathway to mental health careers"),
        ("Human Resources & Organisational Behaviour", "Psychology supports people management roles"),
        ("Education & Special Needs Teaching", "Psychology is foundational for educational support roles"),
    ],
    "sociology": [
        ("Social Work & NGO Sector", "Sociology is the core subject for community development careers"),
        ("Civil Services", "Sociology is a popular optional for UPSC exams"),
        ("Research & Journalism", "Sociology supports qualitative research and investigative journalism"),
    ],
    "accountancy": [
        ("Chartered Accountancy (CA) & Auditing", "Accountancy is the direct pathway to CA careers"),
        ("Finance & Banking", "Accountancy skills are foundational for financial management"),
    ],
    "business studies": [
        ("Business Management & Entrepreneurship", "Business Studies directly supports MBA and startup careers"),
        ("Marketing & Sales", "Business Studies covers market research and sales strategy"),
    ],
    "art": [
        ("Graphic Design & Visual Communication", "Art skills are directly applicable to design and branding"),
        ("Architecture", "Art and spatial thinking are foundational for architecture"),
        ("Animation & Game Design", "Art is a core requirement for digital media careers"),
    ],
    "art & drawing": [
        ("Graphic Design & Visual Communication", "Drawing skills are directly applicable to design careers"),
        ("Animation & Illustration", "Art & Drawing is the foundation for animation and visual storytelling"),
        ("Architecture", "Drawing and spatial reasoning are core skills in architectural study"),
    ],
    "art & craft": [
        ("Graphic Design & Visual Communication", "Art & Craft skills support design and creative industry careers"),
        ("Product Design", "Craft skills are foundational for industrial and product design"),
    ],
    "physical education": [
        ("Sports Coaching & Athletic Training", "Physical Education directly supports coaching careers"),
        ("Physiotherapy & Sports Medicine", "PE is a strong foundation for physiotherapy careers"),
    ],
    "music": [
        ("Performing Arts & Music Production", "Music is directly aligned with performance and production careers"),
        ("Music Education & Therapy", "Music qualifications support teaching and therapy roles"),
    ],
    "dance": [
        ("Performing Arts & Choreography", "Dance directly supports performance and choreography careers"),
        ("Physical Education & Wellness", "Dance training supports fitness and wellness industry roles"),
    ],
    "evs": [
        ("Environmental Science", "EVS is the foundation for ecology and conservation careers"),
        ("Primary Education & Teaching", "EVS is a core subject for primary school teaching roles"),
    ],
    "gk": [
        ("Civil Services & Administration", "General Knowledge is essential for competitive exam preparation"),
        ("Journalism & Media", "Broad general knowledge is a core asset for journalism careers"),
    ],
}


def _get_grounded_careers(strong_subjects: list) -> list:
    import re
    seen, results = set(), []
    for subj in strong_subjects:
        subj_clean = re.sub(r"\s*\(.*?\)", "", str(subj).strip().lower()).strip()
        for key, careers in _SUBJECT_CAREER_MAP.items():
            if key in subj_clean or subj_clean in key:
                for career, reason in careers:
                    if career not in seen:
                        seen.add(career)
                        display_subj = re.sub(r"\s*\(.*?\)", "", str(subj)).strip()
                        results.append((career, reason, display_subj))
                break
    return results


def _get_grounded_careers_capped(strong_subjects: list) -> list:
    return _get_grounded_careers(strong_subjects)


def _dedupe_by_subject(careers: list, max_total: int = 3) -> list:
    seen_subjects = set()
    result = []
    for career, reason, subj in careers:
        subj_key = subj.strip().lower()
        if subj_key not in seen_subjects:
            seen_subjects.add(subj_key)
            result.append((career, reason, subj))
        if len(result) >= max_total:
            break
    return result



def _parse_achievements(achievement_list_raw: str) -> list:
    import re
    if not achievement_list_raw or str(achievement_list_raw).strip() in ("", "nan", "None"):
        return []
    ACHIEVEMENT_CAREER_MAP = {
        "cricket":    ("Sports & Athletics",           "Cricket signals physical discipline and team strategy"),
        "football":   ("Sports & Athletics",           "Football signals teamwork and physical fitness"),
        "basketball": ("Sports & Athletics",           "Basketball signals coordination and team play"),
        "badminton":  ("Sports & Athletics",           "Badminton signals agility and competitive drive"),
        "tennis":     ("Sports & Athletics",           "Tennis signals precision and competitive sport potential"),
        "swimming":   ("Sports & Athletics",           "Swimming signals endurance and athletic discipline"),
        "athletics":  ("Sports & Athletics",           "Athletics signals physical training and competitive sport"),
        "kho kho":    ("Sports & Athletics",           "Kho-Kho signals team coordination and agility"),
        "kabaddi":    ("Sports & Athletics",           "Kabaddi signals strength and team sport potential"),
        "chess":      ("Strategy & Academics",         "Chess signals analytical thinking and strategic planning"),
        "sport":      ("Sports & Athletics",           "Sports participation signals discipline and teamwork"),
        "dance":      ("Performing Arts",              "Dance signals creativity, discipline, and performance ability"),
        "singing":    ("Performing Arts",              "Singing signals musical talent and stage confidence"),
        "music":      ("Performing Arts",              "Music signals creative expression and discipline"),
        "drama":      ("Performing Arts",              "Drama signals communication skills and creative expression"),
        "theatre":    ("Performing Arts",              "Theatre signals stage presence and storytelling ability"),
        "cultural":   ("Performing Arts",              "Cultural events signal artistic interest and engagement"),
        "skit":       ("Performing Arts",              "Skit performance signals communication and dramatic expression"),
        "art":        ("Visual Arts & Design",         "Art signals visual creativity and design potential"),
        "drawing":    ("Visual Arts & Design",         "Drawing signals visual arts talent and design aptitude"),
        "painting":   ("Visual Arts & Design",         "Painting signals creative expression and fine arts potential"),
        "craft":      ("Visual Arts & Design",         "Craft signals hands-on creativity and design skills"),
        "poster":     ("Visual Arts & Design",         "Poster-making signals visual communication and design thinking"),
        "rangoli":    ("Visual Arts & Design",         "Rangoli signals cultural creativity and visual design ability"),
        "quiz":       ("Academic Excellence",          "Quiz signals broad knowledge and academic curiosity"),
        "olympiad":   ("Academic Excellence",          "Olympiad signals advanced subject mastery and ambition"),
        "essay":      ("Communication & Writing",      "Essay writing signals language proficiency and critical thinking"),
        "debate":     ("Communication & Writing",      "Debate signals communication, persuasion, and analytical skills"),
        "elocution":  ("Communication & Writing",      "Elocution signals public speaking and language confidence"),
        "speech":     ("Communication & Writing",      "Speech competition signals communication ability"),
        "spelling":   ("Communication & Writing",      "Spelling signals language precision and vocabulary breadth"),
        "leadership": ("Leadership & Organisation",    "Leadership signals initiative and organisational ability"),
        "prefect":    ("Leadership & Organisation",    "Prefect role signals leadership and peer respect"),
        "captain":    ("Leadership & Organisation",    "Captain role signals leadership and team motivation"),
        "monitor":    ("Leadership & Organisation",    "Monitor role signals responsibility and peer trust"),
        "head boy":   ("Leadership & Organisation",    "Head Boy signals exceptional leadership"),
        "head girl":  ("Leadership & Organisation",    "Head Girl signals exceptional leadership"),
    }
    results = []
    seen_categories = set()
    raw_items = re.split(r",(?=\s*\d{4}[-]|\s*[A-Z])", achievement_list_raw)
    for item in raw_items:
        item = item.strip()
        if not item:
            continue
        title = re.sub(r"^\d{4}[-]\d{4}\s*:\s*", "", item).strip()
        title_lower = title.lower()
        for keyword, (category, signal) in ACHIEVEMENT_CAREER_MAP.items():
            if keyword in title_lower and category not in seen_categories:
                seen_categories.add(category)
                results.append({"title": title, "category": category, "career_signal": signal})
                break
    return results

def _generate_career_analysis(career_goal, class_name, overall_pct,
                               full_subjects_df, student_name="", achievements=None):
    import re

    strong_list, weak_list = [], []
    if not full_subjects_df.empty:
        sc = next(
            (c for c in ("avg_percent", "score") if c in full_subjects_df.columns),
            None,
        )
        if sc:
            for _, r in full_subjects_df.iterrows():
                s = pd.to_numeric(r[sc], errors="coerce")
                if pd.notna(s):
                    if s >= 70:
                        strong_list.append(str(r["subject_name"]))
                    elif s < 50:
                        weak_list.append(str(r["subject_name"]))

    name_hint = student_name.split()[0].title() if student_name else "The student"
    pw = "exceptional" if overall_pct >= 85 else ("strong" if overall_pct >= 70 else "developing")
    art = "an" if pw[0] in "aeiou" else "a"
    strong_str = ", ".join(strong_list) if strong_list else "multiple subjects"
    if achievements is None:
        achievements = []
    ach_str = ", ".join(a["title"] + " (" + a["category"] + ")" for a in achievements) if achievements else "none recorded"
    senior = is_senior_class(class_name)

    grounded_careers = _get_grounded_careers_capped(strong_list)

    goal_lower = career_goal.strip().lower()
    goal_match = next(
        (c for c in grounded_careers if goal_lower in c[0].lower() or c[0].lower() in goal_lower),
        None
    )
    required_subjects = _get_subjects_for_goal(career_goal)
    goal_is_supported = goal_match is not None

    try:
        import google.generativeai as genai
        import os

        if grounded_careers:
            career_options_block = "\n".join(
                f"- {career} (because of {subj}: {reason})"
                for career, reason, subj in grounded_careers[:4]
            )
        else:
            career_options_block = "No strongly mapped careers found from current subjects."

        if goal_is_supported:
            goal_feedback = (
                f"✅ GOAL SUPPORTED: '{career_goal}' is directly supported by {goal_match[2]}. "
                f"Reason: {goal_match[1]}."
            )
        elif required_subjects:
            goal_feedback = (
                f"⚠️ GOAL NOT YET SUPPORTED: '{career_goal}' requires strong scores in "
                f"{required_subjects}, but these are not currently in the student's top subjects. "
                f"Be honest and encouraging — tell the student which subjects to focus on to reach this goal."
            )
        else:
            goal_feedback = (
                f"⚠️ GOAL UNCLEAR MATCH: '{career_goal}' could not be directly matched to "
                f"current subject scores. Encourage the student to explore subject alignment."
            )

        senior_section = ""
        if senior:
            senior_section = (
                "\nSuggested Colleges & Entrance Exams:\n"
                "* [specific entrance exam name] — for [career field]\n"
                "* [university type or example] — offering programmes in [field]"
            )

        goal_section_label = f"Goal: {career_goal.title()}:"

        if goal_is_supported:
            goal_section_template = (
                f"{goal_section_label}\n"
                f"* Key supporting subject: {goal_match[2]}\n"
                f"* Why it fits: {goal_match[1]}"
            )
        else:
            goal_section_template = (
                f"{goal_section_label}\n"
                f"* Subjects needed: {required_subjects}\n"
                "* Next step: Focus on these subjects in upcoming terms to keep this goal within reach."
            )

        display_careers_block = "\n".join(
            f"* {career} — {subj} — {reason}"
            for career, reason, subj in _dedupe_by_subject(grounded_careers, max_total=3)
        ) or "* Build stronger subject scores to unlock more specific career paths."

        prompt = (
            "You are a school academic counsellor writing a parent-facing career insight.\n\n"
            f"Student: {name_hint}\n"
            f"Class: {class_name}\n"
            f"Overall average: {overall_pct:.1f}%\n"
            f"Strong subjects: {strong_str}\n"
            f"Weak subjects: {', '.join(weak_list) if weak_list else 'none identified'}\n"
            f"Extracurricular achievements: {ach_str}\n"
            f"Stated career goal: {career_goal}\n\n"
            "GOAL ASSESSMENT:\n"
            f"{goal_feedback}\n\n"
            "PRE-VALIDATED CAREER OPTIONS (use ONLY these — never invent others):\n"
            f"{career_options_block}\n\n"
            "STRICT RULES:\n"
            "1. Only use careers from the PRE-VALIDATED list — never invent others.\n"
            "2. Never link language subjects (Sanskrit, Hindi, English, Marathi) to Science, Engineering, or Geology.\n"
            "3. Never link Art or PE to Medicine, Law, or Engineering.\n"
            "4. Only say a career is supported if that subject is in the student's strong list.\n"
            "5. Each bullet in Potential Career Paths must come from a DIFFERENT subject.\n"
            "6. Pick the single strongest career per subject — not all of them.\n"
            "7. Copy the output format EXACTLY as shown below — do not add extra sections or change labels.\n\n"
            "OUTPUT FORMAT (copy exactly, replace only the bracketed parts):\n\n"
            "[Sentence 1: {name_hint}'s academic profile summary mentioning all strong subjects.] "
            "[Sentence 2: honest one-sentence assessment of whether the career goal matches their subjects.]\n\n"
            f"{goal_section_template}\n\n"
            "Potential Career Paths:\n"
            f"{display_careers_block}"
            + senior_section +
            "\n\nUnder 220 words. Plain text only. No markdown. No bold. No extra sections."
        )

        genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
        return genai.GenerativeModel("gemini-1.5-flash").generate_content(prompt).text.strip()

    except Exception:
        pass

    # Static fallback
    lines = [
        f"{name_hint} demonstrates {art} {pw} academic profile with an overall average of "
        f"{overall_pct:.1f}%, showing particular strength in {strong_str}.",
    ]

    if goal_is_supported:
        lines.append(
            f"The goal of becoming a {career_goal} is well-supported — "
            f"{name_hint} already scores strongly in {goal_match[2]}, "
            f"which is a key subject for this path."
        )
        lines += [
            "",
            f"Goal: {career_goal.title()}:",
            f"* Key supporting subject: {goal_match[2]}",
            f"* Why it fits: {goal_match[1]}.",
            f"* Next step: Build on {goal_match[2]} and explore related entrance exams and college options early.",
        ]
    elif required_subjects:
        lines.append(
            f"To reach the goal of {career_goal}, {name_hint} will need to build strong scores "
            f"in {required_subjects} — these are the core subjects for this career."
        )
        lines += [
            "",
            f"Goal: {career_goal.title()}:",
            f"* Subjects needed: {required_subjects}",
            "* Next step: Focus on these subjects in upcoming terms to keep this goal within reach.",
        ]
    else:
        lines += [
            "",
            f"Goal: {career_goal.title()}:",
            "* This goal could not be directly matched to current subject scores.",
            "* Next step: Speak with a school counsellor to explore which subjects align with this career.",
        ]

    display_careers = _dedupe_by_subject(grounded_careers, max_total=3)
    if display_careers:
        lines += ["", "Potential Career Paths:"]
        for career, reason, subj in display_careers:
            lines.append(f"* {career} — {subj} — {reason}.")
    else:
        lines += ["", "Potential Career Paths:",
                  "* Focus on building subject scores to unlock more career path recommendations."]

    if senior and display_careers:
        lines += [
            "",
            "Suggested Colleges & Entrance Exams:",
            f"* Research entrance exams aligned with {display_careers[0][0]}.",
            f"* Look into universities offering programmes in {display_careers[0][2]}-related fields.",
            "* Speak with your school counsellor for institution-specific guidance.",
        ]

    return "\n".join(lines)


def _get_subjects_for_goal(career_goal: str) -> str:
    goal = career_goal.strip().lower()
    GOAL_REQUIREMENTS = {
        "doctor":               "Biology, Chemistry, Physics",
        "physician":            "Biology, Chemistry, Physics",
        "surgeon":              "Biology, Chemistry, Physics",
        "medicine":             "Biology, Chemistry, Physics",
        "mbbs":                 "Biology, Chemistry, Physics",
        "nurse":                "Biology, Chemistry",
        "nursing":              "Biology, Chemistry",
        "pharmacist":           "Chemistry, Biology, Mathematics",
        "pharmacy":             "Chemistry, Biology, Mathematics",
        "dentist":              "Biology, Chemistry, Physics",
        "physiotherapist":      "Biology, Physical Education",
        "veterinarian":         "Biology, Chemistry, Zoology",
        "vet":                  "Biology, Chemistry, Zoology",
        "engineer":             "Physics, Mathematics, Chemistry",
        "engineering":          "Physics, Mathematics, Chemistry",
        "mechanical":           "Physics, Mathematics",
        "electrical":           "Physics, Mathematics",
        "civil engineer":       "Physics, Mathematics, Geography",
        "software engineer":    "Computer Science, Mathematics",
        "software":             "Computer Science, Mathematics",
        "programmer":           "Computer Science, Mathematics",
        "coder":                "Computer Science, Mathematics",
        "developer":            "Computer Science, Mathematics",
        "web developer":        "Computer Science, Mathematics",
        "app developer":        "Computer Science, Mathematics",
        "robotics":             "Physics, Mathematics, Computer Science",
        "cybersecurity":        "Computer Science, Mathematics",
        "data scientist":       "Mathematics, Statistics, Computer Science",
        "data science":         "Mathematics, Statistics, Computer Science",
        "data analyst":         "Mathematics, Statistics, Computer Science",
        "actuary":              "Mathematics, Statistics, Economics",
        "banker":               "Economics, Mathematics, Accountancy",
        "banking":              "Economics, Mathematics, Accountancy",
        "ca":                   "Accountancy, Mathematics, Economics",
        "chartered accountant": "Accountancy, Mathematics, Economics",
        "accountant":           "Accountancy, Mathematics",
        "economist":            "Economics, Mathematics, Statistics",
        "finance":              "Economics, Mathematics, Accountancy",
        "investment":           "Economics, Mathematics, Statistics",
        "lawyer":               "English, Political Science, History",
        "advocate":             "English, Political Science, History",
        "law":                  "English, Political Science, History",
        "judge":                "English, Political Science, History",
        "ias":                  "History, Geography, Political Science, Economics",
        "ips":                  "History, Geography, Political Science, Economics",
        "civil services":       "History, Geography, Political Science, Economics",
        "upsc":                 "History, Geography, Political Science, Economics",
        "diplomat":             "History, Political Science, Economics, English",
        "politician":           "Political Science, History, Economics",
        "architect":            "Mathematics, Art, Physics",
        "architecture":         "Mathematics, Art, Physics",
        "artist":               "Art, Art & Drawing, Art & Craft",
        "painter":              "Art, Art & Drawing, Art & Craft",
        "sculptor":             "Art, Art & Drawing",
        "designer":             "Art, Art & Drawing, Mathematics",
        "graphic designer":     "Art, Art & Drawing, Computer Science",
        "fashion designer":     "Art, Art & Drawing",
        "interior designer":    "Art, Mathematics",
        "animator":             "Art, Art & Drawing, Computer Science",
        "illustrator":          "Art, Art & Drawing",
        "game designer":        "Art, Computer Science, Mathematics",
        "photographer":         "Art, Art & Drawing",
        "journalist":           "English, Political Science, History",
        "reporter":             "English, Political Science, History",
        "anchor":               "English, Hindi",
        "actor":                "English, Hindi",
        "actress":              "English, Hindi",
        "filmmaker":            "Art, English",
        "film maker":           "Art, English",
        "director":             "Art, English",
        "writer":               "English, Hindi",
        "author":               "English, Hindi",
        "content creator":      "English, Computer Science",
        "youtuber":             "English, Computer Science",
        "teacher":              "The subject you want to teach + English",
        "professor":            "The subject you want to teach + English",
        "psychologist":         "Psychology, Biology, English",
        "counsellor":           "Psychology, English, Sociology",
        "social worker":        "Sociology, Psychology, English",
        "scientist":            "Physics, Chemistry, Biology, Mathematics",
        "biologist":            "Biology, Chemistry, Mathematics",
        "chemist":              "Chemistry, Biology, Mathematics",
        "physicist":            "Physics, Mathematics",
        "astronomer":           "Physics, Mathematics",
        "astronaut":            "Physics, Mathematics, Biology",
        "geologist":            "Geography, Physics, Chemistry",
        "geology":              "Geography, Physics, Chemistry",
        "environmentalist":     "Biology, Geography, Chemistry",
        "botanist":             "Botany, Biology, Chemistry",
        "zoologist":            "Zoology, Biology, Chemistry",
        "cricketer":            "Physical Education, Mathematics",
        "cricket":              "Physical Education, Mathematics",
        "footballer":           "Physical Education",
        "football":             "Physical Education",
        "athlete":              "Physical Education, Biology",
        "sports":               "Physical Education, Biology",
        "coach":                "Physical Education, Psychology",
        "fitness trainer":      "Physical Education, Biology",
        "chef":                 "Chemistry, Home Science",
        "cook":                 "Chemistry, Home Science",
        "hotel management":     "English, Business Studies, Home Science",
        "hospitality":          "English, Business Studies",
        "tourism":              "Geography, English, History",
        "entrepreneur":         "Business Studies, Economics, Mathematics",
        "business":             "Business Studies, Economics, Mathematics",
        "marketing":            "Business Studies, Economics, English",
        "musician":             "Music",
        "singer":               "Music",
        "dancer":               "Dance, Physical Education",
        "choreographer":        "Dance, Physical Education",
        "pilot":                "Physics, Mathematics",
        "aviation":             "Physics, Mathematics",
        "army":                 "Physics, Mathematics, Geography",
        "defence":              "Physics, Mathematics, Geography",
        "economist":            "Economics, Mathematics, Statistics",
    }
    for key, subjects in GOAL_REQUIREMENTS.items():
        if key in goal or goal in key:
            return subjects
    return ""


# ============================================================
#  EXTRACURRICULAR API VIEWS
# ============================================================
@require_POST
def api_eca_save(request):
    if not request.session.get("role"):
        return JsonResponse({"error": "Unauthorised"}, status=401)

    if request.content_type and "application/json" in request.content_type:
        try:
            payload = json.loads(request.body)
        except Exception:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        cert_b64, cert_fname, cert_mime = None, None, None
    else:
        payload = {
            "student_id":       request.POST.get("student_id"),
            "title":            request.POST.get("title", "").strip(),
            "category":         request.POST.get("category", "").strip(),
            "achievement_date": request.POST.get("achievement_date", "").strip(),
            "level":            request.POST.get("level", "").strip(),
            "position":         request.POST.get("position", "").strip() or None,
            "description":      request.POST.get("description", "").strip() or None,
        }
        cert_file = request.FILES.get("certificate")
        if cert_file:
            import base64
            payload["certificate_data"]     = base64.b64encode(cert_file.read()).decode("utf-8")
            payload["certificate_filename"] = cert_file.name
            payload["certificate_mime"]     = cert_file.content_type
        else:
            payload["certificate_data"]     = None
            payload["certificate_filename"] = None
            payload["certificate_mime"]     = None

    required = ("student_id", "title", "category", "achievement_date", "level")
    if not all(payload.get(k) for k in required):
        missing = [k for k in required if not payload.get(k)]
        log.error("ECA save: missing fields %s | payload=%s", missing, payload)
        return JsonResponse({"error": f"Missing fields: {missing}"}, status=400)

    payload["submitted_by_parent_id"] = request.session.get("parent_id")
    for field in ("position", "description", "certificate_filename",
                  "certificate_data", "certificate_mime"):
        payload.setdefault(field, None)

    log.info("ECA save payload (no cert): %s", {k:v for k,v in payload.items() if k != 'certificate_data'})

    try:
        ok = save_extracurricular_achievement(payload)
    except Exception as exc:
        log.exception("ECA save EXCEPTION: %s", exc)
        return JsonResponse({"error": str(exc)}, status=500)  # ← shows real error in browser

    if ok:
        return JsonResponse({"ok": True})
    return JsonResponse({"error": "Could not save achievement"}, status=500)
# @require_POST
# def api_eca_save(request):
#     if not request.session.get("role"):
#         return JsonResponse({"error": "Unauthorised"}, status=401)
 
#     # Accept both multipart (file upload) and JSON
#     if request.content_type and "application/json" in request.content_type:
#         try:
#             payload = json.loads(request.body)
#         except Exception:
#             return JsonResponse({"error": "Invalid JSON"}, status=400)
#         cert_b64, cert_fname, cert_mime = None, None, None
#     else:
#         payload = {
#             "student_id":       request.POST.get("student_id"),
#             "title":            request.POST.get("title", "").strip(),
#             "category":         request.POST.get("category", "").strip(),
#             "achievement_date": request.POST.get("achievement_date", "").strip(),
#             "level":            request.POST.get("level", "").strip(),
#             "position":         request.POST.get("position", "").strip() or None,
#             "description":      request.POST.get("description", "").strip() or None,
#         }
#         cert_file = request.FILES.get("certificate")
#         if cert_file:
#             cert_b64  = base64.b64encode(cert_file.read()).decode("utf-8")
#             cert_fname = cert_file.name
#             cert_mime  = cert_file.content_type
#         else:
#             cert_b64, cert_fname, cert_mime = None, None, None
 
#         payload["certificate_data"]     = cert_b64
#         payload["certificate_filename"] = cert_fname
#         payload["certificate_mime"]     = cert_mime
 
#     required = ("student_id", "title", "category", "achievement_date", "level")
#     if not all(payload.get(k) for k in required):
#         return JsonResponse({"error": "Missing required fields"}, status=400)
 
#     payload["submitted_by_parent_id"] = request.session.get("parent_id")
#     for field in ("position", "description", "certificate_filename",
#                   "certificate_data", "certificate_mime"):
#         payload.setdefault(field, None)
 
#     ok = save_extracurricular_achievement(payload)
#     if ok:
#         return JsonResponse({"ok": True})
#     return JsonResponse({"error": "Could not save achievement"}, status=500)


@require_POST
def api_eca_update(request):
    if not request.session.get("role"):
        return JsonResponse({"error": "Unauthorised"}, status=401)
 
    if request.content_type and "application/json" in request.content_type:
        try:
            payload = json.loads(request.body)
        except Exception:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
    else:
        payload = {
            "id":               request.POST.get("id"),
            "title":            request.POST.get("title", "").strip(),
            "category":         request.POST.get("category", "").strip(),
            "achievement_date": request.POST.get("achievement_date", "").strip(),
            "level":            request.POST.get("level", "").strip(),
            "position":         request.POST.get("position", "").strip() or None,
            "description":      request.POST.get("description", "").strip() or None,
        }
        cert_file = request.FILES.get("certificate")
        if cert_file:
            payload["certificate_data"]     = base64.b64encode(cert_file.read()).decode("utf-8")
            payload["certificate_filename"] = cert_file.name
            payload["certificate_mime"]     = cert_file.content_type
 
    if not payload.get("id"):
        return JsonResponse({"error": "Missing id"}, status=400)
 
    ok = update_extracurricular_achievement(payload)
    if ok:
        return JsonResponse({"ok": True})
    return JsonResponse({"error": "Could not update achievement"}, status=500)


@require_POST
def api_eca_delete(request):
    if not request.session.get("role"):
        return JsonResponse({"error": "Unauthorised"}, status=401)
    try:
        body = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    record_id = body.get("id")
    if not record_id:
        return JsonResponse({"error": "Missing id"}, status=400)

    ok = delete_extracurricular_achievement(int(record_id))
    if ok:
        return JsonResponse({"status": "deleted"})
    return JsonResponse({"error": "Could not delete achievement"}, status=500)


# ============================================================
#  LOGIN HELPERS
# ============================================================

_VALID_TABS = {"Parent", "Teacher", "Principal"}


def _lookup_teacher(teacher_id: int):
    try:
        conn = _get_connection()
        cur  = conn.cursor()
        cur.execute(
            "SELECT name, designation, class_id, section_id "
            "FROM dim_teachers WHERE teacher_id = %s",
            (teacher_id,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        name, desig, class_id, section_id = row

        desig_lower = (desig or "").strip().lower()

        # ── Widen principal check — covers "vice principal", "vice-principal", etc.
        is_principal = any(kw in desig_lower for kw in (
            "principal", "vice principal", "vice-principal",
            "headmaster", "headmistress", "head master", "head mistress",
        ))

        # ── Don't gate on designation keywords at all — if the row exists in
        #    dim_teachers the person is staff. Remove the old hard-coded keyword
        #    filter that was silently returning None.
        return {
            "name":         name,
            "designation":  desig,
            "class_id":     class_id,
            "section_id":   section_id,
            "is_principal": is_principal,
        }
    except Exception as e:
        log.warning("Teacher lookup error: %s", e)
        return None


# ============================================================
#  LOGIN VIEW
# ============================================================

def login_view(request):
    if request.session.get("role"):
        return redirect("student_view")

    raw_tab = (
        request.GET.get("tab")
        or request.POST.get("tab")
        or request.session.get("login_tab")
        or "Parent"
    )
    tab = raw_tab if raw_tab in _VALID_TABS else "Parent"
    request.session["login_tab"] = tab

    is_login_attempt = (
        request.method == "POST"
        and request.POST.get("action") == "login"
    )
    if not is_login_attempt:
        return render(request, "login.html", {"tab": tab})

    user_id  = request.POST.get("user_id",  "").strip()
    password = request.POST.get("password", "").strip()
    error    = None

    if not user_id or not password:
        error = "Please enter your User ID and password."
    else:
        # ── STEP 1: Find school from master DB ───────────────────
        from dashboard.school_registry import get_school_for_user, get_or_register_school_db
        from dashboard.data_loader import set_active_school_db

        school_id, role_table = get_school_for_user(user_id)

        if school_id is None:
            error = "Your account is not linked to any school. Contact admin."
        else:
            try:
                school_db_alias = get_or_register_school_db(school_id)
            except ValueError as e:
                school_db_alias = None
                error = f"School database not configured: {e}"

        if not error:
            # ── STEP 2: Activate school DB for this thread ───────
            set_active_school_db(school_db_alias)

            # ── STEP 3: Authenticate against that school's DB ────
            user = authenticate_user(user_id, password)
            print(f"DEBUG: user_id={user_id}, tab={tab}, school_id={school_id}, "
                  f"alias={school_db_alias}, user={user}")

            if user is None:
                error = "Invalid credentials. Please try again."

            elif tab == "Parent" and user["role_id"] != ROLE_PARENT:
                error = "This account is not registered as a Parent."

            elif tab == "Teacher" and user["role_id"] != ROLE_TEACHER:
                error = "This account is not registered as a Teacher."

            elif tab == "Principal" and user["role_id"] != ROLE_PRINCIPAL:
                error = "This account is not registered as a Principal."

            elif tab == "Parent":
                request.session.flush()
                request.session.update({
                    "role":            "parent",
                    "parent_id":       user["reg_id"],
                    "user_id":         user["user_id"],
                    "name":            user["name"],
                    "school_id":       school_id,
                    "school_db_alias": school_db_alias,
                })
                set_active_school_db(school_db_alias)
                return redirect("student_view")

            elif tab == "Teacher":
                record = _lookup_teacher(user["reg_id"])
                if record is None:
                    error = "Teacher record not found. Contact admin."
                elif record["is_principal"]:
                    error = "This account has Principal access. Please use the Principal tab."
                else:
                    request.session.flush()
                    request.session.update({
                        "role":             "admin",
                        "admin_type":       "teacher",
                        "user_id":          user["user_id"],
                        "name":             user["name"],
                        "teacher_id":       user["reg_id"],
                        "teacher_name":     record["name"],
                        "teacher_desig":    record["designation"],
                        "teacher_class_id": record["class_id"],
                        "teacher_sec_id":   record["section_id"],
                        "parent_id":        None,
                        "school_id":        school_id,
                        "school_db_alias":  school_db_alias,
                    })
                    set_active_school_db(school_db_alias)
                    return redirect("student_view")

            elif tab == "Principal":
                request.session.flush()
                request.session.update({
                    "role":            "admin",
                    "admin_type":      "principal",
                    "user_id":         user["user_id"],
                    "name":            user["name"],
                    "teacher_id":      user["reg_id"],
                    "teacher_name":    user["name"],
                    "teacher_desig":   "Principal",
                    "parent_id":       None,
                    "school_id":       school_id,
                    "school_db_alias": school_db_alias,
                })
                # Enrich with actual name/designation from dim_teachers if available
                record = _lookup_teacher(user["reg_id"])
                if record:
                    request.session["teacher_name"]  = record["name"]
                    request.session["teacher_desig"] = record["designation"]
                set_active_school_db(school_db_alias)
                return redirect("student_view")
    return render(request, "login.html", {"tab": tab, "error": error})


# ============================================================
#  LOGOUT VIEW
# ============================================================

def logout_view(request):
    from django.contrib.auth import logout as auth_logout
    auth_logout(request)
    request.session.flush()
    return redirect("login")