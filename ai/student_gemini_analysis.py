import os
import re
import math
import hashlib
from google import genai
from django.conf import settings

# ==================================================
# CACHE  — avoids re-calling API for same student data
# ==================================================
_cache: dict = {}

def _cache_key(student: dict) -> str:
    fields = (
        str(student.get("student_name", "")),
        str(student.get("class_name", "")),
        str(student.get("avg_percent", "")),
        str(student.get("written_avg", "")),
        str(student.get("oral_avg", "")),
        str(student.get("strong_subjects", "")),
        str(student.get("strong_subjects_ai", "")),
        str(student.get("achievement_list", "")),
    )
    return hashlib.md5("|".join(fields).encode()).hexdigest()

# ==================================================
# CONFIG
# ==================================================

if not settings.GEMINI_API_KEY:
    raise Exception("GEMINI_API_KEY missing in settings")

genai.configure(api_key=settings.GEMINI_API_KEY)

# keep same variable names
_MODEL = "gemini-3.1-flash-lite-preview"   # use stable model (recommended)

_client = genai.GenerativeModel(
    _MODEL,
    generation_config={
        "temperature": 0.3,
        "max_output_tokens": 2800,
    }
)

# ==================================================
# SUBJECT → CAREER MAPPING (ground truth)
# ==================================================
# Maps each subject keyword to valid career fields and the reason why.
# The prompt will be built from this — the LLM cannot invent its own mappings.

SUBJECT_CAREER_MAP = {
    # Science subjects
    "physics": [
        ("Engineering (Mechanical, Electrical, Civil)", "Physics is the foundation of all engineering disciplines"),
        ("Astronomy & Astrophysics", "Physics is the core science behind space and planetary study"),
        ("Research & Academics", "Strong Physics skills suit university research and teaching"),
    ],
    "chemistry": [
        ("Medicine & Pharmacy", "Chemistry underpins pharmacology, drug development, and medical diagnostics"),
        ("Chemical Engineering", "Chemistry is directly applied in industrial process and materials design"),
        ("Biotechnology & Research", "Chemistry is essential for lab-based biological research"),
    ],
    "biology": [
        ("Medicine & Healthcare (MBBS, Nursing)", "Biology is the primary subject for medical and health science careers"),
        ("Biotechnology & Genetics", "Biology drives research in genetic engineering and life sciences"),
        ("Environmental Science", "Biology supports ecology, conservation, and environmental management"),
    ],
    "biotechnology": [
        ("Biotechnology & Pharmaceuticals", "Direct subject alignment with biotech industry roles"),
        ("Genetic Research", "Biotechnology is the foundation of genetic and molecular research"),
    ],
    "zoology": [
        ("Veterinary Science", "Zoology directly supports animal medicine and wildlife careers"),
        ("Wildlife Biology & Conservation", "Zoology aligns with ecological field research"),
    ],
    "botany": [
        ("Agriculture & Plant Science", "Botany supports crop science, horticulture, and agri-tech"),
        ("Environmental Conservation", "Botany is core to plant ecology and conservation work"),
    ],

    # Mathematics
    "mathematics": [
        ("Data Science & Analytics", "Mathematics (statistics, algebra) is the backbone of data science"),
        ("Finance & Actuarial Science", "Strong Maths skills are required for banking, insurance, and financial modelling"),
        ("Engineering", "Mathematics is essential across all engineering streams"),
    ],
    "maths": [
        ("Data Science & Analytics", "Mathematics (statistics, algebra) is the backbone of data science"),
        ("Finance & Actuarial Science", "Strong Maths skills are required for banking, insurance, and financial modelling"),
    ],
    "statistics": [
        ("Data Science & Analytics", "Statistics is the primary tool in data analysis and machine learning"),
        ("Economics & Policy Research", "Statistics is essential for economic modelling and public policy analysis"),
    ],

    # Computer Science / IT
    "computer science": [
        ("Software Engineering & Development", "Computer Science directly prepares students for coding and system design"),
        ("Artificial Intelligence & Machine Learning", "CS is the gateway to AI/ML roles in the tech industry"),
        ("Cybersecurity", "Computer Science covers the fundamentals of network security"),
    ],
    "information technology": [
        ("Software Development", "IT skills map directly to application and web development careers"),
        ("IT Management & Systems Administration", "IT supports infrastructure, networking, and enterprise system roles"),
    ],
    "artificial intelligence": [
        ("AI/ML Engineering", "Direct subject alignment with the AI and machine learning industry"),
        ("Data Science", "AI coursework overlaps strongly with data science and analytics"),
    ],

    # Languages
    "english": [
        ("Journalism & Media", "Strong English is essential for writing, reporting, and broadcasting"),
        ("Law", "Legal careers require precise written and verbal communication in English"),
        ("Content & Communications", "English proficiency supports copywriting, PR, and content strategy roles"),
    ],
    "hindi": [
        ("Journalism & Regional Media", "Hindi fluency supports careers in regional news, publishing, and broadcasting"),
        ("Education & Teaching", "Hindi is a valued subject for teaching careers across India"),
    ],
    "sanskrit": [
        ("Linguistics & Classical Studies", "Sanskrit is directly relevant to academic study of language and ancient texts"),
        ("Education & Teaching (Sanskrit/Humanities)", "Sanskrit qualifies students for teaching roles in humanities"),
        ("Philosophy & Religious Studies", "Sanskrit is foundational for studying Indian philosophy and theology"),
    ],

    # Social Sciences
    "economics": [
        ("Finance & Banking", "Economics is directly aligned with banking, investment, and financial services"),
        ("Public Policy & Civil Services (IAS/IPS)", "Economics is a core subject for administrative and policy roles"),
        ("Business Management (MBA)", "Economics provides strong analytical grounding for business careers"),
    ],
    "history": [
        ("Civil Services & Public Administration", "History is a valued optional subject for IAS and administrative exams"),
        ("Archaeology & Heritage Management", "History is the foundation for archaeological and museum careers"),
        ("Law", "Historical and constitutional knowledge supports legal studies"),
    ],
    "geography": [
        ("Urban Planning & Civil Engineering", "Geography supports spatial planning, infrastructure, and GIS careers"),
        ("Environmental Science & Conservation", "Geography is core to climate, ecology, and environmental management"),
        ("Geology & Earth Sciences", "Physical Geography is closely related to geological and earth science study"),
    ],
    "political science": [
        ("Civil Services (IAS/IPS/IFS)", "Political Science is the most directly aligned subject for administrative careers"),
        ("Law & Governance", "Political Science supports legal and policy careers"),
        ("Journalism & Policy Analysis", "Political Science is essential for political reporting and think-tank roles"),
    ],
    "psychology": [
        ("Clinical Psychology & Counselling", "Psychology is the direct pathway to mental health and counselling careers"),
        ("Human Resources & Organisational Behaviour", "Psychology supports people management and HR roles"),
        ("Education & Special Needs Teaching", "Psychology is foundational for child development and educational support"),
    ],
    "sociology": [
        ("Social Work & NGO Sector", "Sociology is the core subject for social welfare and community development"),
        ("Civil Services", "Sociology is a popular optional for UPSC and state civil service exams"),
        ("Research & Journalism", "Sociology supports qualitative research and investigative journalism"),
    ],

    # Commerce
    "accountancy": [
        ("Chartered Accountancy (CA) & Auditing", "Accountancy is the direct pathway to CA and financial audit careers"),
        ("Finance & Banking", "Accountancy skills are foundational for banking and financial management"),
    ],
    "business studies": [
        ("Business Management & Entrepreneurship", "Business Studies directly supports MBA and startup careers"),
        ("Marketing & Sales", "Business Studies covers market research, branding, and sales strategy"),
    ],

    # Arts / Vocational
    "art": [
        ("Graphic Design & Visual Communication", "Art skills are directly applicable to design, illustration, and branding"),
        ("Architecture", "Art and spatial thinking are foundational for architecture studies"),
        ("Animation & Game Design", "Art is a core requirement for careers in digital media and gaming"),
    ],
    "physical education": [
        ("Sports Coaching & Athletic Training", "Physical Education directly supports coaching and sports science careers"),
        ("Physiotherapy & Sports Medicine", "PE is a strong foundation for physiotherapy and rehabilitation careers"),
    ],
    "music": [
        ("Performing Arts & Music Production", "Music is directly aligned with performance, composition, and production careers"),
        ("Music Education & Therapy", "Music qualifications support teaching and music therapy roles"),
    ],
}

# ==================================================
# ACHIEVEMENT → SKILL MAP
# ==================================================

ACHIEVEMENT_SKILL_MAP = {
    "football": ["athletic discipline", "teamwork"],
    "cricket": ["athletic discipline", "teamwork"],
    "sports": ["athletic discipline"],

    "music": ["creativity", "performance confidence"],
    "dance": ["creativity", "performance confidence"],
    "art": ["creativity", "visual thinking"],
    "drawing": ["creativity", "visual thinking"],

    "debate": ["communication", "argumentation", "leadership"],
    "speech": ["communication"],
    "public speaking": ["communication"],

    "olympiad": ["analytical ability", "problem solving"],
    "science fair": ["analytical ability", "scientific curiosity"],
    "coding": ["logical thinking", "problem solving"],
    "robotics": ["engineering mindset", "problem solving"],

    "volunteer": ["empathy", "social responsibility"],
}

# ==================================================
# SKILL → CAREER MAP
# ==================================================

SKILL_CAREER_MAP = {
    "analytical ability": [
        ("Engineering", "Analytical thinking supports engineering problem solving"),
        ("Data Science", "Analytical ability aligns with statistical reasoning"),
    ],

    "problem solving": [
        ("Software Engineering", "Problem solving is central to software design"),
        ("Artificial Intelligence", "AI research relies on strong problem solving"),
    ],

    "communication": [
        ("Law", "Strong communication is essential for legal careers"),
        ("Journalism", "Communication skills support journalism and media"),
    ],

    "leadership": [
        ("Business Management", "Leadership supports management roles"),
        ("Civil Services", "Leadership aligns with administrative careers"),
    ],

    "creativity": [
        ("Graphic Design", "Creativity directly supports visual design careers"),
        ("Architecture", "Creative spatial thinking supports architecture"),
    ],

    "visual thinking": [
        ("Architecture", "Visual thinking is essential for architectural design"),
        ("Animation & Game Design", "Visual imagination supports animation careers"),
    ],

    "athletic discipline": [
        ("Professional Sports", "Athletic discipline supports sports careers"),
        ("Sports Coaching", "Sports experience supports coaching pathways"),
    ],

    "teamwork": [
        ("Business Management", "Team collaboration skills support management"),
    ],

    "scientific curiosity": [
        ("Scientific Research", "Curiosity drives research careers"),
        ("Biotechnology", "Scientific curiosity aligns with biotech fields"),
    ],
}

def _get_career_options_from_subjects(subjects_list: list) -> list:
    """
    Given a list of subject strings, return a deduplicated list of
    (career_field, reason, supporting_subject) tuples grounded in SUBJECT_CAREER_MAP.
    """
    seen_careers = set()
    results = []

    for subj in subjects_list:
        subj_lower = re.sub(r"\s*\(.*?\)", "", subj).strip().lower()
        for key, careers in SUBJECT_CAREER_MAP.items():
            if key in subj_lower or subj_lower in key:
                for career_field, reason in careers:
                    if career_field not in seen_careers:
                        seen_careers.add(career_field)
                        # Use the original subject name (cleaned) for display
                        display_subj = re.sub(r"\s*\(.*?\)", "", subj).strip()
                        results.append((career_field, reason, display_subj))

    return results


# ==================================================
# ACHIEVEMENT → CAREER OPTIONS
# ==================================================

def _get_career_options_from_achievements(achievement_text: str):

    if not achievement_text:
        return []

    achievement_text = achievement_text.lower()

    skills = set()

    for keyword, skill_list in ACHIEVEMENT_SKILL_MAP.items():
        if keyword in achievement_text:
            skills.update(skill_list)

    results = []
    seen = set()

    for skill in skills:
        for career_field, reason in SKILL_CAREER_MAP.get(skill, []):
            if career_field not in seen:
                seen.add(career_field)
                results.append((career_field, reason, skill))

    return results

# ==================================================
# HELPERS
# ==================================================
def _clean(value, fallback=None):
    if value is None:
        return fallback
    try:
        if math.isnan(float(value)) if isinstance(value, (int, float)) else False:
            return fallback
    except (TypeError, ValueError):
        pass
    s = str(value).strip()
    if s.lower() in ("nan", "none", "nat", "n/a", ""):
        return fallback
    return value


# ==================================================
# SUBJECT PRIORITIZATION
# ==================================================
def _prioritize_subjects(strong_subjects):
    if not strong_subjects:
        return [], []

    subjects_raw = str(strong_subjects).strip()
    all_subjects = [s.strip() for s in re.split(r"[,/|;]+", subjects_raw) if s.strip()]

    cleaned_subjects = []
    for subj in all_subjects:
        base = re.sub(r"\s*\(.*?\)", "", subj).strip()
        cleaned_subjects.append((base, subj))

    CORE_SCIENCE = ["Physics", "Chemistry", "Biology", "Biotechnology",
                    "Zoology", "Botany", "Science"]
    CORE_MATH    = ["Mathematics", "Maths", "Math", "Statistics",
                    "Applied Mathematics"]
    CORE_LANG    = ["English", "Hindi", "Sanskrit", "Tamil", "Telugu",
                    "Marathi", "Urdu"]
    CORE_COMP    = ["Computer Science", "Computer", "CS",
                    "Information Technology", "IT",
                    "Artificial Intelligence", "AI"]
    CORE_SOCIAL  = ["Economics", "History", "Geography",
                    "Political Science", "Civics", "Psychology", "Sociology"]

    core, supporting = [], []

    for base, original in cleaned_subjects:
        base_lower = base.lower()
        is_core = False
        for core_list in [CORE_SCIENCE, CORE_MATH, CORE_LANG, CORE_COMP, CORE_SOCIAL]:
            if any(c.lower() in base_lower for c in core_list):
                core.append(original)
                is_core = True
                break
        if not is_core:
            supporting.append(original)

    return core, supporting


# ==================================================
# SCIENCE COMPONENT PARSER
# ==================================================
def _parse_science_components(subjects_str):
    result = {}
    if not subjects_str:
        return result

    for part in str(subjects_str).split(","):
        part = part.strip()
        match = re.match(r"^(Science)\s*\((.+)\)$", part, re.IGNORECASE)
        if match:
            components = {}
            for comp in match.group(2).split(","):
                comp = comp.strip()
                if "not assessed" in comp.lower():
                    name = re.sub(r"\s*\(not assessed\)", "", comp, flags=re.IGNORECASE).strip()
                    components[name] = False
                else:
                    components[comp] = True
            result["Science"] = components

    return result


def _science_component_note(subjects_ai_str):
    sci_map = _parse_science_components(subjects_ai_str)
    if "Science" not in sci_map:
        return ""

    components   = sci_map["Science"]
    assessed     = [k for k, v in components.items() if v is True]
    not_assessed = [k for k, v in components.items() if v is False]

    parts = []
    if assessed:
        parts.append(f"Strong in {', '.join(assessed)} within Science")
    if not_assessed:
        parts.append(
            f"{', '.join(not_assessed)} mark{'s' if len(not_assessed) > 1 else ''} "
            f"{'were' if len(not_assessed) > 1 else 'was'} not recorded this year"
        )

    return ". ".join(parts) + "." if parts else ""


# ==================================================
# PROMPT BUILDER
# ==================================================
def _build_prompt(student, history_df=None):
    student_name    = _clean(student.get("student_name"), "Student")
    overall_avg     = _clean(student.get("avg_percent"))
    written_avg     = _clean(student.get("written_avg"))
    internal_avg    = _clean(student.get("oral_avg"))
    strong_subjects = _clean(student.get("strong_subjects"))
    subjects_ai     = _clean(student.get("strong_subjects_ai"), strong_subjects)
    achievements = _clean(student.get("achievement_list"))
    class_name      = _clean(student.get("class_name"), "")

    class_match = re.search(r"(\d+)", str(class_name))
    class_level = int(class_match.group(1)) if class_match else 0
    is_senior   = class_level >= 7

    has_data = any([overall_avg, written_avg, internal_avg, strong_subjects])
    if not has_data:
        return None, None, is_senior, False

    core, supporting = _prioritize_subjects(subjects_ai or strong_subjects or "")
    sci_note = _science_component_note(str(subjects_ai or ""))

    # Determine communication strength
    comm_style = "unknown"
    if internal_avg and written_avg:
        try:
            o, w = float(internal_avg), float(written_avg)
            if o > w + 10:
                comm_style = "stronger in oral/internal assessments (participative learner)"
            elif w > o + 10:
                comm_style = "stronger in written exams (independent studier)"
            else:
                comm_style = "balanced across oral and written assessments"
        except (ValueError, TypeError):
            pass

    # Historical trend
    trend_note = ""
    if history_df is not None and not history_df.empty and "avg_percent" in history_df.columns:
        scores = history_df["avg_percent"].dropna().tolist()
        if len(scores) >= 2:
            direction = (
                "improving" if scores[-1] > scores[0]
                else "declining" if scores[-1] < scores[0]
                else "steady"
            )
            trend_note = f"Historical trend: {direction} (from {scores[0]:.1f}% to {scores[-1]:.1f}%)"

    # === KEY FIX: Pre-compute grounded career options from SUBJECT_CAREER_MAP ===
    all_subjects = core + supporting

    subject_careers = _get_career_options_from_subjects(all_subjects)
    achievement_careers = _get_career_options_from_achievements(achievements)

    career_options = subject_careers + achievement_careers

    # Limit to top 4 most relevant (first matched = highest priority subjects first)
    career_options = career_options[:4]

    if career_options:
        career_block = "\n".join(
            f"- {field} (because of {subj}: {reason})"
            for field, reason, subj in career_options
        )
    else:
        career_block = "No strongly mapped careers identified from current subjects."

    # Assemble student context block
    lines = [
        f"Student name: {student_name}",
        f"Class: {class_name or 'unknown'}",
        f"Overall average: {overall_avg or 'not available'}%",
        f"Written exam average: {written_avg or 'not available'}%",
        f"Oral/internal average: {internal_avg or 'not available'}%",
        f"Core subjects: {', '.join(core) if core else 'not identified'}",
        f"Supporting subjects: {', '.join(supporting) if supporting else 'none'}",
        f"Achievements: {achievements or 'none recorded'}",
        f"Communication style: {comm_style}",
    ]
    if sci_note:
        lines.append(f"Science data note: {sci_note}")
    if trend_note:
        lines.append(trend_note)

    lines.append(
        f"\nPRE-VALIDATED CAREER OPTIONS (use ONLY these — do not invent others):\n{career_block}"
    )

    student_context = "\n".join(lines)

    system_prompt = (
        "You are an expert school counselor and academic advisor. "
        "You write clear, warm, specific insights for parents about their child's academic profile. "
        "Always be encouraging but honest. Never use hollow praise. "
        "CRITICAL RULE: You must ONLY suggest career paths from the PRE-VALIDATED CAREER OPTIONS list provided. "
        "Never invent or hallucinate career connections that are not in that list. "
        "Never connect a language subject like Sanskrit or English to fields like Geology or Engineering. "
        "Never add a preamble, greeting, or sign-off — output only the insight text."
    )

    if is_senior:
        user_prompt = (
            f"{student_context}\n\n"
            "Write a concise dashboard insight for parents.\n\n"
            "Structure:\n"
            "1. 2–3 sentences summarizing the student's academic strengths and subject performance.\n"
            "2. Section titled 'Aligned Career Directions' with 2–3 bullet points.\n"
            "   Each bullet must come from the PRE-VALIDATED CAREER OPTIONS list only.\n"
            "   Format: • Career field — why the student's specific strong subject supports it.\n"
            "3. Section titled 'Future Study Pathways' — mention example university types or "
            "well-known institutions offering programs in these fields.\n\n"
            "Tone: professional, encouraging, specific to the student's subjects.\n"
            "No greeting. No preamble. Do not suggest careers not in the validated list."
        )
    else:
        user_prompt = (
            f"{student_context}\n\n"
            "Write a concise, parent-friendly academic insight (80–120 words) for this junior student (Class 6 or below).\n"
            "Format:\n"
            "  - 1–2 sentences: upbeat performance summary\n"
            "  - 1 sentence listing 2–3 age-appropriate activity suggestions that match their strengths\n"
            "Tone: warm, celebratory. No career talk — focus on exploration and fun."
        )

    return system_prompt, user_prompt, is_senior, True


# ==================================================
# DASHBOARD INSIGHT  — Groq AI
# ==================================================
def generate_dashboard_insight(current_student, history_df=None):
    student = current_student

    key = _cache_key(student)
    if key in _cache:
        return _cache[key]

    try:
        system_prompt, user_prompt, is_senior, has_data = _build_prompt(student, history_df)

        if not has_data:
            return (
                "Not enough academic data is available yet. "
                "Once attendance, assessments, and subject performance are logged, "
                "a personalized learning profile will appear here."
            )

        response = _client.generate_content(
            contents=f"{system_prompt}\n\n{user_prompt}",
            generation_config={
                "temperature": 0.2,
                "max_output_tokens": 400,
            }
        )

        result = response.text.strip()
        _cache[key] = result
        return result

    except Exception as e:
        print(f"[ERROR] Gemini API error: {e}")
        import traceback
        traceback.print_exc()

        overall  = _clean(student.get("avg_percent"))
        subjects = _clean(student.get("strong_subjects"))
        if overall and subjects:
            core, _ = _prioritize_subjects(subjects)
            career_options = _get_career_options_from_subjects(core)
            if career_options:
                fields = ", ".join(f[0] for f in career_options[:2])
                return (
                    f"Strong performance across core subjects (overall {overall}%). "
                    f"Based on subject profile, potential directions include: {fields}."
                )
            core_str = ", ".join(
                re.sub(r"\s*\(.*?\)", "", s).strip() for s in core[:2]
            ) or subjects[:40]
            return (
                f"Strong performance in {core_str} (overall {overall}%). "
                "Detailed career mapping will appear once more assessment data is available."
            )
        return (
            "Academic data is still being collected. "
            "Check back after the next assessment cycle."
        )


# ==================================================
# CLASS SUMMARY  — Groq AI
# ==================================================
def generate_class_ai_summary(class_df):
    try:
        student_count = len(class_df)
        avg_perf = (
            round(class_df["avg_percent"].mean(), 1)
            if "avg_percent" in class_df.columns else None
        )
        avg_att = (
            round(class_df["attendance_percentage"].mean(), 1)
            if "attendance_percentage" in class_df.columns else None
        )

        context_parts = [f"{student_count} students"]
        if avg_perf:
            context_parts.append(f"class average score: {avg_perf}%")
        if avg_att:
            context_parts.append(f"average attendance: {avg_att}%")

        response = _client.generate_content(
            model=_MODEL,
            max_tokens=150,
            temperature=0.5,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a school analytics assistant. "
                        "Write brief, professional class summaries for teacher dashboards."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Class data: {', '.join(context_parts)}.\n\n"
                        "Write a single short paragraph (2–3 sentences) summarizing this class. "
                        "Cover engagement level, academic standing, and one actionable suggestion. "
                        "No bullet points. No preamble."
                    ),
                },
            ],
        )
        return response.choices[0].message.content.strip()

    except Exception:
        return f"Class of {len(class_df)} students with diverse strengths."


# ==================================================
# DETAILED REPORT (reserved for counselors)
# ==================================================
def generate_detailed_student_report(student, history_df=None):
    return "Detailed counselor report preserved (not shown in parent dashboard)."


# ==================================================
# TEST
# ==================================================
if __name__ == "__main__":
    test_students = [
        {
            "student_name": "Anaya Mehta",
            "class_name": "Class 10",
            "avg_percent": 92.6,
            "written_avg": 90,
            "oral_avg": 94,
            "strong_subjects": "Sanskrit, English, Art & Drawing",
            "strong_subjects_ai": "Sanskrit, English, Art & Drawing",
        },
        {
            "student_name": "Priya Sharma",
            "class_name": "Class 10",
            "avg_percent": 82,
            "written_avg": 78,
            "oral_avg": 91,
            "strong_subjects": "Science (Biology, Chemistry (not assessed)), English, Maths",
            "strong_subjects_ai": "Science (Biology, Chemistry (not assessed)), English, Maths",
        },
        {
            "student_name": "Arjun Patel",
            "class_name": "Class 4",
            "avg_percent": 74,
            "written_avg": 72,
            "oral_avg": 76,
            "strong_subjects": "Mathematics, Art, Physical Education",
            "strong_subjects_ai": "Mathematics, Art, Physical Education",
        },
        {
            "student_name": "Sneha Rao",
            "class_name": "Class 12",
            "avg_percent": 91,
            "written_avg": 94,
            "oral_avg": 80,
            "strong_subjects": "Physics, Mathematics, Computer Science",
            "strong_subjects_ai": "Physics, Mathematics, Computer Science",
        },
    ]

    for s in test_students:
        print(f"\n{'='*60}")
        print(f"Student: {s['student_name']} | {s['class_name']}")
        print(f"{'='*60}")
        insight = generate_dashboard_insight(s)
        print(insight)
