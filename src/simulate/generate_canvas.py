import os
import json
from datetime import datetime, timedelta

#!/usr/bin/env python3
"""
Generate sample Canvas JSONL export files with a few intentional dirty records:
- Duplicate submission id (tests uniqueness)
- A user missing the 'email' field (tests completeness)
- A submission referencing a non-existent user_id (tests referential integrity)
Outputs written to data/out/:
- canvas_users.jsonl
- canvas_courses.jsonl
- canvas_enrollments.jsonl
- canvas_submissions.jsonl
"""


OUT_DIR = os.path.join("data", "out")


def iso_now(offset_days=0):
    return (datetime.utcnow() + timedelta(days=offset_days)).isoformat() + "Z"


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def generate_users():
    # One user intentionally missing 'email' key to simulate incomplete data
    users = [
        {"id": 1, "name": "Alice Chen", "email": "alice.chen@example.edu", "updated_at": iso_now(-2)},
        {"id": 2, "name": "Bob Smith", "email": "bob.smith@example.edu", "updated_at": iso_now(-1)},
        {"id": 3, "name": "Carol Zhang", "email": "carol.zhang@example.edu", "updated_at": iso_now()},
        {"id": 4, "name": "David Lee", "updated_at": iso_now()},  # MISSING email (dirty)
        {"id": 5, "name": "Eve Kim", "email": "eve.kim@example.edu", "updated_at": iso_now(-3)},
    ]
    return users


def generate_courses():
    courses = [
        {"id": 101, "course_code": "MATH101", "term": "2025 Spring", "updated_at": iso_now(-10)},
        {"id": 102, "course_code": "CS201", "term": "2025 Spring", "updated_at": iso_now(-5)},
        {"id": 103, "course_code": "HIST300", "term": "2024 Fall", "updated_at": iso_now(-30)},
    ]
    return courses


def generate_enrollments(users, courses):
    # create enrollments linking users and courses
    enrollments = [
        {"id": 1001, "user_id": 1, "course_id": 101, "status": "active", "updated_at": iso_now(-9)},
        {"id": 1002, "user_id": 2, "course_id": 101, "status": "active", "updated_at": iso_now(-8)},
        {"id": 1003, "user_id": 3, "course_id": 102, "status": "completed", "updated_at": iso_now(-2)},
        {"id": 1004, "user_id": 4, "course_id": 102, "status": "inactive", "updated_at": iso_now(-1)},
        {"id": 1005, "user_id": 5, "course_id": 103, "status": "active", "updated_at": iso_now(-4)},
        # additional enrollment
        {"id": 1006, "user_id": 1, "course_id": 102, "status": "active", "updated_at": iso_now(-6)},
    ]
    return enrollments


def generate_submissions():
    # Prepare submissions. Introduce:
    # - duplicate submission id (two records with id 5001)
    # - a submission with user_id that doesn't exist (e.g., 9999)
    # Other submissions reference existing users (1-5) and courses (101-103)
    submissions = [
        {"id": 5001, "user_id": 1, "course_id": 101, "submitted_at": iso_now(-7), "score": 88.5, "updated_at": iso_now(-7)},
        {"id": 5002, "user_id": 2, "course_id": 101, "submitted_at": iso_now(-6), "score": 92.0, "updated_at": iso_now(-6)},
        {"id": 5003, "user_id": 3, "course_id": 102, "submitted_at": iso_now(-5), "score": 76.0, "updated_at": iso_now(-5)},
        # Duplicate submission id (dirty)
        {"id": 5001, "user_id": 1, "course_id": 101, "submitted_at": iso_now(-4), "score": 90.0, "updated_at": iso_now(-4)},
        # Submission referencing a non-existent user (dirty)
        {"id": 5004, "user_id": 9999, "course_id": 103, "submitted_at": iso_now(-3), "score": 50.0, "updated_at": iso_now(-3)},
        # Another normal submission
        {"id": 5005, "user_id": 5, "course_id": 103, "submitted_at": iso_now(-2), "score": 85.0, "updated_at": iso_now(-2)},
    ]
    return submissions


def main():
    ensure_out_dir()

    users = generate_users()
    courses = generate_courses()
    enrollments = generate_enrollments(users, courses)
    submissions = generate_submissions()

    write_jsonl(os.path.join(OUT_DIR, "canvas_users.jsonl"), users)
    write_jsonl(os.path.join(OUT_DIR, "canvas_courses.jsonl"), courses)
    write_jsonl(os.path.join(OUT_DIR, "canvas_enrollments.jsonl"), enrollments)
    write_jsonl(os.path.join(OUT_DIR, "canvas_submissions.jsonl"), submissions)

    print(f"Wrote {len(users)} users, {len(courses)} courses, {len(enrollments)} enrollments, {len(submissions)} submissions to {OUT_DIR}")


if __name__ == "__main__":
    main()