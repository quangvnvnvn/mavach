from flask import Flask, request, redirect, url_for, render_template_string, session, jsonify, flash, send_file
from functools import wraps
from uuid import uuid4
from datetime import datetime
from werkzeug.utils import secure_filename
import sqlite3
import os
import io
import re
from openpyxl import Workbook, load_workbook

app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET_KEY", "822231")

DB_FILE = "barcode_noi_bo.db"
EXPORT_FILE = "du_lieu_ma_vach.xlsx"
UPLOAD_BARCODE_FOLDER = "uploads_barcode_file"

os.makedirs(UPLOAD_BARCODE_FOLDER, exist_ok=True)

ALLOWED_BARCODE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".pdf"}

DEFAULT_ROWS = [
    {
        "id": str(uuid4()),
        "ten_thung": "Viên treo bồn cầu",
        "so_luong_san_pham_thung": "9 vỉ/thùng",
        "ten": "EverShine - Viên treo bồn cầu hương Chanh",
        "ma_vach_sp": "8935367400014",
        "so_ma_vach_thung": "38935367400015",
        "updated_by": "Super Admin",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    },
    {
        "id": str(uuid4()),
        "ten_thung": "Viên treo bồn cầu",
        "so_luong_san_pham_thung": "9 vỉ/thùng",
        "ten": "EverShine - Viên treo bồn cầu hương Lavender",
        "ma_vach_sp": "8935367400021",
        "so_ma_vach_thung": "38935367400022",
        "updated_by": "Super Admin",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    },
]

DEFAULT_SETTINGS = {
    "group_pattern": "3-5-5-1",
}


def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def format_grouped_number(value: str, pattern: str) -> str:
    digits = only_digits(value)
    if not digits:
        return value or ""
    try:
        groups = [int(x) for x in str(pattern).split("-") if x.strip()]
    except ValueError:
        groups = []
    if not groups:
        return digits

    out = []
    idx = 0
    for size in groups:
        if idx >= len(digits):
            break
        out.append(digits[idx:idx + size])
        idx += size
    if idx < len(digits):
        out.append(digits[idx:])
    return " ".join([x for x in out if x])


def allowed_barcode_file(filename: str) -> bool:
    ext = os.path.splitext(filename.lower())[1]
    return ext in ALLOWED_BARCODE_EXTENSIONS


def barcode_file_type(filename: str) -> str:
    ext = os.path.splitext(filename.lower())[1]
    if ext == ".pdf":
        return "pdf"
    return "image"


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admins (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE,
            password TEXT,
            role TEXT,
            display_name TEXT,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS barcode_rows (
            id TEXT PRIMARY KEY,
            ten_thung TEXT,
            so_luong_san_pham_thung TEXT,
            ten TEXT,
            ma_vach_sp TEXT,
            so_ma_vach_thung TEXT,
            updated_by TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS row_barcode_files (
            id TEXT PRIMARY KEY,
            row_id TEXT UNIQUE,
            original_name TEXT,
            saved_name TEXT,
            file_type TEXT,
            uploaded_by TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()

    cur.execute("PRAGMA table_info(admins)")
    admin_cols = [r[1] for r in cur.fetchall()]
    if "role" not in admin_cols:
        cur.execute("ALTER TABLE admins ADD COLUMN role TEXT DEFAULT 'admin'")
    if "display_name" not in admin_cols:
        cur.execute("ALTER TABLE admins ADD COLUMN display_name TEXT DEFAULT ''")

    cur.execute("PRAGMA table_info(barcode_rows)")
    row_cols = [r[1] for r in cur.fetchall()]
    if "updated_by" not in row_cols:
        cur.execute("ALTER TABLE barcode_rows ADD COLUMN updated_by TEXT DEFAULT 'Super Admin'")

    cur.execute("PRAGMA table_info(row_barcode_files)")
    barcode_cols = [r[1] for r in cur.fetchall()]
    if "file_type" not in barcode_cols:
        cur.execute("ALTER TABLE row_barcode_files ADD COLUMN file_type TEXT DEFAULT 'image'")

    conn.commit()

    cur.execute("SELECT COUNT(*) AS total FROM admins")
    if cur.fetchone()["total"] == 0:
        now = now_str()
        cur.executemany(
            "INSERT INTO admins (id, username, password, role, display_name, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (str(uuid4()), "superadmin", "123456", "super_admin", "Super Admin", now),
                (str(uuid4()), "admin1", "123456", "admin", "Admin 1", now),
                (str(uuid4()), "admin2", "123456", "admin", "Admin 2", now),
            ],
        )
        conn.commit()

    cur.execute("UPDATE admins SET role = 'super_admin' WHERE username = 'superadmin' AND (role IS NULL OR role = '')")
    cur.execute("UPDATE admins SET role = 'admin' WHERE role IS NULL OR role = ''")
    cur.execute("UPDATE admins SET display_name = username WHERE display_name IS NULL OR display_name = ''")

    for key, value in DEFAULT_SETTINGS.items():
        cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)", (key, value))

    cur.execute("SELECT COUNT(*) AS total FROM barcode_rows")
    if cur.fetchone()["total"] == 0:
        for row in DEFAULT_ROWS:
            cur.execute(
                """
                INSERT INTO barcode_rows (
                    id, ten_thung, so_luong_san_pham_thung, ten,
                    ma_vach_sp, so_ma_vach_thung, updated_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["ten_thung"],
                    row["so_luong_san_pham_thung"],
                    row["ten"],
                    row["ma_vach_sp"],
                    row["so_ma_vach_thung"],
                    row["updated_by"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        conn.commit()

    conn.close()


def get_settings():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM app_settings")
    settings = {r["key"]: r["value"] for r in cur.fetchall()}
    conn.close()
    merged = DEFAULT_SETTINGS.copy()
    merged.update(settings)
    return merged


def list_barcode_file_map():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM row_barcode_files")
    rows = {r["row_id"]: dict(r) for r in cur.fetchall()}
    conn.close()
    return rows


def list_rows(search=""):
    conn = get_db()
    cur = conn.cursor()
    if search.strip():
        like = f"%{search.strip()}%"
        cur.execute(
            """
            SELECT * FROM barcode_rows
            WHERE ten_thung LIKE ?
               OR so_luong_san_pham_thung LIKE ?
               OR ten LIKE ?
               OR ma_vach_sp LIKE ?
               OR so_ma_vach_thung LIKE ?
               OR updated_by LIKE ?
            ORDER BY ten_thung ASC, ten ASC
            """,
            (like, like, like, like, like, like),
        )
    else:
        cur.execute("SELECT * FROM barcode_rows ORDER BY ten_thung ASC, ten ASC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    settings = get_settings()
    barcode_map = list_barcode_file_map()

    for row in rows:
        row["ma_vach_sp_grouped"] = format_grouped_number(row["ma_vach_sp"], settings["group_pattern"])
        row["so_ma_vach_thung_grouped"] = format_grouped_number(row["so_ma_vach_thung"], settings["group_pattern"])
        row["barcode_file"] = barcode_map.get(row["id"])
        row["has_barcode_file"] = row["id"] in barcode_map
        row["barcode_file_type"] = barcode_map.get(row["id"], {}).get("file_type", "")
    return rows


def get_row(row_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM barcode_rows WHERE id = ?", (row_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_row(form, updated_by):
    now = now_str()
    ma_vach_sp = only_digits(form.get("ma_vach_sp", ""))
    so_ma_vach_thung = only_digits(form.get("so_ma_vach_thung", ""))

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO barcode_rows (
            id, ten_thung, so_luong_san_pham_thung, ten,
            ma_vach_sp, so_ma_vach_thung, updated_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            form.get("ten_thung", "").strip(),
            form.get("so_luong_san_pham_thung", "").strip(),
            form.get("ten", "").strip(),
            ma_vach_sp,
            so_ma_vach_thung,
            updated_by,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()


def update_row_db(row_id, form, updated_by):
    now = now_str()
    ma_vach_sp = only_digits(form.get("ma_vach_sp", ""))
    so_ma_vach_thung = only_digits(form.get("so_ma_vach_thung", ""))

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE barcode_rows
        SET ten_thung = ?, so_luong_san_pham_thung = ?, ten = ?, ma_vach_sp = ?,
            so_ma_vach_thung = ?, updated_by = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            form.get("ten_thung", "").strip(),
            form.get("so_luong_san_pham_thung", "").strip(),
            form.get("ten", "").strip(),
            ma_vach_sp,
            so_ma_vach_thung,
            updated_by,
            now,
            row_id,
        ),
    )
    conn.commit()
    conn.close()


def delete_row_db(row_id):
    delete_row_barcode_file(row_id)
    conn = get_db()
    conn.execute("DELETE FROM barcode_rows WHERE id = ?", (row_id,))
    conn.commit()
    conn.close()


def replace_all_rows(rows, updated_by):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM barcode_rows")
        cur.execute("DELETE FROM row_barcode_files")
        conn.commit()

        for filename in os.listdir(UPLOAD_BARCODE_FOLDER):
            try:
                os.remove(os.path.join(UPLOAD_BARCODE_FOLDER, filename))
            except OSError:
                pass

        cur.execute("BEGIN IMMEDIATE")
        for row in rows:
            now = now_str()
            ma_vach_sp = only_digits(row.get("ma_vach_sp", ""))
            so_ma_vach_thung = only_digits(row.get("so_ma_vach_thung", ""))

            cur.execute(
                """
                INSERT INTO barcode_rows (
                    id, ten_thung, so_luong_san_pham_thung, ten,
                    ma_vach_sp, so_ma_vach_thung, updated_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    row.get("ten_thung", "").strip(),
                    row.get("so_luong_san_pham_thung", "").strip(),
                    row.get("ten", "").strip(),
                    ma_vach_sp,
                    so_ma_vach_thung,
                    updated_by,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def group_rows(rows):
    grouped = []
    current = None
    for row in rows:
        key = (row["ten_thung"], row["so_luong_san_pham_thung"])
        if current and current["key"] == key:
            current["items"].append(row)
        else:
            current = {
                "key": key,
                "ten_thung": row["ten_thung"],
                "so_luong_san_pham_thung": row["so_luong_san_pham_thung"],
                "items": [row],
            }
            grouped.append(current)
    return grouped


def list_admins():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, password, role, display_name, created_at FROM admins "
        "ORDER BY CASE WHEN role='super_admin' THEN 0 ELSE 1 END, username ASC"
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def admin_exists(username):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM admins WHERE lower(username) = lower(?)", (username.strip(),))
    row = cur.fetchone()
    conn.close()
    return row is not None


def verify_admin(username, password):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, role, display_name FROM admins WHERE username = ? AND password = ?",
        (username.strip(), password),
    )
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_admin_by_id(admin_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role, display_name, created_at FROM admins WHERE id = ?", (admin_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_admin_user(username, password, role, display_name):
    conn = get_db()
    conn.execute(
        "INSERT INTO admins (id, username, password, role, display_name, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (str(uuid4()), username.strip(), password, role, display_name.strip(), now_str()),
    )
    conn.commit()
    conn.close()


def delete_admin_user(admin_id):
    conn = get_db()
    conn.execute("DELETE FROM admins WHERE id = ?", (admin_id,))
    conn.commit()
    conn.close()


def update_admin_password(admin_id, new_password):
    conn = get_db()
    conn.execute("UPDATE admins SET password = ? WHERE id = ?", (new_password, admin_id))
    conn.commit()
    conn.close()


def get_row_barcode_file(row_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM row_barcode_files WHERE row_id = ?", (row_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def save_row_barcode_file(row_id, file_storage, uploaded_by):
    old = get_row_barcode_file(row_id)
    if old:
        old_path = os.path.join(UPLOAD_BARCODE_FOLDER, old["saved_name"])
        if os.path.exists(old_path):
            os.remove(old_path)

    original_name = file_storage.filename or "barcode_file"
    safe_original = secure_filename(original_name)
    file_id = str(uuid4())
    saved_name = f"{file_id}_{safe_original}"
    save_path = os.path.join(UPLOAD_BARCODE_FOLDER, saved_name)
    file_storage.save(save_path)

    file_type = barcode_file_type(original_name)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO row_barcode_files (id, row_id, original_name, saved_name, file_type, uploaded_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (file_id, row_id, original_name, saved_name, file_type, uploaded_by, now_str()),
    )
    conn.commit()
    conn.close()


def delete_row_barcode_file(row_id):
    row = get_row_barcode_file(row_id)
    if not row:
        return

    file_path = os.path.join(UPLOAD_BARCODE_FOLDER, row["saved_name"])
    if os.path.exists(file_path):
        os.remove(file_path)

    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM row_barcode_files WHERE row_id = ?", (row_id,))
    conn.commit()
    conn.close()


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            flash("Bạn cần đăng nhập admin để chỉnh sửa.", "error")
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper


PAGE_HTML = """
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bảng mã vạch nội bộ</title>
  <script src="https://unpkg.com/html5-qrcode" type="text/javascript"></script>
  <style>
    :root { --bg:#eef2f7; --panel:#fff; --line:#cfd8e3; --line-dark:#94a3b8; --text:#0f172a; --muted:#475569; --brand:#0f766e; --brand-dark:#115e59; --brand-soft:#ecfdf5; --danger:#c62828; --warning:#b45309; --info:#175cd3; --shadow:0 12px 30px rgba(15,23,42,.08); --radius:18px; }
    *{box-sizing:border-box}
    body{margin:0;background:linear-gradient(180deg,#edf3f9 0%,#f8fafc 100%);color:var(--text);font-family:system-ui,-apple-system,"Segoe UI",Roboto,Arial,Helvetica,sans-serif}
    .container{max-width:1680px;margin:0 auto;padding:20px}
    .topbar{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:18px}
    h1{margin:0;font-size:30px}
    .sub{margin-top:6px;color:var(--muted);font-size:14px}
    .card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);padding:18px;margin-bottom:16px}
    .toolbar,.toolbar2{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
    .toolbar2{margin-top:12px}
    .btn{border:0;background:var(--brand);color:#fff;padding:10px 16px;border-radius:12px;cursor:pointer;text-decoration:none;font-size:14px;font-weight:700;white-space:nowrap;box-shadow:0 6px 16px rgba(15,118,110,.18)}
    .btn:hover{background:var(--brand-dark)}
    .btn.secondary{background:#fff;color:var(--text);border:1px solid var(--line);box-shadow:none}
    .btn.danger{background:var(--danger);box-shadow:none}
    .btn.warning{background:var(--warning);box-shadow:none}
    .btn.download{background:#1d4ed8;box-shadow:none}
    .badge{display:inline-flex;align-items:center;gap:6px;background:var(--brand-soft);color:#047857;font-size:12px;font-weight:800;padding:7px 12px;border-radius:999px;border:1px solid #bbf7d0}
    .badge.viewer{background:#eff6ff;color:var(--info);border-color:#bfdbfe}
    .flash{border-radius:12px;padding:12px 14px;margin-bottom:10px;font-size:14px;background:#edfdf2;color:#067647;border:1px solid #ccebcf}
    .flash.error{background:#fff1f1;color:#b42318;border-color:#f0c7c7}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
    .grid-1{grid-column:1 / -1}
    label{display:block;margin-bottom:6px;font-size:14px;font-weight:800;color:#1e293b}
    input[type='text'],input[type='password'],input[type='file'],select{width:100%;border:1px solid var(--line);border-radius:12px;padding:11px 12px;font-size:14px;background:#fff;font-family:inherit}
    .table-wrap{overflow-x:auto;border:1px solid var(--line-dark);background:#fff;border-radius:14px}
    table{width:100%;border-collapse:collapse;min-width:1600px;table-layout:fixed;background:#fff}
    th,td{border:1px solid var(--line-dark);padding:12px 10px;text-align:center;vertical-align:middle;font-size:14px;line-height:1.35}
    th{background:#e2e8f0;font-weight:800;color:#0f172a}
    td.left{text-align:left}
    td.group-cell,td.qty-cell{font-size:18px;line-height:1.45;font-weight:700;background:#fafafa}
    .barcode-action-box{display:flex;flex-direction:column;gap:8px;align-items:center;justify-content:center}
    .status-ok{font-weight:800;color:#067647}
    .status-no{font-weight:800;color:#b42318}
    .status-pdf{font-weight:800;color:#1d4ed8}
    .actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:center}
    .login-box{max-width:450px;margin:40px auto}
    .scan-grid{display:grid;grid-template-columns:360px 1fr;gap:18px;align-items:start}
    #reader{width:100%;max-width:360px;min-height:250px;border:1px solid var(--line);border-radius:16px;overflow:hidden;background:#fff}
    .scan-result{border:2px dashed #94a3b8;border-radius:16px;padding:16px 18px;background:linear-gradient(180deg,#fff 0%,#f8fafc 100%);font-size:18px;font-weight:700;color:#0f172a}
    .scan-label{color:var(--muted);font-size:14px;font-weight:600;margin-top:6px}
    .hit-card{border:2px solid #dbe3ec;border-radius:16px;padding:18px 20px;margin-top:12px;background:linear-gradient(180deg,#fff 0%,#f8fafc 100%);font-size:18px;line-height:1.55;font-weight:600;box-shadow:0 8px 20px rgba(15,23,42,.04)}
    .hit-title{font-size:30px;line-height:1.2;font-weight:900;color:#0f172a;margin-bottom:10px}
    .hit-row{margin-top:4px;font-size:20px}
    .hit-code{font-size:28px;font-weight:900;color:#0f172a;letter-spacing:.02em}
    .hit-code.red{color:#b91c1c}
    .hit-meta{color:var(--muted);font-size:15px;font-weight:600;margin-top:6px}
    .meta{color:var(--muted);font-size:13px}
    .stats-pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:#f8fafc;border:1px solid var(--line);font-size:13px;font-weight:700;color:#334155}
    .section-title{font-size:18px;font-weight:900;color:#0f172a;margin:0}
    .form-note{margin-top:8px;color:var(--muted);font-size:13px}
    @media print{.topbar,.toolbar form,.btn,.scan-grid,.actions,.barcode-action-box form{display:none !important}body{background:#fff}.card{box-shadow:none;border:1px solid #d1d5db}}
    @media (max-width:1100px){.grid,.scan-grid{grid-template-columns:1fr}td.group-cell,td.qty-cell{font-size:16px}.hit-title{font-size:24px}.hit-row{font-size:18px}.hit-code{font-size:24px}}
  </style>
</head>
<body>
  <div class="container">
    <div class="topbar">
      <div><h1>Bảng mã vạch nội bộ</h1><div class="sub">Mỗi sản phẩm có thể upload barcode dạng ảnh hoặc PDF.</div></div>
      <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
        {% if is_admin %}
          <span class="badge">{{ admin_display_name }}{% if is_super_admin %} - Super Admin{% else %} - Admin{% endif %}</span>
          <a class="btn secondary" href="{{ url_for('logout') }}">Đăng xuất</a>
        {% else %}
          <span class="badge viewer">Chế độ xem</span>
          <a class="btn" href="{{ url_for('login') }}">Đăng nhập admin</a>
        {% endif %}
      </div>
    </div>

    {% with messages = get_flashed_messages(with_categories=true) %}
      {% if messages %}
        {% for category, message in messages %}
          <div class="flash {% if category == 'error' %}error{% endif %}">{{ message }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    {% if show_login %}
      <div class="card login-box">
        <h2 class="section-title">Đăng nhập admin</h2>
        <form method="post" action="{{ url_for('login') }}" style="margin-top:14px;">
          <label>Tài khoản</label><input type="text" name="username" required>
          <label style="margin-top:10px;">Mật khẩu</label><input type="password" name="password" required>
          <div style="margin-top:14px; display:flex; gap:8px;">
            <button class="btn" type="submit">Đăng nhập</button>
            <a class="btn secondary" href="{{ url_for('index') }}">Quay lại</a>
          </div>
        </form>
      </div>
    {% else %}

      <div class="card">
        <div class="toolbar">
          <div>
            <h2 class="section-title">Tìm nhanh / scan mã</h2>
            <div class="meta">Quét hoặc nhập mã để xem kết quả lớn, rõ.</div>
          </div>
          <form method="get" action="{{ url_for('index') }}" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
            <input type="text" name="q" value="{{ q }}" placeholder="Tìm theo tên, mã..." style="width:360px; max-width:100%;">
            <button class="btn secondary" type="submit">Tìm</button>
            {% if q %}<a class="btn secondary" href="{{ url_for('index') }}">Xoá lọc</a>{% endif %}
          </form>
        </div>
        <div class="scan-grid" style="margin-top:16px;">
          <div>
            <div id="reader"></div>
            <div class="toolbar2">
              <button class="btn" type="button" onclick="startScanner()">Bật camera scan</button>
              <button class="btn secondary" type="button" onclick="stopScanner()">Tắt camera</button>
            </div>
          </div>
          <div>
            <div class="scan-result">
              <div><strong>Mã vừa quét:</strong> <span id="scanText">Chưa có</span></div>
              <div class="scan-label">Không có camera thì nhập tay hoặc dùng máy quét USB.</div>
            </div>
            <div id="scanHits"></div>
          </div>
        </div>
      </div>

      {% if is_admin %}
      <div class="card">
        <div class="toolbar">
          <div>
            <h2 class="section-title">Quản lý admin</h2>
            <div class="meta">Super Admin được tạo, xoá admin và đổi mọi mật khẩu. Admin thường chỉ được đổi mật khẩu của chính mình.</div>
          </div>
          <div class="stats-pill">Tổng tài khoản quản trị: {{ admins|length }}</div>
        </div>
        <div class="grid" style="margin-top:14px;">
          <div>
            {% if is_super_admin %}
              <form method="post" action="{{ url_for('add_admin') }}">
                <label>Tên hiển thị</label><input type="text" name="display_name" required>
                <label style="margin-top:10px;">Tài khoản admin mới</label><input type="text" name="username" required>
                <label style="margin-top:10px;">Mật khẩu</label><input type="text" name="password" required>
                <label style="margin-top:10px;">Quyền</label>
                <select name="role">
                  <option value="admin">Admin thường</option>
                  <option value="super_admin">Super Admin</option>
                </select>
                <div style="margin-top:12px;"><button class="btn" type="submit">Tạo admin</button></div>
              </form>
            {% else %}
              <div class="hit-card" style="margin-top:0;">
                <div class="hit-title" style="font-size:22px;">Bạn là admin thường</div>
                <div class="hit-meta">Bạn chỉ được đổi mật khẩu của chính mình.</div>
              </div>
            {% endif %}
          </div>
          <div>
            <div class="meta" style="margin-bottom:8px;">Danh sách admin</div>
            {% for admin in admins %}
              <div class="hit-card" style="margin-top:0; margin-bottom:10px; font-size:16px;">
                <div style="font-size:22px; font-weight:900;">{{ admin['display_name'] }}</div>
                <div class="hit-meta">Tài khoản: {{ admin['username'] }} | Quyền: {{ 'Super Admin' if admin['role'] == 'super_admin' else 'Admin' }}</div>
                <div class="hit-meta">Tạo lúc: {{ admin['created_at'] }}</div>
                {% if is_super_admin or session_admin_id == admin['id'] %}
                  <form method="post" action="{{ url_for('change_admin_password', admin_id=admin['id']) }}" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:12px;">
                    <input type="text" name="new_password" required placeholder="Mật khẩu mới" style="max-width:240px;">
                    <button class="btn secondary" type="submit">Đổi mật khẩu</button>
                  </form>
                {% endif %}
                {% if is_super_admin and admins|length > 1 %}
                  <form method="post" action="{{ url_for('delete_admin', admin_id=admin['id']) }}" onsubmit="return confirm('Xoá admin này?');" style="margin-top:10px;">
                    <button class="btn danger" type="submit">Xoá admin</button>
                  </form>
                {% endif %}
              </div>
            {% endfor %}
          </div>
        </div>
      </div>

      <div class="card">
        <div class="toolbar">
          <div>
            <h2 class="section-title">Import / Export dữ liệu</h2>
            <div class="meta">Import và export bằng file Excel .xlsx.</div>
          </div>
          <a class="btn secondary" href="{{ url_for('export_xlsx') }}">Export XLSX</a>
        </div>
        <form method="post" action="{{ url_for('import_xlsx') }}" enctype="multipart/form-data" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:12px;">
          <input type="file" name="file" accept=".xlsx" required style="max-width:380px;">
          <button class="btn warning" type="submit">Import XLSX</button>
        </form>
      </div>

      <div class="card">
        <div class="toolbar">
          <div>
            <h2 class="section-title">{% if edit_row %}Sửa dòng{% else %}Thêm dòng mới{% endif %}</h2>
            <div class="meta">Mã thùng nhập tay theo đúng mã bạn muốn.</div>
          </div>
          {% if edit_row %}<a class="btn secondary" href="{{ url_for('index') }}">Huỷ sửa</a>{% endif %}
        </div>
        <form method="post" action="{% if edit_row %}{{ url_for('update_row', row_id=edit_row['id']) }}{% else %}{{ url_for('add_row') }}{% endif %}" style="margin-top:12px;">
          <div class="grid">
            <div><label>Tên thùng</label><input type="text" name="ten_thung" value="{{ edit_row['ten_thung'] if edit_row else '' }}" required></div>
            <div><label>Số lượng sản phẩm/thùng</label><input type="text" name="so_luong_san_pham_thung" value="{{ edit_row['so_luong_san_pham_thung'] if edit_row else '' }}" required></div>
            <div class="grid-1"><label>Tên</label><input type="text" name="ten" value="{{ edit_row['ten'] if edit_row else '' }}" required></div>
            <div><label>Mã vạch SP</label><input type="text" name="ma_vach_sp" value="{{ edit_row['ma_vach_sp'] if edit_row else '' }}" required></div>
            <div><label>Số mã vạch thùng</label><input type="text" name="so_ma_vach_thung" value="{{ edit_row['so_ma_vach_thung'] if edit_row else '' }}" required></div>
          </div>
          <div style="margin-top:14px; display:flex; gap:8px; flex-wrap:wrap;">
            <button class="btn" type="submit">{% if edit_row %}Lưu thay đổi{% else %}Thêm dòng{% endif %}</button>
          </div>
        </form>
      </div>
      {% endif %}

      <div class="card">
        <div class="toolbar">
          <div>
            <h2 class="section-title">Bảng dữ liệu</h2>
            <div class="meta">Tổng dòng hiển thị: {{ rows|length }}</div>
          </div>
          <div style="display:flex; gap:8px; flex-wrap:wrap;">
            <div class="stats-pill">Người sửa hiển thị trên từng dòng</div>
            <button class="btn secondary" type="button" onclick="window.print()">In bảng</button>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th style="width:11%;">Tên Thùng</th>
                <th style="width:9%;">Số lượng SP/thùng</th>
                <th style="width:17%;">Tên</th>
                <th style="width:9%;">Mã vạch SP</th>
                <th style="width:11%;">Số mã vạch thùng</th>
                <th style="width:21%;">Mã vạch</th>
                <th style="width:10%;">Người sửa</th>
                {% if is_admin %}<th style="width:12%;">Thao tác</th>{% endif %}
              </tr>
            </thead>
            <tbody>
              {% for group in grouped_rows %}
                {% for row in group['items'] %}
                  <tr>
                    {% if loop.index0 == 0 %}
                      <td class="group-cell" rowspan="{{ group['items']|length }}">{{ group['ten_thung'] }}</td>
                      <td class="qty-cell" rowspan="{{ group['items']|length }}">{{ group['so_luong_san_pham_thung'] }}</td>
                    {% endif %}
                    <td class="left">{{ row['ten'] }}</td>
                    <td>{{ row['ma_vach_sp_grouped'] }}</td>
                    <td>{{ row['so_ma_vach_thung_grouped'] }}</td>
                    <td>
                      <div class="barcode-action-box">
                        {% if row['has_barcode_file'] %}
                          {% if row['barcode_file_type'] == 'pdf' %}
                            <div class="status-pdf">Đã có barcode PDF</div>
                          {% else %}
                            <div class="status-ok">Đã có barcode ảnh</div>
                          {% endif %}
                          <a class="btn download" href="{{ url_for('download_row_barcode', row_id=row['id']) }}">Tải xuống</a>
                        {% else %}
                          <div class="status-no">Chưa có barcode</div>
                        {% endif %}

                        {% if is_admin %}
                          <form method="post" action="{{ url_for('upload_row_barcode', row_id=row['id']) }}" enctype="multipart/form-data" style="display:flex; gap:6px; flex-direction:column; width:100%;">
                            <input type="file" name="barcode_file" accept=".png,.jpg,.jpeg,.webp,.pdf" required>
                            <button class="btn" type="submit">Tải lên</button>
                          </form>

                          {% if row['has_barcode_file'] %}
                          <form method="post" action="{{ url_for('delete_row_barcode', row_id=row['id']) }}" onsubmit="return confirm('Xoá barcode của dòng này?');">
                            <button class="btn danger" type="submit">Xoá barcode</button>
                          </form>
                          {% endif %}
                        {% endif %}
                      </div>
                    </td>
                    <td>{{ row['updated_by'] or '' }}</td>
                    {% if is_admin %}
                      <td>
                        <div class="actions">
                          <a class="btn secondary" href="{{ url_for('edit_row', row_id=row['id']) }}">Sửa</a>
                          <form method="post" action="{{ url_for('delete_row', row_id=row['id']) }}" onsubmit="return confirm('Xoá dòng này?');">
                            <button class="btn danger" type="submit">Xoá</button>
                          </form>
                        </div>
                      </td>
                    {% endif %}
                  </tr>
                {% endfor %}
              {% else %}
                <tr><td colspan="8">Không có dữ liệu phù hợp.</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    {% endif %}
  </div>

  <script>
    const allRows = {{ rows_json | safe }};
    const SETTINGS = {{ settings_json | safe }};
    let scannerInstance = null;
    let scanning = false;

    function digitsOnly(v){
      return String(v || '').replace(/\\D/g, '');
    }

    function groupNumber(value, pattern){
      const digits = digitsOnly(value);
      const groups = String(pattern || '').split('-').map(x => parseInt(x, 10)).filter(x => !Number.isNaN(x) && x > 0);
      if (!groups.length) return digits;
      let out = [];
      let idx = 0;
      groups.forEach(size => {
        if (idx < digits.length) {
          out.push(digits.slice(idx, idx + size));
          idx += size;
        }
      });
      if (idx < digits.length) out.push(digits.slice(idx));
      return out.join(' ');
    }

    function findMatches(code) {
      const normalized = (code || '').trim().toLowerCase();
      if (!normalized) return [];
      return allRows.filter(row =>
        String(row.ma_vach_sp || '').includes(normalized) ||
        String(row.so_ma_vach_thung || '').includes(normalized) ||
        String(row.ten || '').toLowerCase().includes(normalized) ||
        String(row.ten_thung || '').toLowerCase().includes(normalized)
      );
    }

    function renderHits(code) {
      const wrap = document.getElementById('scanHits');
      document.getElementById('scanText').innerText = code || 'Chưa có';
      const hits = findMatches(code);
      if (!code) {
        wrap.innerHTML = '';
        return;
      }
      if (!hits.length) {
        wrap.innerHTML = '<div class="hit-card"><div class="hit-title" style="font-size:24px;">Không tìm thấy dữ liệu phù hợp</div><div class="hit-meta">Hãy kiểm tra lại mã quét hoặc nhập tay vào ô tìm kiếm.</div></div>';
        return;
      }
      wrap.innerHTML = hits.map(row => `
        <div class="hit-card">
          <div class="hit-title">${row.ten}</div>
          <div class="hit-row">Thùng: <strong>${row.ten_thung}</strong> | SL/thùng: <strong>${row.so_luong_san_pham_thung}</strong></div>
          <div class="hit-row">Mã SP: <span class="hit-code">${groupNumber(row.ma_vach_sp, SETTINGS.group_pattern)}</span></div>
          <div class="hit-row">Mã thùng: <span class="hit-code red">${groupNumber(row.so_ma_vach_thung, SETTINGS.group_pattern)}</span></div>
          <div class="hit-row">Người sửa: <strong>${row.updated_by || ''}</strong></div>
          <div class="hit-row">Trạng thái barcode: <strong>${row.has_barcode_file ? (row.barcode_file_type === 'pdf' ? 'Đã có barcode PDF' : 'Đã có barcode ảnh') : 'Chưa có barcode'}</strong></div>
        </div>`).join('');
    }

    function onScanSuccess(decodedText) {
      renderHits(decodedText);
      const searchInput = document.querySelector('input[name="q"]');
      if (searchInput) searchInput.value = decodedText;
    }

    function startScanner() {
      if (scanning) return;
      scannerInstance = new Html5Qrcode('reader');
      Html5Qrcode.getCameras().then(devices => {
        if (!devices || !devices.length) {
          alert('Không tìm thấy camera trên thiết bị này.');
          return;
        }
        const cameraId = devices[0].id;
        scannerInstance.start(
          cameraId,
          { fps: 10, qrbox: { width: 250, height: 120 } },
          onScanSuccess,
          () => {}
        ).then(() => {
          scanning = true;
        }).catch(() => {
          alert('Không bật được camera. Hãy kiểm tra quyền truy cập camera.');
        });
      }).catch(() => {
        alert('Thiết bị không hỗ trợ lấy danh sách camera.');
      });
    }

    function stopScanner() {
      if (!scannerInstance || !scanning) return;
      scannerInstance.stop().then(() => {
        scannerInstance.clear();
        scanning = false;
      }).catch(() => {});
    }

    {% if q %}renderHits({{ q | tojson }});{% endif %}
  </script>
</body>
</html>
"""


init_db()


def render_page(rows, q, is_admin, show_login, edit_row):
    settings = get_settings()
    return render_template_string(
        PAGE_HTML,
        rows=rows,
        grouped_rows=group_rows(rows),
        rows_json=rows,
        q=q,
        settings=settings,
        settings_json=settings,
        is_admin=is_admin,
        is_super_admin=session.get("admin_role") == "super_admin",
        admin_display_name=session.get("admin_display_name", ""),
        session_admin_id=session.get("admin_id", ""),
        show_login=show_login,
        edit_row=edit_row,
        admins=list_admins(),
    )


@app.route("/")
def index():
    q = request.args.get("q", "")
    rows = list_rows(q)
    return render_page(rows, q, session.get("is_admin", False), False, None)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        admin = verify_admin(username, password)
        if admin:
            session["is_admin"] = True
            session["admin_id"] = admin["id"]
            session["admin_username"] = admin["username"]
            session["admin_role"] = admin["role"]
            session["admin_display_name"] = admin["display_name"]
            flash("Đăng nhập admin thành công.", "success")
            return redirect(request.args.get("next") or url_for("index"))
        flash("Sai tài khoản hoặc mật khẩu admin.", "error")
    return render_page(list_rows(), "", False, True, None)


@app.route("/logout")
def logout():
    session.clear()
    flash("Đã đăng xuất.", "success")
    return redirect(url_for("index"))


@app.route("/add", methods=["POST"])
@admin_required
def add_row():
    if not request.form.get("ten", "").strip():
        flash("Tên sản phẩm không được để trống.", "error")
        return redirect(url_for("index"))
    create_row(request.form, session.get("admin_display_name", session.get("admin_username", "")))
    flash("Đã thêm dòng mới.", "success")
    return redirect(url_for("index"))


@app.route("/edit/<row_id>")
@admin_required
def edit_row(row_id):
    row = get_row(row_id)
    if not row:
        flash("Không tìm thấy dòng cần sửa.", "error")
        return redirect(url_for("index"))
    return render_page(list_rows(), "", True, False, row)


@app.route("/update/<row_id>", methods=["POST"])
@admin_required
def update_row(row_id):
    if not get_row(row_id):
        flash("Dòng này không còn tồn tại.", "error")
        return redirect(url_for("index"))
    update_row_db(row_id, request.form, session.get("admin_display_name", session.get("admin_username", "")))
    flash("Đã cập nhật dòng.", "success")
    return redirect(url_for("index"))


@app.route("/delete/<row_id>", methods=["POST"])
@admin_required
def delete_row(row_id):
    delete_row_db(row_id)
    flash("Đã xoá dòng.", "success")
    return redirect(url_for("index"))


@app.route("/export")
@admin_required
def export_xlsx():
    rows = list_rows()
    wb = Workbook()
    ws = wb.active
    ws.title = "Du lieu ma vach"
    headers = ["ten_thung", "so_luong_san_pham_thung", "ten", "ma_vach_sp", "so_ma_vach_thung", "updated_by"]
    ws.append(headers)
    for row in rows:
        ws.append([
            row["ten_thung"],
            row["so_luong_san_pham_thung"],
            row["ten"],
            row["ma_vach_sp"],
            row["so_ma_vach_thung"],
            row.get("updated_by", ""),
        ])

    for column_cells in ws.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = min(max_length + 2, 42)

    mem = io.BytesIO()
    wb.save(mem)
    mem.seek(0)
    return send_file(
        mem,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=EXPORT_FILE,
    )


@app.route("/import", methods=["POST"])
@admin_required
def import_xlsx():
    file = request.files.get("file")
    if not file or not file.filename.lower().endswith(".xlsx"):
        flash("Hãy chọn file XLSX hợp lệ.", "error")
        return redirect(url_for("index"))

    try:
        wb = load_workbook(filename=io.BytesIO(file.read()), data_only=True)
        ws = wb.active
    except Exception:
        flash("Không đọc được file XLSX.", "error")
        return redirect(url_for("index"))

    rows_iter = list(ws.iter_rows(values_only=True))
    if not rows_iter:
        flash("File XLSX không có dữ liệu.", "error")
        return redirect(url_for("index"))

    headers = ["" if h is None else str(h).strip() for h in rows_iter[0]]
    required_cols = ["ten_thung", "so_luong_san_pham_thung", "ten", "ma_vach_sp", "so_ma_vach_thung"]
    if not all(col in headers for col in required_cols):
        flash("File XLSX thiếu cột bắt buộc.", "error")
        return redirect(url_for("index"))

    index_map = {name: headers.index(name) for name in required_cols}
    imported_rows = []
    for excel_row in rows_iter[1:]:
        ten_value = excel_row[index_map["ten"]] if index_map["ten"] < len(excel_row) else ""
        if ten_value is None or str(ten_value).strip() == "":
            continue
        imported_rows.append({
            "ten_thung": "" if excel_row[index_map["ten_thung"]] is None else str(excel_row[index_map["ten_thung"]]).strip(),
            "so_luong_san_pham_thung": "" if excel_row[index_map["so_luong_san_pham_thung"]] is None else str(excel_row[index_map["so_luong_san_pham_thung"]]).strip(),
            "ten": str(ten_value).strip(),
            "ma_vach_sp": "" if excel_row[index_map["ma_vach_sp"]] is None else str(excel_row[index_map["ma_vach_sp"]]).strip(),
            "so_ma_vach_thung": "" if excel_row[index_map["so_ma_vach_thung"]] is None else str(excel_row[index_map["so_ma_vach_thung"]]).strip(),
        })

    replace_all_rows(imported_rows, session.get("admin_display_name", session.get("admin_username", "")))
    flash(f"Đã import {len(imported_rows)} dòng từ XLSX.", "success")
    return redirect(url_for("index"))


@app.route("/row-barcode/<row_id>/upload", methods=["POST"])
@admin_required
def upload_row_barcode(row_id):
    if not get_row(row_id):
        flash("Không tìm thấy dòng sản phẩm.", "error")
        return redirect(url_for("index"))

    file = request.files.get("barcode_file")
    if not file or not file.filename:
        flash("Hãy chọn file barcode.", "error")
        return redirect(url_for("index"))

    if not allowed_barcode_file(file.filename):
        flash("Chỉ cho phép file PNG/JPG/JPEG/WEBP/PDF.", "error")
        return redirect(url_for("index"))

    save_row_barcode_file(row_id, file, session.get("admin_display_name", session.get("admin_username", "")))
    flash("Đã tải lên barcode cho sản phẩm.", "success")
    return redirect(url_for("index"))


@app.route("/row-barcode/<row_id>/download")
def download_row_barcode(row_id):
    row = get_row_barcode_file(row_id)
    if not row:
        flash("Sản phẩm này chưa có barcode.", "error")
        return redirect(url_for("index"))

    file_path = os.path.join(UPLOAD_BARCODE_FOLDER, row["saved_name"])
    if not os.path.exists(file_path):
        flash("File barcode không còn tồn tại.", "error")
        return redirect(url_for("index"))

    return send_file(file_path, as_attachment=True, download_name=row["original_name"])


@app.route("/row-barcode/<row_id>/delete", methods=["POST"])
@admin_required
def delete_row_barcode(row_id):
    delete_row_barcode_file(row_id)
    flash("Đã xoá barcode của sản phẩm.", "success")
    return redirect(url_for("index"))


@app.route("/admins/add", methods=["POST"])
@admin_required
def add_admin():
    if session.get("admin_role") != "super_admin":
        flash("Chỉ Super Admin mới được tạo admin.", "error")
        return redirect(url_for("index"))
    display_name = request.form.get("display_name", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "admin").strip()
    if not display_name or not username or not password:
        flash("Tên hiển thị, tài khoản và mật khẩu không được để trống.", "error")
        return redirect(url_for("index"))
    if admin_exists(username):
        flash("Tài khoản admin đã tồn tại.", "error")
        return redirect(url_for("index"))
    if role not in ["admin", "super_admin"]:
        role = "admin"
    create_admin_user(username, password, role, display_name)
    flash("Đã tạo admin mới.", "success")
    return redirect(url_for("index"))


@app.route("/admins/<admin_id>/password", methods=["POST"])
@admin_required
def change_admin_password(admin_id):
    new_password = request.form.get("new_password", "").strip()
    if session.get("admin_role") != "super_admin" and session.get("admin_id") != admin_id:
        flash("Admin thường chỉ được đổi mật khẩu của chính mình.", "error")
        return redirect(url_for("index"))
    if not new_password:
        flash("Mật khẩu mới không được để trống.", "error")
        return redirect(url_for("index"))
    update_admin_password(admin_id, new_password)
    flash("Đã đổi mật khẩu admin.", "success")
    return redirect(url_for("index"))


@app.route("/admins/<admin_id>/delete", methods=["POST"])
@admin_required
def delete_admin(admin_id):
    if session.get("admin_role") != "super_admin":
        flash("Chỉ Super Admin mới được xoá admin.", "error")
        return redirect(url_for("index"))
    admins = list_admins()
    if len(admins) <= 1:
        flash("Phải giữ ít nhất 1 admin.", "error")
        return redirect(url_for("index"))
    target = get_admin_by_id(admin_id)
    if target and target.get("role") == "super_admin":
        super_admin_count = sum(1 for a in admins if a.get("role") == "super_admin")
        if super_admin_count <= 1:
            flash("Phải giữ ít nhất 1 Super Admin.", "error")
            return redirect(url_for("index"))
    delete_admin_user(admin_id)
    if session.get("admin_id") == admin_id:
        session.clear()
        flash("Bạn đã tự xoá tài khoản đang đăng nhập.", "success")
        return redirect(url_for("index"))
    flash("Đã xoá admin.", "success")
    return redirect(url_for("index"))


@app.route("/api/rows")
def api_rows():
    return jsonify(list_rows())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)