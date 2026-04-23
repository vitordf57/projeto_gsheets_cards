from __future__ import annotations

import os
import re
import secrets
import sqlite3
import time
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Callable, Iterable

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash


DB_DEFAULT_NAME = "auth_users.db"
LOGIN_RATE_LIMIT_WINDOW_SECONDS = 15 * 60
LOGIN_RATE_LIMIT_MAX_ATTEMPTS = 8
MAX_FAILED_LOGINS_BEFORE_LOCK = 5
ACCOUNT_LOCK_MINUTES = 20
PASSWORD_MIN_LENGTH = 12

# Rate limit simples em memória. Em produção distribuída, troque por Redis.
_LOGIN_ATTEMPTS: dict[str, list[float]] = {}


auth_bp = Blueprint("auth_bp", __name__)


@dataclass
class AuthUser:
    id: int
    nome: str
    email: str
    is_admin: bool
    ativo: bool
    permissoes: set[str]


# =========================
# Banco / utilitários
# =========================

def get_auth_db_path() -> str:
    path = current_app.config.get("AUTH_DB_PATH")
    if path:
        return path

    instance_path = Path(current_app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)
    return str(instance_path / DB_DEFAULT_NAME)


def get_auth_db() -> sqlite3.Connection:
    if "auth_db" not in g:
        conn = sqlite3.connect(get_auth_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.auth_db = conn
    return g.auth_db


@auth_bp.teardown_app_request
def close_auth_db(_exception=None):
    conn = g.pop("auth_db", None)
    if conn is not None:
        conn.close()


def init_auth_db() -> None:
    db = get_auth_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            ativo INTEGER NOT NULL DEFAULT 1,
            failed_login_count INTEGER NOT NULL DEFAULT 0,
            lock_until INTEGER,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        );

        CREATE TABLE IF NOT EXISTS user_screen_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            screen_key TEXT NOT NULL,
            UNIQUE(user_id, screen_key),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            actor_user_id INTEGER,
            action TEXT NOT NULL,
            details TEXT,
            ip_address TEXT,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        """
    )
    db.commit()


def _now_ts() -> int:
    return int(time.time())


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return forwarded or request.remote_addr or "0.0.0.0"


def _validate_password_strength(password: str) -> list[str]:
    errors: list[str] = []
    if len(password) < PASSWORD_MIN_LENGTH:
        errors.append(f"A senha precisa ter pelo menos {PASSWORD_MIN_LENGTH} caracteres.")
    if not re.search(r"[A-Z]", password):
        errors.append("A senha precisa ter ao menos 1 letra maiúscula.")
    if not re.search(r"[a-z]", password):
        errors.append("A senha precisa ter ao menos 1 letra minúscula.")
    if not re.search(r"\d", password):
        errors.append("A senha precisa ter ao menos 1 número.")
    if not re.search(r"[^A-Za-z0-9]", password):
        errors.append("A senha precisa ter ao menos 1 caractere especial.")
    return errors


def _csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def _require_csrf() -> None:
    sent = request.form.get("csrf_token", "")
    stored = session.get("csrf_token", "")
    if not sent or not stored or not secrets.compare_digest(sent, stored):
        abort(400, description="Token CSRF inválido.")


def _prune_ip_attempts(ip: str) -> list[float]:
    now = time.time()
    attempts = [ts for ts in _LOGIN_ATTEMPTS.get(ip, []) if now - ts <= LOGIN_RATE_LIMIT_WINDOW_SECONDS]
    _LOGIN_ATTEMPTS[ip] = attempts
    return attempts


def _is_ip_rate_limited(ip: str) -> bool:
    attempts = _prune_ip_attempts(ip)
    return len(attempts) >= LOGIN_RATE_LIMIT_MAX_ATTEMPTS


def _register_ip_attempt(ip: str) -> None:
    attempts = _prune_ip_attempts(ip)
    attempts.append(time.time())
    _LOGIN_ATTEMPTS[ip] = attempts


def _row_to_user(user_row: sqlite3.Row | None) -> AuthUser | None:
    if not user_row:
        return None
    db = get_auth_db()
    perm_rows = db.execute(
        "SELECT screen_key FROM user_screen_permissions WHERE user_id = ? ORDER BY screen_key",
        (user_row["id"],),
    ).fetchall()
    return AuthUser(
        id=int(user_row["id"]),
        nome=str(user_row["nome"]),
        email=str(user_row["email"]),
        is_admin=bool(user_row["is_admin"]),
        ativo=bool(user_row["ativo"]),
        permissoes={str(r["screen_key"]) for r in perm_rows},
    )


def get_current_user() -> AuthUser | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    db = get_auth_db()
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return _row_to_user(row)


def registrar_auditoria(action: str, *, user_id: int | None = None, details: str = "") -> None:
    db = get_auth_db()
    actor_user_id = session.get("user_id")
    db.execute(
        """
        INSERT INTO audit_log (user_id, actor_user_id, action, details, ip_address)
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_id, actor_user_id, action, details[:1000], _client_ip()),
    )
    db.commit()


# =========================
# Decorators / helpers de acesso
# =========================

def login_required(view_func: Callable):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not get_current_user():
            flash("Faça login para continuar.", "warning")
            return redirect(url_for("auth_bp.login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapper


def admin_required(view_func: Callable):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            flash("Faça login para continuar.", "warning")
            return redirect(url_for("auth_bp.login", next=request.path))
        if not user.is_admin:
            abort(403)
        return view_func(*args, **kwargs)

    return wrapper


def screen_required(screen_key: str):
    def decorator(view_func: Callable):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            user = get_current_user()
            if not user:
                flash("Faça login para continuar.", "warning")
                return redirect(url_for("auth_bp.login", next=request.path))
            if user.is_admin or screen_key in user.permissoes:
                return view_func(*args, **kwargs)
            abort(403)

        return wrapper

    return decorator


def usuario_pode(screen_key: str) -> bool:
    user = get_current_user()
    if not user:
        return False
    return user.is_admin or screen_key in user.permissoes


def _default_route_for_user(user: AuthUser | None) -> str:
    if not user:
        return url_for("auth_bp.login")
    if user.is_admin:
        return url_for("auth_bp.admin_users")

    prioridade = [
        ("home", "/home"),
        ("principal", "/"),
        ("metricas_full", "/metricas-full"),
        ("conferencia", "/conferencia"),
        ("picking", "/picking"),
        ("enviando", "/?tela=enviando"),
        ("ecommerce", "/?tela=ecommerce"),
        ("compras", "/?tela=compras"),
        ("acompanhamento", "/?tela=acompanhamento"),
        ("homologar", "/?tela=homologar"),
        ("naoEnviar", "/?tela=naoEnviar"),
    ]

    for screen_key, rota in prioridade:
        if screen_key in user.permissoes:
            return rota

    return url_for("auth_bp.login")


# =========================
# Integração no app
# =========================

def configure_auth_app(app) -> None:
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("REMEMBER_COOKIE_HTTPONLY", True)
    app.config.setdefault("PERMANENT_SESSION_LIFETIME", 60 * 60 * 8)

    # Quando subir em produção com HTTPS, ligue isso.
    if os.getenv("FLASK_ENV") == "production":
        app.config.setdefault("SESSION_COOKIE_SECURE", True)

    app.register_blueprint(auth_bp)

    @app.context_processor
    def inject_auth_context():
        return {
            "auth_user": get_current_user(),
            "csrf_token": _csrf_token(),
            "usuario_pode": usuario_pode,
        }

    with app.app_context():
        init_auth_db()
        _ensure_default_admin()


def _ensure_default_admin() -> None:
    email = os.getenv("ADMIN_EMAIL", "admin@admin.local").strip().lower()
    password = os.getenv("ADMIN_PASSWORD", "Troque-Agora-123!")
    db = get_auth_db()
    exists = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if exists:
        return
    db.execute(
        """
        INSERT INTO users (nome, email, password_hash, is_admin, ativo)
        VALUES (?, ?, ?, 1, 1)
        """,
        ("Administrador", email, generate_password_hash(password, method="scrypt")),
    )
    db.commit()


# =========================
# Rotas públicas
# =========================
@auth_bp.get("/login")
def login():
    user = get_current_user()
    if user:
        return redirect(_default_route_for_user(user))
    return render_template("login.html")


@auth_bp.post("/login")
def login_post():
    _require_csrf()

    ip = _client_ip()
    if _is_ip_rate_limited(ip):
        flash("Muitas tentativas. Aguarde alguns minutos e tente novamente.", "danger")
        return redirect(url_for("auth_bp.login"))

    email = _normalize_email(request.form.get("email", ""))
    password = request.form.get("password", "")

    db = get_auth_db()
    user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()

    if not user:
        _register_ip_attempt(ip)
        flash("Credenciais inválidas.", "danger")
        return redirect(url_for("auth_bp.login"))

    lock_until = user["lock_until"]
    if lock_until and int(lock_until) > _now_ts():
        flash("Conta temporariamente bloqueada. Procure o administrador.", "danger")
        return redirect(url_for("auth_bp.login"))

    if not bool(user["ativo"]):
        flash("Usuário desativado.", "danger")
        return redirect(url_for("auth_bp.login"))

    if not check_password_hash(user["password_hash"], password):
        failed_count = int(user["failed_login_count"] or 0) + 1
        lock_until_value = None
        if failed_count >= MAX_FAILED_LOGINS_BEFORE_LOCK:
            lock_until_value = _now_ts() + ACCOUNT_LOCK_MINUTES * 60
            failed_count = 0
        db.execute(
            "UPDATE users SET failed_login_count = ?, lock_until = ?, updated_at = ? WHERE id = ?",
            (failed_count, lock_until_value, _now_ts(), user["id"]),
        )
        db.commit()
        _register_ip_attempt(ip)
        registrar_auditoria("login_falhou", user_id=int(user["id"]), details=f"email={email}")
        flash("Credenciais inválidas.", "danger")
        return redirect(url_for("auth_bp.login"))

    session.clear()
    session["user_id"] = int(user["id"])
    session["csrf_token"] = secrets.token_urlsafe(32)
    session.permanent = True

    db.execute(
        "UPDATE users SET failed_login_count = 0, lock_until = NULL, updated_at = ? WHERE id = ?",
        (_now_ts(), user["id"]),
    )
    db.commit()

    registrar_auditoria("login_ok", user_id=int(user["id"]), details=f"email={email}")
    next_url = request.args.get("next") or _default_route_for_user(_row_to_user(user))
    return redirect(next_url)


@auth_bp.post("/logout")
@login_required
def logout():
    _require_csrf()
    registrar_auditoria("logout", user_id=session.get("user_id"), details="logout manual")
    session.clear()
    flash("Sessão encerrada com sucesso.", "success")
    return redirect(url_for("auth_bp.login"))


# =========================
# Admin usuários
# =========================
AVAILABLE_SCREENS = [
    ("home", "Início"),
    ("principal", "MLBS em análise"),
    ("enviando", "Enviando"),
    ("ecommerce", "E-commerce"),
    ("compras", "Compras"),
    ("acompanhamento", "Acompanhamento"),
    ("homologar", "Homologar"),
    ("naoEnviar", "Não enviar"),
    ("metricas_full", "Lotes de envio"),
    ("conferencia", "Conferência"),
    ("picking", "Picking"),
]


@auth_bp.get("/admin/usuarios")
@admin_required
def admin_users():
    db = get_auth_db()
    rows = db.execute(
        "SELECT id, nome, email, is_admin, ativo, created_at FROM users ORDER BY nome ASC"
    ).fetchall()

    usuarios = []
    for row in rows:
        permissoes = db.execute(
            "SELECT screen_key FROM user_screen_permissions WHERE user_id = ? ORDER BY screen_key",
            (row["id"],),
        ).fetchall()
        usuarios.append(
            {
                "id": int(row["id"]),
                "nome": row["nome"],
                "email": row["email"],
                "is_admin": bool(row["is_admin"]),
                "ativo": bool(row["ativo"]),
                "created_at": int(row["created_at"]),
                "permissoes": [p["screen_key"] for p in permissoes],
            }
        )

    return render_template(
        "admin_usuarios.html",
        usuarios=usuarios,
        telas_disponiveis=AVAILABLE_SCREENS,
    )


@auth_bp.post("/admin/usuarios/criar")
@admin_required
def admin_users_create():
    _require_csrf()
    nome = (request.form.get("nome") or "").strip()
    email = _normalize_email(request.form.get("email") or "")
    senha = request.form.get("senha") or ""
    permissoes = set(request.form.getlist("screens"))
    is_admin = 1 if request.form.get("is_admin") == "1" else 0
    ativo = 1 if request.form.get("ativo") == "1" else 0

    if not nome or not email or not senha:
        flash("Preencha nome, email e senha.", "danger")
        return redirect(url_for("auth_bp.admin_users"))

    password_errors = _validate_password_strength(senha)
    if password_errors:
        for err in password_errors:
            flash(err, "danger")
        return redirect(url_for("auth_bp.admin_users"))

    allowed_screen_keys = {key for key, _label in AVAILABLE_SCREENS}
    permissoes = {screen for screen in permissoes if screen in allowed_screen_keys}

    db = get_auth_db()
    try:
        cur = db.execute(
            """
            INSERT INTO users (nome, email, password_hash, is_admin, ativo)
            VALUES (?, ?, ?, ?, ?)
            """,
            (nome, email, generate_password_hash(senha, method="scrypt"), is_admin, ativo),
        )
        user_id = int(cur.lastrowid)
        for screen in permissoes:
            db.execute(
                "INSERT INTO user_screen_permissions (user_id, screen_key) VALUES (?, ?)",
                (user_id, screen),
            )
        db.commit()
        registrar_auditoria("usuario_criado", user_id=user_id, details=f"email={email}")
        flash("Funcionário criado com sucesso.", "success")
    except sqlite3.IntegrityError:
        flash("Já existe um usuário com esse email.", "danger")

    return redirect(url_for("auth_bp.admin_users"))


@auth_bp.post("/admin/usuarios/<int:user_id>/atualizar")
@admin_required
def admin_users_update(user_id: int):
    _require_csrf()
    nome = (request.form.get("nome") or "").strip()
    email = _normalize_email(request.form.get("email") or "")
    senha = request.form.get("senha") or ""
    permissoes = set(request.form.getlist("screens"))
    is_admin = 1 if request.form.get("is_admin") == "1" else 0
    ativo = 1 if request.form.get("ativo") == "1" else 0

    if not nome or not email:
        flash("Nome e email são obrigatórios.", "danger")
        return redirect(url_for("auth_bp.admin_users"))

    allowed_screen_keys = {key for key, _label in AVAILABLE_SCREENS}
    permissoes = {screen for screen in permissoes if screen in allowed_screen_keys}

    db = get_auth_db()

    if senha:
        password_errors = _validate_password_strength(senha)
        if password_errors:
            for err in password_errors:
                flash(err, "danger")
            return redirect(url_for("auth_bp.admin_users"))
        db.execute(
            """
            UPDATE users
            SET nome = ?, email = ?, password_hash = ?, is_admin = ?, ativo = ?, updated_at = ?, lock_until = NULL, failed_login_count = 0
            WHERE id = ?
            """,
            (nome, email, generate_password_hash(senha, method="scrypt"), is_admin, ativo, _now_ts(), user_id),
        )
    else:
        db.execute(
            """
            UPDATE users
            SET nome = ?, email = ?, is_admin = ?, ativo = ?, updated_at = ?
            WHERE id = ?
            """,
            (nome, email, is_admin, ativo, _now_ts(), user_id),
        )

    db.execute("DELETE FROM user_screen_permissions WHERE user_id = ?", (user_id,))
    for screen in permissoes:
        db.execute(
            "INSERT INTO user_screen_permissions (user_id, screen_key) VALUES (?, ?)",
            (user_id, screen),
        )
    db.commit()
    registrar_auditoria("usuario_atualizado", user_id=user_id, details=f"email={email}")
    flash("Usuário atualizado com sucesso.", "success")
    return redirect(url_for("auth_bp.admin_users"))


@auth_bp.post("/admin/usuarios/<int:user_id>/resetar-bloqueio")
@admin_required
def admin_users_unlock(user_id: int):
    _require_csrf()
    db = get_auth_db()
    db.execute(
        "UPDATE users SET failed_login_count = 0, lock_until = NULL, updated_at = ? WHERE id = ?",
        (_now_ts(), user_id),
    )
    db.commit()
    registrar_auditoria("usuario_desbloqueado", user_id=user_id)
    flash("Bloqueio limpo com sucesso.", "success")
    return redirect(url_for("auth_bp.admin_users"))
