# Login + Admin de usuários

Arquivos criados para você incluir no projeto Flask/Jinja.

## Arquivos
- `auth_blueprint.py`
- `templates/login.html`
- `templates/admin_usuarios.html`
- `sql/auth_schema.sql`

## O que esse pacote faz
- Login com senha criptografada usando `scrypt`
- Bloqueio temporário após múltiplas tentativas inválidas
- Rate limit simples por IP
- CSRF token nos formulários
- Sessão com cookies `HttpOnly`
- Painel de administrador para:
  - cadastrar funcionários
  - ativar/desativar usuários
  - trocar senha
  - marcar administrador
  - liberar telas específicas
- Log de auditoria básico

## Como integrar no app Flask
No seu arquivo principal do Flask, adicione algo assim:

```python
import os
from flask import Flask
from auth_blueprint import configure_auth_app, login_required, screen_required

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "troque-isso-em-producao")
app.config["AUTH_DB_PATH"] = os.getenv("AUTH_DB_PATH", "instance/auth_users.db")

configure_auth_app(app)

@app.route("/")
@login_required
@screen_required("principal")
def index():
    return render_template("index(20)-ajustado.html")

@app.route("/compras")
@login_required
@screen_required("compras")
def compras():
    return render_template("compras.html")
```

## Variáveis importantes para produção
Defina no servidor:

```bash
export SECRET_KEY='UMA_CHAVE_LONGA_E_ALEATORIA'
export ADMIN_EMAIL='seu-admin@empresa.com'
export ADMIN_PASSWORD='Troque-Agora-123!'
export FLASK_ENV='production'
```

## Observações importantes de segurança
Isso é uma base forte para o seu sistema, mas para subir em produção de forma realmente séria você ainda deve:
- colocar HTTPS obrigatório
- usar proxy reverso confiável
- guardar banco fora da pasta pública
- trocar SQLite por PostgreSQL/MySQL se houver múltiplos acessos e crescimento
- revisar permissões de servidor
- fazer backup e rotação de logs
- idealmente usar rate limit em Redis e 2FA

## Rotas criadas
- `/login`
- `POST /login`
- `POST /logout`
- `/admin/usuarios`
- `POST /admin/usuarios/criar`
- `POST /admin/usuarios/<id>/atualizar`
- `POST /admin/usuarios/<id>/resetar-bloqueio`

## Telas liberáveis
- `principal`
- `enviando`
- `ecommerce`
- `compras`
- `acompanhamento`
- `homologar`
- `naoEnviar`
