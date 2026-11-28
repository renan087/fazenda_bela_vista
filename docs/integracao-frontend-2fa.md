# Integração do autenticador no SiSFarm

O backend disponibiliza o vínculo por QR Code ou chave manual, a confirmação por TOTP e a recuperação com dez códigos de uso único. O frontend deve integrar esses fluxos em **Meu Perfil → Segurança** e em **`/login/verificacao`**, usando a sessão web atual.

## Sessão e formulários

- Envie requisições à mesma origem com o cookie de sessão. Os POSTs aceitam `FormData` ou `application/x-www-form-urlencoded` e exigem o campo `csrf_token`.
- Em Meu Perfil, obtenha o CSRF com o GET de estado. No login, use o CSRF da página atual ou do GET da verificação. Para respostas JSON nos POSTs de verificação e reenvio, envie `Accept: application/json`.
- A sessão pendente de login só permite concluir a verificação. As rotas de Meu Perfil exigem uma sessão autenticada. Sessões expiradas de Meu Perfil podem redirecionar para `/login`. Se a sessão mudar durante uma operação protegida, a resposta pode ser `401`.

## Meu Perfil

Prefixo das rotas de configuração `/meu-perfil/seguranca/autenticador`.

| Método e caminho | Campos além do CSRF | Resposta de sucesso |
| --- | --- | --- |
| `GET` no prefixo | — | `enabled`, `enrollment_available`, `email_enabled`, `preferred_method`, `recovery_codes_remaining` e `csrf_token` |
| `POST /vincular/iniciar` | `current_password` | `stage: "email"`, `email` e `expires_at` quando for preciso verificar o e-mail. Em outro caso, retorna os dados da etapa `"totp"`. |
| `POST /vincular/email` | `code` | Dados da etapa `"totp"` após a verificação do e-mail. |
| `POST /vincular/confirmar` | `code` | Estado atualizado e `recovery_codes` com dez códigos. |
| `POST /vincular/cancelar` | — | `{"cancelled": true}` |
| `POST /gerenciar` | `action`, `current_password`, `code`, `method` e, para preferência, `preferred_method` | Estado atualizado. A regeneração também retorna `recovery_codes`. |

Na etapa `"totp"`, use `qr_code_data_url` diretamente em uma imagem e ofereça `manual_key` para cópia. A resposta inclui `expires_at`, `digits: 6`, `period: 30` e `algorithm: "SHA1"`. A confirmação consome o código usado no vínculo. Para autenticar novamente dentro dos mesmos 30 segundos, aguarde o próximo código no aplicativo.

O estado pode indicar `preferred_method` igual a `none`, `email`, `totp` ou `email_then_totp`. Para `/gerenciar`, use as ações abaixo. Em todas, `method` aceita `totp` ou `recovery` e o padrão é `totp`.

| Ação | Comportamento |
| --- | --- |
| `preference` | Aceita `preferred_method=totp` ou `email_then_totp`. A opção com e-mail está disponível quando `email_enabled=true`. |
| `regenerate_recovery_codes` | Invalida os códigos anteriores e retorna dez novos. |
| `disable` | Desvincula o aplicativo e invalida seus códigos de recuperação. A conta volta a seguir sua configuração de verificação por e-mail. |

Mostre os códigos de recuperação assim que chegarem. Ofereça uma opção de cópia e confirmação de que a pessoa os guardou. O GET devolve apenas a quantidade disponível. Mantenha chave, QR e códigos na memória da tela e retire-os ao sair do fluxo. Exclua esses valores de armazenamento persistente e de telemetria. Se `enrollment_available=false`, desabilite o início de uma nova vinculação.

## Login

1. Mantenha o formulário de credenciais em `POST /login` e o fluxo do Google. Quando houver uma segunda etapa, o servidor redireciona para `/login/verificacao`.
2. Consulte `GET /login/verificacao/estado` para obter `stage`, `authenticator_required`, `methods`, `email`, `resend_available_in`, `expires_at` e `csrf_token`. O template também recebe `authenticator_challenge` quando existe uma verificação por aplicativo.
3. Em `stage: "email"`, envie `POST /login/verificacao` com `method=email` e o `code`. Exiba reenvio via `POST /login/verificacao/reenviar` depois de `resend_available_in` segundos. Com a preferência `email_then_totp`, a confirmação do e-mail retorna `stage: "totp"`. Nesse ponto a sessão ainda está pendente.
4. Em `stage: "totp"`, envie `POST /login/verificacao` com `method=totp` e um código de seis dígitos. Ofereça `method=recovery` com um campo de texto para o código de recuperação. A etapa do aplicativo dispensa o campo `trust_browser` e a opção de reenvio por e-mail.
5. Em `{"stage": "complete", "redirect_url": "/…"}`, navegue para o destino retornado pelo backend. O cookie de sessão já foi atualizado. A redefinição de senha por e-mail devolve a pessoa ao login e mantém a exigência do aplicativo ou do código de recuperação.

O e-mail continua disponível para contas que já o usavam. Na verificação por e-mail com `authenticator_required=false`, o formulário pode usar `trust_browser`. Contas com autenticador devem usar o fluxo web para entrar. A emissão do token pela API continua reservada às contas elegíveis pelo fluxo legado.

## Erros a tratar na interface

As falhas do autenticador retornam `{"detail": {"code": "...", "message": "..."}}`. O FastAPI também pode devolver `detail` como texto para falhas de CSRF ou como lista para campos inválidos.

| HTTP | Códigos | Comportamento da interface |
| --- | --- | --- |
| `400` | `invalid_credentials`, `invalid_code`, `reused_code`, `invalid_preference` | Mostre a mensagem junto ao formulário. Quando o TOTP já foi utilizado, peça o próximo código do aplicativo. |
| `409` | `challenge_expired`, `wrong_stage`, `authenticator_enabled`, `authenticator_disabled` | Recarregue o estado. Se a verificação expirou, reinicie o vínculo ou volte ao login. |
| `429` | `rate_limited`, `resend_cooldown` | Aguarde o tempo indicado pelo cabeçalho `Retry-After`. |
| `503` | `authenticator_unavailable`, `delivery_failed` | Mostre indisponibilidade. Se `methods` incluir `recovery`, mantenha essa opção disponível. |

## Configuração e proteção no backend

O PostgreSQL recebe as tabelas e colunas do autenticador pela rotina de migração da aplicação na inicialização. No Render, configure `TOTP_ENCRYPTION_KEY` no ambiente do serviço web com uma chave Fernet exclusiva e persistente. Gere a chave em um ambiente confiável e guarde-a junto aos segredos da aplicação.

```sh
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

PyOTP valida o TOTP de 30 segundos e QRCode gera o PNG localmente. Os segredos ficam criptografados. Os códigos de recuperação são armazenados por hash e invalidados de forma transacional quando usados. O controle de tentativas é por conta e vale entre sessões. As operações de segurança geram eventos de auditoria `auth.totp.*` sem incluir senhas, segredos ou códigos. [RFC 6238](https://www.rfc-editor.org/rfc/rfc6238.html)
