"""Sandbox subscription creation; failures never trigger an automatic retry."""

import httpx


def create_sandbox_subscription(*, api_key: str, payload: dict) -> tuple[dict | None, str | None]:
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                "https://api-sandbox.asaas.com/v3/subscriptions",
                headers={"access_token": api_key, "User-Agent": "SiSFarm/AsaasIntegration", "Accept": "application/json"},
                json=payload,
            )
        if response.status_code not in (200, 201):
            return None, f"Asaas retornou HTTP {response.status_code}."
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("id"), str) or not data["id"].strip() or len(data["id"].strip()) > 80:
            return None, "O Asaas não confirmou o identificador da assinatura."
        return data, None
    except (httpx.HTTPError, ValueError):
        return None, "Não foi possível confirmar a resposta do Asaas."
