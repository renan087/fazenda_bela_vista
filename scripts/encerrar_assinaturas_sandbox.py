"""Encerra os dados de teste (Asaas sandbox) das assinaturas antes de ativar a producao.

Uso:
    python scripts/encerrar_assinaturas_sandbox.py              # apenas mostra o que seria feito
    python scripts/encerrar_assinaturas_sandbox.py --confirmar  # aplica

O que faz (somente registros do ambiente sandbox):
- assinaturas atuais passam a historico (status "encerrada_teste"), preservando cobrancas;
- links de cartao pendentes sao encerrados;
- o vinculo com o cliente do sandbox e removido (os dados comerciais da organizacao ficam),
  para que o cliente de producao possa ser vinculado.
Nada e enviado ao Asaas.
"""

import argparse
import json

from app.db.session import SessionLocal
from app.models import AuditLog, OrganizationBilling, OrganizationCardMigration, OrganizationSubscription


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--confirmar", action="store_true", help="aplica as alteracoes")
    args = parser.parse_args()
    with SessionLocal() as db:
        subscriptions = db.query(OrganizationSubscription).filter_by(environment="sandbox", is_current=True).all()
        sandbox_org_ids = {row.organization_id for row in db.query(OrganizationSubscription.organization_id).filter_by(environment="sandbox").all()}
        migrations = db.query(OrganizationCardMigration).filter(OrganizationCardMigration.is_open.is_(True), OrganizationCardMigration.organization_id.in_(sandbox_org_ids or {-1})).all()
        billings = db.query(OrganizationBilling).filter(OrganizationBilling.asaas_environment == "sandbox", OrganizationBilling.asaas_customer_id.isnot(None)).all()
        print(f"Assinaturas de teste atuais: {len(subscriptions)}")
        print(f"Links de cartão pendentes: {len(migrations)}")
        print(f"Clientes do sandbox vinculados: {len(billings)}")
        if not args.confirmar:
            print("Nada foi alterado. Rode novamente com --confirmar para aplicar.")
            return
        for subscription in subscriptions:
            subscription.is_current = False
            subscription.status = "encerrada_teste"
        for migration in migrations:
            migration.is_open = False
            migration.status = "encerrado"
        for billing in billings:
            billing.asaas_customer_id = None
            billing.asaas_environment = None
        db.add(AuditLog(event_type="comercial.ambiente.encerrar_sandbox", outcome="success", metadata_json=json.dumps({
            "assinaturas": len(subscriptions), "checkouts": len(migrations), "clientes": len(billings)})))
        db.commit()
        print("Dados de teste encerrados. Histórico preservado.")


if __name__ == "__main__":
    main()
