"""Bind a verified ICP-Brasil certificate holder's individual SiSFarm account."""
import json
from argparse import ArgumentParser
from getpass import getpass

from app.db.session import SessionLocal
from app.models import AuditLog, Farm, User
from app.models.digital_certificate import DigitalCertificate
from app.services import digital_certificates as certificates


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--certificate", required=True, help="ID exibido na Central de Certificados")
    parser.add_argument("--user", required=True, help="E-mail verificado da conta individual")
    parser.add_argument("--reference", required=True, help="Referência da verificação independente de identidade")
    args = parser.parse_args()
    if not 1 <= len(args.reference.strip()) <= 160:
        parser.error("Informe uma referência de verificação válida.")

    with SessionLocal() as db:
        cert = db.get(DigitalCertificate, args.certificate)
        user = db.query(User).filter_by(email=args.user.strip().lower(), is_active=True).first()
        farm = db.get(Farm, cert.farm_id) if cert is not None else None
        if cert is None or user is None or farm is None or farm.organization_id != cert.organization_id:
            parser.error("Certificado, conta ou fazenda indisponível.")
        cpf = getpass("CPF conferido do titular ou responsável (11 dígitos): ")
        password = getpass("Senha do A1, informada pelo titular ou responsável: ")
        try:
            certificates.verify_responsible_identity(db, cert=cert, user=user, cpf=cpf, password=password,
                                                      reference=args.reference.strip())
        except (ValueError, PermissionError) as exc:
            parser.exit(1, f"{exc}\n")
        db.add(AuditLog(
            organization_id=cert.organization_id, event_type="documentos.vincular_identidade",
            metadata_json=json.dumps({
                "verified_user_id": user.id, "farm_id": cert.farm_id, "certificate_id": cert.id,
                "fingerprint": cert.fingerprint_sha256, "verification_reference": args.reference.strip(),
            }),
        ))
        db.commit()
    print("Identidade vinculada. O titular ou responsável já pode autorizar as finalidades na Central.")


if __name__ == "__main__":
    main()
