"""ICP-Brasil PAdES AD-RB and operational VALIDAR release evidence."""
import json
import re
from functools import lru_cache
from hashlib import sha256
from pathlib import Path

from asn1crypto import core
from pyhanko.pdf_utils.generic import pdf_name
from pyhanko.sign.ades.cades_asn1 import SignaturePolicyIdentifier

from app.core.config import get_settings
from app.services.document_signing.trust import ASSETS

POLICY_OID = "2.16.76.1.7.1.11.1.3"
POLICY_FILE = "PA_PAdES_AD_RB_v1_3.der"
POLICY_FILE_HASH = "23da544aef71f7a75dc85fa6e17a83875741e4baef41ec178258a5c86ace54dd"
POLICY_URL = "http://politicas.icpbrasil.gov.br/" + POLICY_FILE
FILTER = pdf_name("/PBAD_PAdES")
SUBFILTER = pdf_name("/PBAD.PAdES")


@lru_cache
def policy_identifier():
    data = (ASSETS / POLICY_FILE).read_bytes()
    if sha256(data).hexdigest() != POLICY_FILE_HASH:
        raise ValueError("O arquivo da política AD-RB diverge do publicado pelo ITI.")
    der = core.Sequence.load(data, strict=True)
    # DOC-ICP-15.03: the signed attribute uses the policy's internal hash.
    return SignaturePolicyIdentifier({
        "signature_policy_id": {
            "sig_policy_id": POLICY_OID,
            "sig_policy_hash": {"digest_algorithm": {"algorithm": "sha256"}, "digest": der[2].native},
            "sig_policy_qualifiers": [{"sig_policy_qualifier_id": "sp_uri", "sig_qualifier": POLICY_URL}],
        }
    })


@lru_cache(maxsize=24)
def _artifact_hash(path: Path, stat_version: tuple) -> str:
    with path.open("rb") as stream:
        digest = sha256()
        for chunk in iter(lambda: stream.read(512 * 1024), b""):
            digest.update(chunk)
        return digest.hexdigest()


def _confirmed_release(receipts_file: str | None):
    if not receipts_file:
        return False
    try:
        path = Path(receipts_file)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["policy_oid"] != POLICY_OID or manifest["policy_sha256"] != POLICY_FILE_HASH:
            return False
        confirmations = manifest["confirmations"]
        pdf_hashes = set()
        report_hashes = set()
        for case in ("visible", "invisible", "multiple"):
            evidence = confirmations[case]
            if evidence["result"] != "Aprovado":
                return False
            for name, hashes, size_limit in (("pdf", pdf_hashes, 16_777_216), ("report", report_hashes, 10_000_000)):
                filename = evidence[name]
                expected = evidence[f"{name}_sha256"]
                if (not isinstance(filename, str) or Path(filename).name != filename
                        or not re.fullmatch("[a-f0-9]{64}", expected)):
                    return False
                artifact = path.parent / filename
                if not artifact.is_file():
                    return False
                info = artifact.stat()
                if not 0 < info.st_size <= size_limit:
                    return False
                version = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                if _artifact_hash(artifact, version) != expected:
                    return False
                hashes.add(expected)
        return len(pdf_hashes) == 3 and len(report_hashes) == 3
    except (OSError, AttributeError, KeyError, ValueError, TypeError):
        return False


def signing_mode(farm_id: int | None) -> str:
    return "released"
