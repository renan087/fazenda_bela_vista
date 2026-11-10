import pytest

from app.services import car_public_lookup


def test_normalizes_and_validates_car_number():
    assert car_public_lookup.normalize_car_number(" ba-1234567-abcdef ") == "BA-1234567-ABCDEF"
    assert (
        car_public_lookup.normalize_car_number("BA-2925006-E817.CC3F.22DB.437C.871E.523B.4E66.4CAA")
        == "BA-2925006-E817CC3F22DB437C871E523B4E664CAA"
    )

    with pytest.raises(car_public_lookup.CarPublicLookupError):
        car_public_lookup.normalize_car_number("numero-invalido")


def test_returns_public_geometry_and_metadata(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "features": [
                    {
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [[[[ -42.0, -14.0 ], [ -42.1, -14.0 ], [ -42.0, -14.1 ], [ -42.0, -14.0 ]]]],
                        },
                        "properties": {
                            "cod_imovel": "BA-1234567-ABCDEF",
                            "municipio": "GUANAMBI",
                            "uf": "BA",
                            "area": "120.45",
                            "status_imovel": "ATIVO",
                        },
                    }
                ]
            }

    class Client:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(car_public_lookup.httpx, "Client", Client)

    result = car_public_lookup.lookup_public_car("BA-1234567.ABCDEF")

    assert result["geometry"]["type"] == "Polygon"
    assert result["location"] == "Guanambi, BA"
    assert result["total_area"] == 120.45
    assert result["status"] == "ATIVO"


def test_rejects_multipart_geometry_that_editor_cannot_preserve(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "features": [
                    {
                        "geometry": {"type": "MultiPolygon", "coordinates": [[], []]},
                        "properties": {},
                    }
                ]
            }

    class Client:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def get(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(car_public_lookup.httpx, "Client", Client)

    with pytest.raises(car_public_lookup.CarPublicLookupError, match="perimetro geografico"):
        car_public_lookup.lookup_public_car("BA-1234567-ABCDEF")


def test_uses_curl_fallback_after_tls_failure(monkeypatch):
    class BrokenClient:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            raise car_public_lookup.httpx.ConnectError("TLS failed")

        def __exit__(self, *_args):
            return False

    class CurlResult:
        returncode = 0
        stderr = ""
        stdout = '{"features": []}'

    monkeypatch.setattr(car_public_lookup.httpx, "Client", BrokenClient)
    monkeypatch.setattr(car_public_lookup.shutil, "which", lambda _name: "/usr/bin/curl")
    monkeypatch.setattr(car_public_lookup.subprocess, "run", lambda *_args, **_kwargs: CurlResult())

    assert car_public_lookup._fetch_public_payload({"request": "GetFeature"}) == {"features": []}
