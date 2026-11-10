import pytest

from app.services import car_public_lookup


def test_normalizes_and_validates_car_number():
    assert car_public_lookup.normalize_car_number(" ba-1234.abcd ") == "BA-1234.ABCD"

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
                            "cod_imovel": "BA-1234.ABCD",
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

    result = car_public_lookup.lookup_public_car("BA-1234.ABCD")

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
        car_public_lookup.lookup_public_car("BA-1234.ABCD")
