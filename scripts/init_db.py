from app.db.init_db import create_tables, seed_admin, seed_demo_data
from app.db.session import SessionLocal


def main() -> None:
    create_tables()
    with SessionLocal() as db:
        seed_admin(db)
        seed_demo_data(db)
    print("Banco inicializado com sucesso.")


if __name__ == "__main__":
    main()
