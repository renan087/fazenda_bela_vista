from app.services.chat_format import to_html, to_whatsapp


def test_html_bold_lists_and_escape():
    html = str(to_html("Saldo do **Diesel S10**:\n\n- **120 L** na fazenda;\n- mínimo 50 L.\n\n1. Confira\n2. Confirme\n<script>x</script>"))
    assert "<strong>Diesel S10</strong>" in html
    assert "<ul><li><strong>120 L</strong> na fazenda;</li><li>mínimo 50 L.</li></ul>" in html
    assert "<ol><li>Confira</li><li>Confirme</li></ol>" in html
    assert "<script>" not in html and "&lt;script&gt;" in html


def test_html_heading_becomes_bold_and_plain_text_kept():
    assert str(to_html("## Resumo\nTudo certo.")) == "<strong>Resumo</strong>\nTudo certo."
    assert str(to_html(None)) == ""


def test_whatsapp_uses_single_asterisk_and_bullets():
    assert to_whatsapp("Lancei **100 L** de **Diesel S10**.\n- item um\n## Total") == \
        "Lancei *100 L* de *Diesel S10*.\n• item um\n*Total*"


def test_long_block_is_split_into_paragraphs_without_breaking_numbers():
    from app.services.chat_format import paragraphs
    text = ("O saldo do Diesel S10 é de 1.250,00 L na fazenda. A última compra foi em 10/11/2026 por R$ 6,15 o litro. "
            "Desde então houve três saídas para os tratores da colheita. O consumo médio ficou em 85 L por dia. "
            "Se continuar assim, o estoque dura cerca de duas semanas.")
    result = paragraphs(text)
    assert result.count("\n\n") == 2
    assert "1.250,00 L" in result and "R$ 6,15" in result
    assert paragraphs("Curto. Texto. Aqui.") == "Curto. Texto. Aqui."
    assert paragraphs("Já tem\nquebra. " + "x" * 300) == "Já tem\nquebra. " + "x" * 300
