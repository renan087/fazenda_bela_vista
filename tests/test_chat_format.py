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
