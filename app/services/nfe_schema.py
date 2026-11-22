"""Validador XSD offline. A ausência de pacote oficial nunca é tratada como sucesso."""
from hashlib import sha256
from pathlib import Path
import json
from lxml import etree

SCHEMA_DIRECTORY = Path(__file__).resolve().parents[1] / 'fiscal' / 'schemas' / 'nfe'
MAX_XML_BYTES = 5 * 1024 * 1024
MAX_SCHEMA_BYTES = 10 * 1024 * 1024


class SchemaUnavailable(ValueError):
    pass


class UnsafeXML(ValueError):
    pass


def _load_bundle(directory):
    directory = Path(directory).resolve()
    manifest_path = directory / 'manifest.json'
    try:
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        version, entry, hashes = manifest['version'], manifest['entry'], manifest['files']
        if not isinstance(version, str) or not version.strip() or not isinstance(hashes, dict) or not hashes:
            raise ValueError
        contents = {}
        for name, digest in hashes.items():
            if (not isinstance(name, str) or not name.endswith('.xsd') or Path(name).name != name
                    or not isinstance(digest, str) or len(digest) != 64):
                raise ValueError
            path = directory / name
            if path.is_symlink() or path.resolve().parent != directory:
                raise ValueError
            data = path.read_bytes()
            if len(data) > MAX_SCHEMA_BYTES or sha256(data).hexdigest() != digest.lower():
                raise ValueError
            contents[name] = data
        if entry not in contents:
            raise ValueError
        return version, entry, contents
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        raise SchemaUnavailable('Pacote XSD local ausente ou inválido; validação oficial indisponível.') from None


class _BundleResolver(etree.Resolver):
    def __init__(self, contents):
        super().__init__()
        self.contents = contents

    def resolve(self, url, pubid, context):
        # Somente referências simples a arquivos incluídos e verificados pelo manifesto.
        if not isinstance(url, str) or '/' in url or '\\' in url or url not in self.contents:
            raise SchemaUnavailable('Referência XSD não autorizada.')
        return self.resolve_string(self.contents[url], context, base_url=url)


def validate_xml(xml_bytes, *, schema_directory=SCHEMA_DIRECTORY):
    """Valida contra pacote local explícito e íntegro; nunca transmite ou busca recursos."""
    version, entry, contents = _load_bundle(schema_directory)
    if not isinstance(xml_bytes, bytes) or len(xml_bytes) > MAX_XML_BYTES:
        raise UnsafeXML('XML inválido ou maior que o limite permitido.')
    parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True, huge_tree=False)
    parser.resolvers.add(_BundleResolver(contents))
    try:
        schema_root = etree.fromstring(contents[entry], parser, base_url=entry)
        if schema_root.getroottree().docinfo.doctype:
            raise SchemaUnavailable('DTD não é permitida no schema.')
        validator = etree.XMLSchema(schema_root)
    except (etree.XMLSyntaxError, etree.XMLSchemaParseError, OSError, SchemaUnavailable):
        raise SchemaUnavailable('Não foi possível carregar o pacote XSD local com segurança.') from None
    try:
        root = etree.fromstring(xml_bytes, parser)
        if root.getroottree().docinfo.doctype:
            raise UnsafeXML('DTD não é permitida no documento.')
    except etree.XMLSyntaxError:
        raise UnsafeXML('XML malformado ou inseguro.') from None
    valid = validator.validate(root)
    return {'valid': valid, 'schema_version': version,
            'errors': [{'line': error.line, 'message': error.message[:300]} for error in list(validator.error_log)[:20]]}
