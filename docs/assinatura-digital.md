# Assinatura digital no SiSFarm

A Central de Certificados fica em `/certificados-digitais`. A área de assinatura e validação fica em `/assinatura-digital`. O SiSFarm executa a assinatura e a validação com pyHanko. A prévia usa PDF.js 5.4.624 servido pelo próprio SiSFarm, com os arquivos e licenças da distribuição oficial.

## Configuração

- Gere duas chaves independentes de 32 bytes, codificadas em Base64, por exemplo com `openssl rand -base64 32`. Configure `DIGITAL_CERTIFICATE_KEY` para o cofre central e `DIGITAL_DOCUMENT_KEY` para os PDFs. Guarde as chaves fora do banco e preserve-as ao restaurar backups. A troca de chave requer recifrar os registros correspondentes.
- Preserve `NFE_CERT_MASTER_KEY` para abrir o A1 fiscal legado. A Central reutiliza o registro cifrado original e habilita cada uso separadamente.
- Configure o SMTP do SiSFarm para entregar códigos de autorização. Cada assinatura exige a confirmação do PDF e da aparência, o código de uso único enviado à conta vinculada e a senha do A1.
- A validação usa as âncoras da [AC-Raiz do ITI](https://www.gov.br/iti/pt-br/assuntos/repositorio/repositorio-ac-raiz) e as evidências de certificadoras. Quando uma AC legítima usa domínio próprio, verifique esse domínio no repositório da AC e inclua o host exato em `DIGITAL_VALIDATION_HOSTS`. Os hosts padrão estão em `trust.py`. Evidência indisponível pode produzir o resultado **Indeterminada**. Uma nova assinatura só é emitida com revogação e confiança comprovadas.

## Vínculo do titular ou responsável

Um operador autorizado a verificar identidade deve conferir, por canal independente, a identidade da pessoa, o controle da conta individual, o vínculo com o certificado e a política da certificadora. Para e-CPF, deve conferir o titular. Para e-CNPJ, deve conferir o responsável de uso. Use uma referência do atendimento sem CPF, senha ou documento pessoal no texto.

O comando abaixo é executado na implantação em que o certificado foi cadastrado. A pessoa verificada informa o CPF e a senha do A1 em prompts locais sem eco. O comando confere o CPF com o campo ICP-Brasil do certificado e grava a referência da verificação.

```sh
python -m scripts.verify_digital_certificate_holder \
  --certificate ID_DA_CENTRAL \
  --user titular@example.com \
  --reference PROTOCOLO_DA_VERIFICACAO
```

O vínculo habilita a conta a decidir sobre cada finalidade. A própria pessoa deve entrar na Central e autorizar o uso desejado. Alterar o vínculo invalida as autorizações da Central e requer novo consentimento. O fluxo fiscal legado usa a seleção da NF-e e mantém o escopo de fazenda e ambiente.

## Homologação obrigatória no VALIDAR

A assinatura fica bloqueada por padrão. Para gerar os arquivos de homologação, use uma implantação fora de produção com `DIGITAL_SIGNING_PILOT_FARM_IDS` limitado às fazendas de teste. Use um A1 ICP-Brasil vigente com autorização do responsável e PDFs criados para esse fim.

1. Gere um PDF com assinatura visível, um PDF com assinatura invisível e um PDF com pelo menos duas assinaturas em revisões incrementais.
2. Submeta esses três arquivos ao [VALIDAR do ITI](https://validar.iti.gov.br/). Confira o resultado **Aprovado** em cada assinatura e arquive o relatório de cada PDF. Confira o vínculo entre o PDF testado e o relatório antes de registrar as evidências.
3. Na implantação de produção, coloque os três PDFs, os relatórios e o manifesto no mesmo diretório privado, fora de `app/static`. Configure `DIGITAL_SIGNING_ITI_RECEIPTS` com o caminho do manifesto. Registre os hashes com `shasum -a 256 nome-do-arquivo`.

O manifesto tem `policy_oid` igual a `2.16.76.1.7.1.11.1.3` e `policy_sha256` igual a `23da544aef71f7a75dc85fa6e17a83875741e4baef41ec178258a5c86ace54dd`. Em `confirmations`, informe as chaves `visible`, `invisible` e `multiple`. Cada registro contém `result` com o valor `Aprovado`, `pdf` e `report` com os nomes dos arquivos, e `pdf_sha256` e `report_sha256` com os hashes em hexadecimal.

O manifesto é um registro operacional da aprovação. O serviço confere a política, a presença e a integridade dos seis arquivos. Repita a homologação se mudar a produção do perfil PAdES ou a política aplicável. A política AD-RB usa o hash interno do DER no CMS, conforme o [DOC-ICP-15.03](https://repositorio.iti.gov.br/instrucoes-normativas/IN2021_03_DOC-ICP-15.03.htm). A hora de autorização fica no histórico do SiSFarm. A referência temporal independente depende de carimbo do tempo verificável.
