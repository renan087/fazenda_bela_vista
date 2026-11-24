# Assinatura digital no SiSFarm

A Central de Certificados fica em `/certificados-digitais`. A área de assinatura e validação fica em `/assinatura-digital`. O SiSFarm executa a assinatura e a validação com pyHanko. A prévia usa PDF.js 5.4.624 servido pelo próprio SiSFarm, com os arquivos e licenças da distribuição oficial.

## Configuração

- Gere duas chaves independentes de 32 bytes, codificadas em Base64, por exemplo com `openssl rand -base64 32`. Configure `DIGITAL_CERTIFICATE_KEY` para o cofre central e `DIGITAL_DOCUMENT_KEY` para os PDFs. Guarde as chaves fora do banco e preserve-as ao restaurar backups. A troca de chave requer recifrar os registros correspondentes.
- Preserve `NFE_CERT_MASTER_KEY` para abrir o A1 fiscal legado. A Central reutiliza o registro cifrado original e habilita cada uso separadamente.
- Configure o SMTP do SiSFarm para entregar códigos de autorização. Cada assinatura exige a confirmação do PDF e da aparência, o código de uso único enviado à conta vinculada e a senha do A1.
- A validação usa as âncoras da [AC-Raiz do ITI](https://www.gov.br/iti/pt-br/assuntos/repositorio/repositorio-ac-raiz) e as evidências de certificadoras. Quando uma AC legítima usa domínio próprio, verifique esse domínio no repositório da AC e inclua o host exato em `DIGITAL_VALIDATION_HOSTS`. Os hosts padrão estão em `trust.py`. Evidência indisponível pode produzir o resultado **Indeterminada**. Uma nova assinatura só é emitida com revogação e confiança comprovadas.

## Vínculo do titular ou responsável

Na Central de Certificados, selecione a fazenda, escolha um usuário ativo da organização no campo **Responsável pelo certificado** e clique em **Vincular responsável**. A lista está disponível a contas com permissão para gerenciar certificados. Selecione o titular do e-CPF ou o responsável de uso do e-CNPJ.

A pessoa selecionada entra na própria conta e clica em **Autorizar uso** na finalidade desejada. Para confirmar o primeiro uso após um novo vínculo, o SiSFarm compara o CPF com o certificado e confere a senha do A1. A troca de responsável encerra as autorizações anteriores da Central para o certificado.

## Homologação obrigatória no VALIDAR

A assinatura fica bloqueada por padrão. Para gerar os arquivos de homologação, use uma implantação fora de produção com `DIGITAL_SIGNING_PILOT_FARM_IDS` limitado às fazendas de teste. Use um A1 ICP-Brasil vigente com autorização do responsável e PDFs criados para esse fim.

1. Gere um PDF com assinatura visível, um PDF com assinatura invisível e um PDF com pelo menos duas assinaturas em revisões incrementais.
2. Submeta esses três arquivos ao [VALIDAR do ITI](https://validar.iti.gov.br/). Confira o resultado **Aprovado** em cada assinatura e arquive o relatório de cada PDF. Confira o vínculo entre o PDF testado e o relatório antes de registrar as evidências.
3. Na implantação de produção, coloque os três PDFs, os relatórios e o manifesto no mesmo diretório privado, fora de `app/static`. Configure `DIGITAL_SIGNING_ITI_RECEIPTS` com o caminho do manifesto. Registre os hashes com `shasum -a 256 nome-do-arquivo`.

O manifesto tem `policy_oid` igual a `2.16.76.1.7.1.11.1.3` e `policy_sha256` igual a `23da544aef71f7a75dc85fa6e17a83875741e4baef41ec178258a5c86ace54dd`. Em `confirmations`, informe as chaves `visible`, `invisible` e `multiple`. Cada registro contém `result` com o valor `Aprovado`, `pdf` e `report` com os nomes dos arquivos, e `pdf_sha256` e `report_sha256` com os hashes em hexadecimal.

O manifesto é um registro operacional da aprovação. O serviço confere a política, a presença e a integridade dos seis arquivos. Repita a homologação se mudar a produção do perfil PAdES ou a política aplicável. A política AD-RB usa o hash interno do DER no CMS, conforme o [DOC-ICP-15.03](https://repositorio.iti.gov.br/instrucoes-normativas/IN2021_03_DOC-ICP-15.03.htm). A hora de autorização fica no histórico do SiSFarm. A referência temporal independente depende de carimbo do tempo verificável.
