# Guia Prático de Homologação NF-e — Produtor Rural PF na Bahia (SEFAZ-BA)

Este guia resume **exatamente o que depende do produtor e do contador** para sair da etapa de testes fictícios offline e iniciar os testes reais em **homologação (`tpAmb=2`)** na SEFAZ-BA.

> **Importante:** o ambiente de homologação da SEFAZ emite notas **sem validade fiscal/jurídica**, mas exige credenciamento real, certificado digital válido e conferência contábil dos códigos tributários.

---

## Passo 1 — Credenciamento na SEFAZ-BA (Produtor Rural Pessoa Física)
1. Confirme sua **Inscrição Estadual (IE)** ativa de Produtor Rural na Bahia.
2. Solicite o **credenciamento específico para emissão de NF-e (modelo 55)** no portal da SEFAZ-BA (em *Inspetoria Eletrônica → ICMS → Cadastro / NF-e*).
   - **Atenção:** ter acesso ao **DT-e** (Domicílio Tributário Eletrônico) **não** significa estar credenciado para emitir NF-e. Confirme expressamente o credenciamento em **Homologação** primeiro (e depois Produção).
3. Verifique se você já utiliza outro emissor (ex.: Emissor gratuito, Nota Fiscal Fácil ou emissor do contador):
   - Para produtor rural PF (emitente CPF), a NT 2018.001 reserva a faixa de séries **`920` a `969`**.
   - **Coordenação obrigatória:** escolha uma série exclusiva para o SiSFarm (ex.: `920` se nenhum outro sistema a utiliza, ou `921` se `920` já estiver em uso) para nunca colidir numeração.

---

## Passo 2 — Certificado Digital e-CPF A1 (`.pfx`)
1. Adquira um **Certificado Digital e-CPF tipo A1** (arquivo `.pfx` ou `.p12`, validade de 1 ano) vinculado ao **mesmo CPF** cadastrado na IE de produtor rural.
   - Certificados tipo **A3** (token USB/cartão) não funcionam em servidor web (Render). Para emissão própria na nuvem, precisa ser **A1**.
2. Guarde o arquivo `.pfx` e a senha em local seguro. **Nunca envie o `.pfx` ou a senha por Git/repositório.**
3. Configure no ambiente de produção/homologação (Render → *Environment Variables*) uma chave mestra aleatória com pelo menos 32 caracteres:
   - `NFE_CERT_MASTER_KEY`: usada exclusivamente pelo cofre `AES-256-GCM` para criptografar o `.pfx` no banco (a chave mestra nunca vai para o banco).
   - `NFE_ENABLE_SEFAZ_NETWORK`: manter `false` até concluir os passos 1 a 4; ativar como `true` somente na janela de testes em homologação.

---

## Passo 3 — Parecer do Contador (Enquadramento Fiscal na Bahia)
Peça ao seu contador a confirmação por escrito (para registrarmos em `NfeTaxProfile` com responsável e vigência) dos seguintes itens para **venda de café na Bahia (Produtor Rural PF → Comprador PF dentro da BA)**:
1. **CRT do emitente** (ex.: `3` – Regime Normal ou outro aplicável ao produtor rural PF na BA).
2. **Descrição fiscal do produto** e **NCM** exato do café vendido (ex.: café cru em grão, beneficiado, etc.).
3. **Natureza da operação** e **CFOP** para venda interna na Bahia.
4. **Origem da mercadoria (`orig`)** e **CST de ICMS** aplicáveis (ex.: isento/diferido/tributado — e se exige `vBC`, `pICMS`, `vICMS` ou observação legal em `infCpl`).
5. **Reforma Tributária (IBS/CBS — NT 2025.002-RTC):** confirmar se o produtor rural PF já precisa informar `IBSCBS` (`CST`, `cClassTrib`, alíquotas de teste) em homologação/produção na data vigente.
6. **Unidade comercial e tributável** (`SC`, `KG`, etc.) e regra de conversão a partir das sacas da venda.
7. **Indicadores da operação:** `indIEDest` do comprador (contribuinte / isento / não contribuinte), `indFinal`, `indPres` e modalidade de frete (`modFrete`).

---

## Passo 4 — Verificação Oficial dos Schemas XSD e URLs da SEFAZ-BA
Antes de destravar a transmissão em homologação:
1. Baixar diretamente do Portal Nacional da NF-e o pacote de schemas vigente (`PL_010h` ou mais recente aplicável à data) e conferir o SHA-256 com o pacote em `app/fiscal/schemas/nfe/manifest.json` para então aprovar `reviewed: true`.
2. Conferir no Portal Nacional (*Serviços → Relação de Serviços Web → Homologação → BA*) as URLs exatas dos serviços da SEFAZ-BA:
   - `NfeStatusServico4`
   - `NfeAutorizacao4`
   - `NfeRetAutorizacao4`
   - `NfeConsultaProtocolo4`
   - `RecepcaoEvento4`
   - `NfeInutilizacao4`

---

## Passo 5 — Roteiro de Testes em Homologação (`tpAmb=2`)
Quando os Passos 1 a 4 estiverem confirmados, executaremos juntos na seguinte ordem:
1. **Teste de Status (`NfeStatusServico4`):** confirmar `cStat=107` (Serviço em Operação) validando o mTLS do seu certificado A1 na SEFAZ-BA.
2. **Autorização em Homologação (`NfeAutorizacao4`):** enviar 1 nota de teste (`tpAmb=2`, destinatário de homologação conforme MOC: `NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL`) e validar `cStat=100` + geração de `nfeProc` + DANFE.
3. **Consulta de Situação (`NfeConsultaProtocolo4`):** consultar a chave autorizada e confirmar o protocolo.
4. **Carta de Correção (`110110`) e Cancelamento (`110111`):** testar os dois eventos em homologação confirmando `cStat=135`.
5. **Auditoria de Segurança e Virada para Produção:** somente após 100% dos testes em homologação sem rejeições e revisão de backups/permissões, habilitar o ambiente `producao` (`tpAmb=1`).
