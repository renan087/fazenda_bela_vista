# Prévia interna e futura validação oficial

A prévia XML gerada nesta etapa é um formato interno de diagnóstico, não usa o namespace nem o leiaute da NF-e e não contém chave de acesso, assinatura ou autorização. Não pode ser transmitida à SEFAZ. É gerada a partir do snapshot comercial e dos dados atualmente salvos para o rascunho; os dados não salvos de sugestão do comprador não são incluídos. Não contém regras de imposto nem afirma conformidade legal.

Em 21/11/2026, a [página oficial de schemas](https://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?AspxAutoDetectCookieSupport=1&tipoConteudo=BMPFMBoln3w%3D) listava pacotes 010g/010h e atualizações RTC, mas as tentativas de obter o pacote pelo portal retornaram redirecionamentos sucessivos. Portanto, nenhum schema foi incorporado ou executado nesta etapa.

Antes de construir e validar NF-e para homologação: obter o pacote oficial aplicável à data e à SEFAZ-BA, verificar a procedência, integridade e licença, registrar versão e hash, mapear inclusões XSD e notas técnicas vigentes, aplicar validação offline sem acesso a entidades externas e elaborar exemplos fictícios que exercitem os grupos obrigatórios. A validação XSD não substitui regras fiscais, assinatura, credenciamento ou autorização. Não transmitir a prévia interna.

## Validador offline preparado

O componente `app/services/nfe_schema.py` carrega exclusivamente um pacote local com `manifest.json`, versão, nome do schema principal e SHA-256 de cada XSD. Sem pacote íntegro, retorna indisponível. O parser bloqueia rede, DTD e entidades externas; referências de schemas só podem apontar para arquivos simples presentes no pacote e conferidos pelo manifesto. O limite de tamanho evita documentos excessivos. Os testes usam somente XSDs artificiais, **não oficiais**, e não representam homologação.

Nenhum pacote oficial foi incorporado. Antes de ativar o componente para NF-e é preciso obter os arquivos diretamente da fonte oficial, confirmar a versão aplicável à Bahia, revisar a cadeia de imports e registrar hashes e procedência no repositório. O manifesto é uma verificação de integridade local, não prova autoria nem conformidade da versão. A prévia interna não deve ser submetida como NF-e nem apresentada como schema validado.

## Preparação de pacote para revisão

O portal oficial exibiu o pacote 010h v1.03 em 21/11/2026. Foi possível identificar o link publicado, mas a tentativa de baixar pelo navegador e por requisição direta não retornou o arquivo. O endereço de referência é `https://www.nfe.fazenda.gov.br/portal/exibirArquivo.aspx?conteudo=YhWikFoTxUo=`; é necessário confirmar a versão aplicável antes do uso.

Com um ZIP obtido e conferido na fonte oficial, o utilitário `python scripts/preparar_schemas_nfe.py ARQUIVO.zip DIRETORIO_NOVO --version VERSAO` prepara os XSDs, registra o SHA-256 do ZIP e dos arquivos e impede extração por caminhos indevidos. Ele cria `reviewed: false`; o validador não carregará o pacote enquanto não houver revisão independente da procedência, versão, imports e integridade. Alterar esse indicador não é confirmação de conformidade ou homologação. Qualquer referência não suportada pelo validador deve ser analisada antes de ativá-lo; não modificar automaticamente os XSDs oficiais.

## Pacote recebido para conferência

Foi recebido do usuário o arquivo `PL_010h_v1.03.zip`, com SHA-256 `bcbf6b277f79a6983d69016dfe0430f09b8a191997a4706a24938acddc18dc81`. A integridade do ZIP foi verificada e o nome corresponde à publicação encontrada no Portal Nacional; **essa correspondência e o hash local não comprovam, por si só, a autenticidade oficial dos bytes nem a aplicabilidade na Bahia**. O portal não disponibilizou arquivo verificável por download automatizado nesta sessão.

Os nove XSDs foram preparados em `app/fiscal/schemas/nfe`, com hashes individuais. O schema principal da NF-e e suas referências locais foram compilados offline em um teste temporário. A aplicação mantém `reviewed: false`; a cópia usada nos testes altera esse indicador apenas em diretório temporário. Foram verificados o carregamento do schema e a rejeição de documentos incompletos e da prévia interna; não há ainda exemplo completo de NF-e aceito pelo XSD nem validação das regras tributárias ou SEFAZ.

Para prosseguir: confirmar independentemente a origem e a versão aplicável; revisar as dependências e notas técnicas; somente então registrar a revisão e preparar exemplos fictícios completos. Alterar o indicador sem tal conferência não substitui esses passos. Não houve comunicação de dados fiscais à SEFAZ.

## Exemplo fictício estrutural

O XML enviado pelo usuário tem raiz `preparacao_sisfarm` e serviu para confirmar as seções e campos já coletados; não é NF-e e não foi adicionado ao repositório. A partir do XSD recebido, foi criado um exemplo separado em `tests/fixtures/nfe/estrutura_ficticia_xsd.xml`, com dados e códigos arbitrários apenas para verificar a estrutura. O arquivo usa `tpAmb=2`, chave fictícia e valores de assinatura e certificado artificiais (`AA==`). Não é assinado, não tem validade fiscal e **jamais deve ser transmitido**. NCM, CFOP, CST e valores são somente dados de teste, não recomendações para venda de café.

O teste carrega uma cópia temporária do pacote recebido, marca-a como revisada somente para compilar o XSD e verifica que o exemplo passa na validação estrutural e que uma versão adulterada é rejeitada. Isso não verifica assinatura criptográfica, chave de acesso, regras de negócio, tributação, credenciamento, vigência ou aceitação da SEFAZ. O pacote da aplicação continua desativado (`reviewed: false`) porque sua autenticidade não foi verificada de forma independente. A geração de NF-e pelo SiSFarm e a transmissão permanecem indisponíveis.

## Estrutura parcial derivada do rascunho

O arquivo `estrutura-incompleta-nao-transmitir.xml` mapeia apenas dados já salvos do produtor, destinatário, descrição, NCM, CFOP, valores comerciais de cada lote, unidades e quantidades fiscais informadas manualmente e modalidade de frete. O identificador do ambiente é fixado em homologação (`tpAmb=2`), sem efetuar comunicação. Ele omite deliberadamente `Id`, assinatura e grupos obrigatórios ainda não implementados; é incompleto e os testes confirmam sua rejeição pelo XSD recebido. Não representa NF-e nem deve ser transmitido. As unidades, quantidades e valores unitários devem ser informados explicitamente; o sistema não os infere das sacas. A conferência por multiplicação decimal e arredondamento a duas casas sinaliza divergências, mas não determina base de cálculo, impostos ou adequação fiscal. Dados do formulário ainda não salvos não entram no arquivo.

Próximos componentes: snapshots fiscais imutáveis, unidade e valores unitários com precisão verificada, identificação e numeração fictícias isoladas de produção, tributação e totais por operação, versão oficial confirmada, montagem completa e validação XSD; depois certificado e homologação controlada. O uso do schema recebido para testes não constitui confirmação independente da procedência.


### Valores tributários de teste

O rascunho permite registrar valores fictícios de ICMS, IBS e CBS por lote. Os totais exibidos são apenas somas dos valores salvos e aparecem somente quando todos os lotes possuem o campo preenchido; zero explícito não equivale a ausência de informação. Esses campos não calculam tributos, não validam a operação e ainda não alimentam o XML candidato. A definição tributária e o mapeamento exigem revisão técnica e fiscal antes de qualquer emissão.


## Mapeamento técnico para revisão

A simulação interna agora inclui os campos de unidade, quantidade, valor unitário e valores tributários salvos por lote, inclusive zero explícito. Um elemento vazio indica ausência de informação; nenhum imposto ou total é calculado na exportação. Essa simulação mantém o formato próprio `preparacao_sisfarm` e não é o XML de uma NF-e.

A inspeção preliminar do pacote recebido indica os seguintes destinos possíveis para valores por item. São caminhos condicionais para orientar o desenvolvimento, não uma determinação de enquadramento ou autorização para gerar o grupo tributário:

| Dado do rascunho | Possível destino no XML NF-e | Condição para implementação |
| --- | --- | --- |
| Base e valor ICMS | `det/imposto/ICMS/ICMSxx/vBC` e `vICMS` | Grupo e campos dependem do CST, regime e operação; alguns grupos não possuem esses campos. |
| Base IBS/CBS | `det/imposto/IBSCBS/gIBSCBS/vBC` | Presença depende do CST, classificação e regras vigentes. |
| IBS estadual | `det/imposto/IBSCBS/gIBSCBS/gIBSUF/vIBSUF` | Exige grupo aplicável, alíquota e demais informações exigidas. |
| IBS municipal | `det/imposto/IBSCBS/gIBSCBS/gIBSMun/vIBSMun` | Exige grupo aplicável, alíquota e demais informações exigidas. |
| CBS | `det/imposto/IBSCBS/gIBSCBS/gCBS/vCBS` | Exige grupo aplicável, alíquota e demais informações exigidas. |

Esses caminhos foram inspecionados no pacote recebido ainda não autenticado; não habilitam o validador da aplicação. A [NT 2025.002-RTC do Portal Nacional](https://www.nfe.fazenda.gov.br/portal/exibirArquivo.aspx?AspxAutoDetectCookieSupport=1&conteudo=xxtjiU7xjnk%3D) descreve regras adicionais, inclusive condições para o valor do IBS. O pacote, as tabelas de CST e classificação e a versão vigente precisam ser conferidos antes de implementar os grupos. A soma simples dos valores de teste não constitui o cálculo de `vIBS`, que pode depender de outras parcelas. O XML candidato permanece sem tributação, chave e assinatura.


### Alíquotas fictícias para conferência

Cada lote aceita percentuais fictícios de ICMS, IBS estadual, IBS municipal e CBS. O backend aceita valores entre 0 e 100, com até quatro casas decimais, e distingue ausência de zero informado. Isso valida somente o formato de entrada: não determina a alíquota legal ou aplicabilidade do grupo e não verifica as regras e a precisão exigidas para cada campo na versão do schema. Os dados salvos constam da simulação interna e não são incluídos no XML candidato. Não há cálculo, assinatura ou transmissão.


### Conferência dos dados salvos

A tela apresenta, por lote, um inventário dos campos de teste preenchidos e ausentes nos grupos ICMS e IBS/CBS. Zero explícito conta como informado. A lista não determina quais campos são obrigatórios, nem indica que os dados estejam corretos ou que o XML esteja pronto: essas decisões dependem do enquadramento, da operação e das regras fiscais aplicáveis. A conferência usa exclusivamente os dados já salvos.


## Identificação, numeração fictícia e chave simulada

Foram implementados os componentes locais de identificação e numeração para testes (`app/services/nfe_access_key.py`, `app/services/nfe_numbering.py` e modelos `NfeTestSequence` / `NfeDraftIdentification`):

- **Ambiente isolado (`teste_ficticio`):** o contador sequencial (`nfe_test_sequences`) e o vínculo com o rascunho (`nfe_draft_identifications`) operam exclusivamente em ambiente fictício, separados por organização, fazenda, modelo (`55`) e série. Essa numeração nunca é usada em produção nem reservada na SEFAZ, e não substitui a coordenação de séries com outros emissores do produtor.
- **Faixa de séries para produtor rural PF:** segue a faixa `920`–`969` (referência NT 2018.001). O algoritmo de chave já aceita `document_type='cnpj'` (séries `0`–`889`) e todas as 27 UFs do IBGE para expansão futura, mantendo o fluxo da interface restrito à Bahia e a CPF nesta etapa.
- **Chave de acesso simulada (44 dígitos):** composta com `cUF`, `AAMM`, documento com 14 posições (`000` + CPF para pessoa física), `mod`, `serie`, `nNF`, `tpEmis`, `cNF` aleatório (evitando padrões proibidos pela NT 2019.001 e igualdade com `nNF`) e dígito verificador `cDV` calculado pelo Módulo 11.
- **Limites preservados:** a chave simulada aparece apenas na tela de revisão e na simulação interna (`preparacao_sisfarm/identificacao_teste`). O arquivo `estrutura-incompleta-nao-transmitir.xml` continua deliberadamente sem `Id`, assinatura ou tributação e segue rejeitado pelo XSD. Nenhuma emissão real ou transmissão à SEFAZ foi habilitada.
