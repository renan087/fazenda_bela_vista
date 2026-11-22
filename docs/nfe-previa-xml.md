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
