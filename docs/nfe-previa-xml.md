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
