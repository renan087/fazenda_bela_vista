# NF-e própria — etapa 1: diagnóstico e especificação inicial

Data do levantamento: 21/11/2026. Situação: **proposta técnica; decisões fiscais e operacionais pendentes de validação**. Este documento não autoriza emissão em produção nem substitui parecer contábil ou confirmação da SEFAZ-BA.

## Escopo inicial e limites

Primeiro cenário: NF-e modelo 55 de saída, venda de café por produtor rural pessoa física estabelecido na Bahia para comprador pessoa física na Bahia. A integração será direta com a SEFAZ, sem API fiscal intermediária. O piloto começa em homologação; outros cenários (PF/PJ, interestadual, outras UFs, devolução e emissão por CNPJ) são modelados para expansão, mas não habilitados sem especificação e testes próprios. Confirmar com o contador se NF-e é o documento adequado para a operação real e as condições do destinatário, da entrega e do transporte.

## Diagnóstico do repositório

| Área | O que existe | Implicação para NF-e |
| --- | --- | --- |
| Fazenda | `app/models/farm.py` registra organização, nome, localização textual, CAR, CCIR e outros dados da propriedade. | Não há, nesse modelo, cadastro fiscal estruturado do emitente com CPF, inscrição estadual e endereço fiscal; fazenda e estabelecimento fiscal não devem ser tratados como equivalentes automaticamente. |
| Comprador | `app/models/coffee_buyer.py` já armazena tipo PF/PJ, documento, IE, indicação de isento/produtor rural, endereço e contato. `app/services/coffee_buyers.py` normaliza e valida documentos e apresenta pendências. | Reaproveitar cadastro sem assumir que está completo: documento e endereço são opcionais e faltam, entre outros, código IBGE do município e tratamento fiscal validado para a operação. `ie_exempt` não substitui uma definição inequívoca do indicador de IE; validar os três casos aplicáveis. |
| CNPJ | O documento tem `String(14)`, mas `digits()` remove letras e `valid_cnpj()` só aceita dígitos. | Antes de habilitar compradores ou emitentes CNPJ, implementar identificação alfanumérica de acordo com os schemas e notas técnicas vigentes, sem corromper dados legados. |
| Comercialização | `CoffeeCommercializationRecord` contém fazenda, lote, comprador opcional, data, quantidades, preço, valores, descontos, frete, status e `sale_code`. Uma venda pode ter várias linhas, agrupadas por `farm_id` e `sale_code` em `app/services/coffee_sales.py`. | A nota poderá partir de dados comerciais, mas não há cabeçalho de venda persistente e imutável. `sale_code` é gerado por fazenda por leitura do maior código, sem garantia de unicidade concorrente no banco; não usá-lo sozinho como identificador fiscal nem chave de idempotência. Registros legados podem ter somente nome do comprador. |
| Alteração e exclusão | `save_sale()` exclui e recria as linhas na edição; há rotas de exclusão de comercialização e exclusão de dados operacionais da organização. | Criar identidade persistente para operação fiscal e preservar snapshots/documentos independentemente de edições ou exclusões da venda, comprador, fazenda ou organização. Definir bloqueios, retenção e política de exclusão com assessoria jurídica e contábil. Não depender de FK com exclusão em cascata para documentos fiscais. |
| Quantidades e valores | A venda trabalha com sacas e equivalência; `compute_values()` e `parse_decimal()` usam `float`/`round`, além de descontos comerciais de frete, Funrural e corretagem. | Calcular valores fiscais com `Decimal`, regras explícitas de precisão e arredondamento e conciliação de totais. Não presumir que descontos comerciais ou percentual informado correspondem à base de cálculo, tributo, retenção ou valor da NF-e. |
| Emissão | O levantamento dos modelos e serviços examinados não identificou um emissor NF-e, armazenamento de XML autorizado ou integração SEFAZ. | Construir os componentes fiscais, com estados e auditoria separados dos status comerciais. |

## Arquitetura proposta

1. **Cadastro fiscal do emitente:** entidade de pessoa e estabelecimento, com vínculos explícitos a organização e fazenda; dados de credenciamento, UF, ambiente e status de verificação. Não reaproveitar dados de cobrança da assinatura como identidade fiscal do produtor.
2. **Destinatário e produto fiscal:** extensão segura dos dados existentes de comprador, código do município e identificação fiscal do produto, com validações por operação. Preservar compatibilidade dos registros existentes.
3. **Rascunho e snapshot:** entidade fiscal independente, com dados do emitente, destinatário, itens, operação, valores e documentos versionados. Capturar a origem da venda e seus lotes; não permitir que edição posterior altere uma NF-e transmitida.
4. **Regras fiscais:** perfis versionados por vigência, UF e tipo de operação, com origem e responsável pela aprovação. Não aplicar alíquotas, NCM, CFOP, CST ou cClassTrib genéricos sem validação; IA não decide tributação.
5. **Núcleo NF-e:** XML, XSD, chave de acesso, assinatura, série, número, protocolo e estado independente da venda. Aplicar a regra vigente para emitente CPF e coordenar a numeração com outros sistemas utilizados pelo mesmo emitente.
6. **Conector SEFAZ:** serviços de autorização, consulta, status e eventos por UF/ambiente, com timeouts, persistência de tentativas e reconciliação do resultado antes de repetir uma transmissão. Bahia primeiro; não presumir que os demais estados compartilhem o mesmo serviço.
7. **Segurança e operação:** controle de autorização por organização e emitente; permissões separadas para certificado, configuração, preparação, transmissão e cancelamento; criptografia de certificados, segredos fora do banco, rotação, backups testados, retenção e logs sem chave privada ou documentos completos. Infraestrutura e método de assinatura serão definidos antes do armazenamento de certificados.
8. **Documentos e eventos:** XML assinado/autorizado, protocolo, eventos, DANFE e histórico íntegro e exportável. Não fazer promessas de contingência automática até validação da modalidade e do fluxo autorizado pela SEFAZ.

## Pontos oficiais que orientam o projeto

- A SEFAZ-BA descreve o cadastro de pessoa física não equiparada no [cadastro de contribuintes](https://www.sefaz.ba.gov.br/inspetoria-eletronica/icms/cadastro/sobre-o-cadastro/). Confirmar os procedimentos atuais e o credenciamento específico de NF-e para o produtor piloto; credenciamento ao DT-e não deve ser confundido com autorização de emissão de NF-e.
- A [NT 2018.001 v1.10](https://www.nfe.fazenda.gov.br/Portal/exibirArquivo.aspx?AspxAutoDetectCookieSupport=1&conteudo=JlPBgTc3TQo%3D) aborda emitente pessoa física, série e numeração. Verificar alterações e regras vigentes antes de implementar e homologar; coordenar a numeração com eventuais emissores externos.
- As URLs e serviços da Bahia para [homologação](https://hom.nfe.fazenda.gov.br/Portal/webServices.aspx?AspxAutoDetectCookieSupport=1&tipoConteudo=OUC%2FYVNWZfo%3D) e [produção](https://www.nfe.fazenda.gov.br/portal/WebServices.aspx/consultaEPECConciliacao.aspx?AspxAutoDetectCookieSupport=1) são mantidos pelo Portal Nacional. Conferir disponibilidade, WSDL e versões na execução da etapa técnica.
- A [relação de notas técnicas](https://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?AspxAutoDetectCookieSupport=&AspxAutoDetectCookieSupport=1&tipoConteudo=04BIflQt1aY%3D) inclui mudanças de IBS/CBS e CNPJ alfanumérico. O [informe de 13/11/2026](https://www.nfe.fazenda.gov.br/portal/informe.aspx?AspxAutoDetectCookieSupport=1&Informe=nIXsNKJXnBY&ehCTG=false) registra atualizações de cronograma e NT 2023.002 v1.02 sobre autorização assíncrona e inutilização: verificar implantação e efeitos antes de escolher estados, interfaces e testes. Não implementar com base em versões antigas presumidas.

## Decisões pendentes antes da etapa 2

- Contador/consultor fiscal: descrição precisa do café e de sua apresentação; tributação e classificação; informações obrigatórias; regime e tratamento do produtor e do comprador; pagamento, frete, entrega e documentação de transporte. Validar vendas presenciais e não presenciais e confirmar modelo 55.
- Produtor piloto: confirmar cadastro, IE, credenciamento para homologação e produção, certificado admitido, relação entre fazendas e emitente e uso de outro sistema de emissão. Não solicitar nem receber certificado ou senha nesta etapa.
- Equipe SiSFarm: política para cancelamento/edição/exclusão de venda e organização, retenção de documentos, gestão de certificado e autorização por usuário; arquitetura de criptografia e backup; limites do ambiente Render atual e necessidade de processamento durável.
- SEFAZ-BA: confirmar procedimentos atuais, endpoints, certificado, ambiente, exigências de credenciamento e contingência. Documentar fonte, versão e responsável por cada confirmação.

## Critérios de saída da etapa 1 e próxima etapa

Este levantamento entrega o inventário e uma proposta para revisão. A etapa 1 só estará fiscalmente validada após as confirmações acima, inclusive definição dos valores fiscais por profissional habilitado. Não houve emissão, credenciamento, acesso a certificados ou alteração em dados existentes.

A etapa 2 poderá desenvolver cadastros e rascunho em escopo seguro, com campos extensíveis, testes de isolamento e compatibilidade com dados legados. Enquanto as decisões fiscais estiverem pendentes, não habilitar transmissão nem preencher regras fiscais como se estivessem aprovadas.
