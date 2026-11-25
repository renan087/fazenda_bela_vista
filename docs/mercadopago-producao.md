# Assinaturas do Mercado Pago em produção

O SiSFarm cria assinaturas mensais e anuais sem plano associado na API do Mercado Pago. O catálogo e os valores ficam no SiSFarm. A autorização da recorrência ocorre no checkout hospedado. O retorno do navegador mostra a situação registrada pelo SiSFarm, que confirma a assinatura e o pagamento consultando a API. [Assinaturas com pagamento pendente](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/integration-configuration/subscription-no-associated-plan/pending-payments)

## Preparação

1. Confirme no Render se o serviço fica ativo continuamente e se o PostgreSQL possui armazenamento durável. O Blueprint do repositório declara planos gratuitos. O serviço gratuito adormece e o Postgres gratuito expira. Teste a restauração de um backup em um banco isolado e registre quem acompanha os alertas. [Limites dos planos gratuitos do Render](https://render.com/docs/free)
2. Confira o catálogo, os preços mensais e anuais, os limites, o teste grátis e os contatos de suporte. Valide o envio de avisos pelo canal de e-mail configurado e confira as organizações que precisam de acesso durante a transição. O histórico de teste fica separado das assinaturas reais. Defina com a equipe responsável como atender solicitações de reembolso e como emitir os documentos fiscais do serviço.
3. Selecione a aplicação de Assinaturas na conta brasileira que receberá os valores. Ative as credenciais de produção no painel do Mercado Pago e obtenha o ID dessa conta. Configure os valores abaixo no serviço, com o checkout público fechado. Use o token da conta real e o segredo gerado para o webhook dessa aplicação.

```dotenv
APP_PUBLIC_URL=https://app.sisfarm.com.br
ASAAS_LIVE_ENABLED=false
MERCADOPAGO_SANDBOX=false
MERCADOPAGO_LIVE_ENABLED=true
MERCADOPAGO_LIVE_CHECKOUT_ENABLED=false
# Configure o ID numérico da conta real em MERCADOPAGO_LIVE_SELLER_ID.
MERCADOPAGO_LIVE_ACCESS_TOKEN=
MERCADOPAGO_LIVE_WEBHOOK_SECRET=
```

A inicialização cria as colunas de ambiente das notificações, data de encerramento da assinatura e valor estornado. A integração confere a identidade da conta em `GET /users/me` antes de chamar a API de assinaturas. Para verificar a credencial e as permissões sem iniciar uma cobrança, execute no serviço com as variáveis já configuradas:

```bash
python -c 'from app.services.mercadopago_subscriptions import MercadoPagoAPI; api = MercadoPagoAPI(); api.request("GET", "/preapproval/search", params={"limit": 1}); print("Consulta de assinaturas do Mercado Pago concluída")'
```

## Webhook de produção

Cadastre `https://app.sisfarm.com.br/api/webhooks/mercadopago` no painel da aplicação de produção. Selecione os tópicos de pagamentos, assinaturas, pagamentos recorrentes e planos. A integração valida `x-signature` e `x-request-id`, responde após persistir o evento e consulta o recurso pelo token de produção antes de aplicar o estado local. O catálogo permanece sob controle do SiSFarm. [Configurar Webhooks](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/additional-content/your-integrations/notifications/webhooks)

Use a simulação do painel para verificar a entrega HTTP e observe o registro com ambiente `mercadopago_live` em `mercadopago_events`. Uma simulação que referencia um recurso fictício pode ser recebida e falhar na consulta à API. Confirme o processamento completo com eventos da primeira assinatura real autorizada. Acompanhe tentativas e erros da fila, referências de requisição da API e pendências em **Assinaturas**. A conciliação periódica consulta assinaturas atuais a partir de cinco minutos da última sincronização. Para as encerradas, há uma nova consulta após o cancelamento e depois a cada seis horas durante 180 dias. O superadministrador também pode sincronizar o histórico pela tela de **Assinaturas**.

## Primeira assinatura e operação

Com a infraestrutura e a entrega do webhook verificadas, habilite `MERCADOPAGO_LIVE_CHECKOUT_ENABLED=true` quando houver uma primeira contratação autorizada. Confira o valor em BRL, o ciclo, o primeiro vencimento, a identificação do vendedor e o retorno a **Minha assinatura**. Compare a autorização, a fatura, o pagamento aprovado e a liberação de acesso. Confira quais meios de recorrência a aplicação oferece antes de divulgar Pix ou boleto.

| Situação | Jornada |
| --- | --- |
| Autorização e renovação | O Mercado Pago processa as cobranças. A liberação por assinatura depende do pagamento confirmado. O SiSFarm aplica a tolerância configurada pela data de renovação mesmo se a nova fatura demorar a aparecer. |
| Atraso | O Mercado Pago conduz as novas tentativas. O SiSFarm sinaliza a pendência, envia os avisos aplicáveis quando a fatura está identificada e entra em modo somente leitura após a tolerância. |
| Pausa e reativação | A equipe opera em **Assinaturas**. O SiSFarm envia a mudança ao Mercado Pago e consulta o resultado. Uma pausa suspende o acesso pela assinatura. A reativação volta a depender das condições financeiras. |
| Cancelamento | O responsável financeiro pode cancelar em **Minha assinatura**. A equipe também pode cancelar em **Assinaturas**. O SiSFarm pede ao Mercado Pago o encerramento da recorrência e mantém o acesso até o fim do ciclo pago, sujeito a estorno ou contestação. Alterações feitas no Mercado Pago aparecem por webhook ou conciliação. |
| Estorno e contestação | O atendimento opera no painel do Mercado Pago. O histórico do SiSFarm reflete estornos totais e parciais e contestações. Um estorno total ou contestação do último pagamento pode restringir o acesso. |
| Cartão, plano ou ciclo | A troca do meio de pagamento fica no Mercado Pago. Mudanças de plano ou ciclo passam pelo atendimento SiSFarm. Uma nova contratação fica disponível quando termina o período já pago. |

[Retentativas de pagamento](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/integration-configuration/subscription-no-associated-plan/authorized-payments) · [Cancelamentos e estornos](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/additional-content/cancellations-and-refunds)

Para bloquear novos checkouts, volte `MERCADOPAGO_LIVE_CHECKOUT_ENABLED=false` e mantenha `MERCADOPAGO_LIVE_ENABLED=true`. As assinaturas existentes continuam sendo conciliadas e podem ser canceladas. Links emitidos anteriormente e cobranças recorrentes continuam sujeitos à situação de cada assinatura no Mercado Pago. Para invalidar um link emitido, cancele a assinatura pendente correspondente. Em caso de criação inconclusiva, confira a referência da tentativa e o ID da requisição antes de intervir. O SiSFarm mantém a reserva local e tenta localizar a assinatura sem repetir o pedido de criação.
