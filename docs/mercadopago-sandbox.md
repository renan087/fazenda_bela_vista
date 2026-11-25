# Assinaturas do Mercado Pago em ambiente de teste

Abra [Integrações do Mercado Pago](https://www.mercadopago.com.br/developers/panel/app) e crie contas fictícias brasileiras de vendedor e comprador. O [guia específico de Assinaturas](https://www.mercadopago.com.br/developers/pt/news/2023/11/16/Questions-on-how-to-test-your-integration--) orienta criar a aplicação na conta fictícia do vendedor e usar o Access Token da aba **Produção** dessa aplicação para o fluxo de assinatura pendente. Use as credenciais dessa conta fictícia exclusivamente na homologação.

Configure as variáveis no ambiente de homologação do SiSFarm:

```dotenv
MERCADOPAGO_SANDBOX=true
MERCADOPAGO_PUBLIC_KEY=
MERCADOPAGO_ACCESS_TOKEN=
MERCADOPAGO_WEBHOOK_SECRET=
```

Em **Webhooks**, cadastre a URL de teste `https://<host-do-sisfarm>/api/webhooks/mercadopago/test` e use o segredo gerado em `MERCADOPAGO_WEBHOOK_SECRET`. Selecione os tópicos de planos e assinaturas e de pagamentos. A URL pública do SiSFarm, configurada em `APP_PUBLIC_URL`, é usada para retornar a **Minha assinatura** depois do checkout. A configuração usa o mesmo host público configurado para o SiSFarm.

1. Entre no SiSFarm como responsável financeiro de uma organização de homologação e contrate um plano mensal ou anual em **Planos disponíveis**. Use o e-mail de um comprador de teste do Mercado Pago.
2. Termine a autorização no checkout com o comprador e o cartão de teste do Mercado Pago. Confira o valor, a frequência, a próxima cobrança e os meios que o checkout oferece. Pix e boleto precisam ser verificados na conta de teste.
3. Em **Minha assinatura**, a autorização pode aparecer antes da primeira cobrança. O SiSFarm libera o acesso pela assinatura após a confirmação financeira. A tela administrativa **Assinaturas** permite sincronizar, pausar, reativar e cancelar a assinatura de teste.
4. Confirme os resultados no painel do Mercado Pago e compare as faturas com os registros em **Assinaturas**. Verifique também a cobrança seguinte e o encerramento da recorrência ao cancelar.

A assinatura é criada sem plano remoto associado, com os valores do catálogo local. Uma notificação `subscription_preapproval_plan` é consultada e registrada para diagnóstico. Os planos e os limites do SiSFarm são gerenciados pelo catálogo local.

Ativar `MERCADOPAGO_SANDBOX` direciona as novas contratações ao Mercado Pago e interrompe as operações do Asaas enquanto o modo de teste estiver ativo. Assinaturas legadas do sandbox do Asaas podem dar lugar a uma nova assinatura de teste para a mesma organização, com histórico preservado. Os registros de produção do Asaas bloqueiam a nova contratação de teste quando ainda estão marcados como atuais. Esta integração oferece somente operações do Mercado Pago em teste.

A fila de webhook registra tópico, ID do recurso, situação, tentativa e resultado em `mercadopago_events`. O log da aplicação identifica falhas de API e, quando enviado pelo Mercado Pago, o ID da requisição. Assinaturas com respostas incertas são buscadas novamente por referência e e-mail do comprador de teste. Verifique a referência no Mercado Pago se a criação continuar em revisão. O sistema conserva a reserva e evita reenviar a criação enquanto o resultado for inconclusivo.

Se o checkout recusar a contratação, consulte a tentativa em **Assinaturas → Organização**. `http_400` e `http_422` indicam que a API rejeitou os dados. `http_401` e `http_403` indicam que é preciso conferir o token e o acesso da aplicação. Quando a API informa uma incompatibilidade entre vendedor e comprador, confira de qual conta vieram o token e o e-mail do comprador. O Mercado Pago também publica [instruções específicas para testar Assinaturas com vendedor e comprador fictícios](https://www.mercadopago.com.br/developers/pt/news/2023/11/16/Questions-on-how-to-test-your-integration--).

Em caso de `http_403`, confirme qual conta é dona da aplicação e de qual aba veio o Access Token. Se a API continuar retornando `403`, informe ao suporte do Mercado Pago o endpoint `POST /preapproval`, o horário e a referência exibida no checkout.

O SiSFarm consulta `GET /users/me` com o token da integração. Quando o Mercado Pago confirma que a conta é um vendedor fictício brasileiro, o SiSFarm omite o header `X-scope: stage` e exige um e-mail de comprador do domínio `testuser.com`, diferente do e-mail do vendedor. A inclusão desse header na criação de uma assinatura pendente com o vendedor fictício causou `403 PA_UNAUTHORIZED_RESULT_FROM_POLICIES` na homologação.

[API de Assinaturas](https://www.mercadopago.com.br/developers/pt/reference/online-payments/subscriptions/overview) · [Teste de compra](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/integration-test/payment-approval) · [Webhooks](https://www.mercadopago.com.br/developers/pt/docs/subscriptions/additional-content/your-integrations/notifications/webhooks)
