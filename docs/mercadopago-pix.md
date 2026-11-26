# Assinaturas via Pix (Mercado Pago · API de Orders)

O SiSFarm utiliza a aplicação independente **SiSFarm Pix** (Checkout Transparente com a [API de Orders](https://www.mercadopago.com.br/developers/pt/docs/checkout-api-orders/payment-integration/websites/pix)) para gerar cobranças Pix de contratação, renovação e alteração de plano dentro da própria plataforma. Esta integração é totalmente independente das assinaturas recorrentes por cartão de crédito (`/preapproval`).

## Dados da aplicação

- **Application ID de produção:** `7441327382083069`
- **Seller ID esperado:** `82986976`
- **Webhook de produção:** `https://app.sisfarm.com.br/api/webhooks/mercadopago/pix`
- **Webhook de teste:** `https://app.sisfarm.com.br/api/webhooks/mercadopago/pix/test`
- **Evento configurado:** `Order` (Mercado Pago)

## Variáveis de ambiente

```dotenv
# Mantém o Pix real desabilitado até concluir a homologação
MERCADOPAGO_PIX_ENABLED=false
MERCADOPAGO_PIX_ACCESS_TOKEN=
MERCADOPAGO_PIX_PUBLIC_KEY=
MERCADOPAGO_PIX_WEBHOOK_SECRET=

# Ambiente de testes (isolado da produção)
MERCADOPAGO_PIX_TEST_ACCESS_TOKEN=
MERCADOPAGO_PIX_TEST_PUBLIC_KEY=
MERCADOPAGO_PIX_TEST_WEBHOOK_SECRET=
```

## Homologação (antes de liberar produção)

1. Mantenha `MERCADOPAGO_PIX_ENABLED=false`.
2. Configure `MERCADOPAGO_SANDBOX=true` com `MERCADOPAGO_PIX_TEST_ACCESS_TOKEN`, `MERCADOPAGO_PIX_TEST_PUBLIC_KEY` e `MERCADOPAGO_PIX_TEST_WEBHOOK_SECRET`.
3. Cadastre `https://app.sisfarm.com.br/api/webhooks/mercadopago/pix/test` no painel de testes para o evento **Order**.
4. Teste contratação, QR Code e copia e cola, pagamento confirmado, expiração, renovação, alteração de plano e estorno.

## Liberação em produção

1. Confirme a chave Pix ativa na conta recebedora (`82986976`) e a URL de webhook de produção `https://app.sisfarm.com.br/api/webhooks/mercadopago/pix` para o evento **Order**.
2. Preencha `MERCADOPAGO_PIX_ACCESS_TOKEN`, `MERCADOPAGO_PIX_PUBLIC_KEY` e `MERCADOPAGO_PIX_WEBHOOK_SECRET` (exclusivos da aplicação de produção, diferentes dos de teste).
3. Ative `MERCADOPAGO_PIX_ENABLED=true` apenas quando concluir os testes e desejar liberar o Pix real aos clientes.

## Fluxo operacional

- **Validade do QR Code:** 24 horas (`PT24H`). Se expirar, o cliente gera um novo Pix em **Minha assinatura** sem duplicidade.
- **Lembretes automáticos:** Como o Pix não possui débito automático, o SiSFarm envia lembretes por e-mail aos 7 dias, 3 dias e no dia do vencimento do ciclo pago.
- **Conciliação e estorno:** O webhook valida `x-signature`, enfileira o evento e consulta `GET /v1/orders/{id}` antes de liberar o acesso. A conciliação periódica acompanha cobranças pendentes e estornos feitos diretamente no Mercado Pago.
