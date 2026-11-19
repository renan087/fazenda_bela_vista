# Entrada em produção das assinaturas

## 1. Homologação

Mantenha `ASAAS_ENVIRONMENT=sandbox` e `ASAAS_LIVE_ENABLED=false`. Use duas organizações com dados de teste e valide:

- Um usuário autenticado só vê suas próprias fazendas na API. Um perfil sem permissão financeira não vê os indicadores financeiros no painel da API.
- Pix, boleto e cartão mostram plano, preço, ciclo e vencimento corretos. O aceite fica na auditoria comercial.
- No cenário de produção simulado, sem cobrança confirmada e sem teste ou liberação ativa, a escrita fica restrita e a regularização continua acessível. Confira também vencimento, estorno e contestação.
- Um duplo envio e a troca de meio de pagamento durante uma tentativa não criam duas contratações. Reenvie o mesmo webhook e simule a interrupção da conexão após o Asaas aceitar uma solicitação.
- O atendimento consegue rastrear criação, conciliação, cancelamento e autorização para retorno ao cartão salvo. Confirme o acesso por código e o prazo de 30 minutos para alterações administrativas.

## 2. Preparação do piloto

- Restaure um backup em um banco isolado e valide os dados antes de alterar o ambiente. Execute o script de encerramento de sandbox inicialmente sem `--confirmar` para revisar o efeito local. Checkouts de teste não devem ser reaproveitados em produção.
- Liste os clientes atuais que ainda precisam de acesso. Use liberações manuais com prazo e justificativa para casos de transição acordados.
- Configure a URL pública HTTPS e o e-mail de suporte. Configure uma chave exclusiva da API Asaas de produção, um token exclusivo de webhook com pelo menos 32 caracteres e o endpoint `/api/webhooks/asaas`. Guarde segredos no painel do serviço.
- Confirme quem está autorizado a operar a conta administrativa. Use contas individuais nos sistemas de atendimento, credenciais separadas por ambiente e mantenha a evidência de autorização do cliente vinculada ao atendimento.
- Prepare alertas no painel do Asaas para interrupção da fila e no provedor de hospedagem para erros de webhook e mensagens de conciliação com pendências. A equipe deve comparar o registro local com o Asaas antes de refazer uma operação. Se a criação de checkout não devolver um identificador, a tentativa permanece bloqueada. Use o atendimento Asaas e a referência da contratação para recuperar ou encerrar a operação com evidência antes de liberar uma nova tentativa.

## 3. Piloto e contenção

Com a homologação concluída, configure `ASAAS_ENVIRONMENT=production`, `ASAAS_API_BASE_URL=https://api.asaas.com`, `ASAAS_PILOT_ORGANIZATION_IDS` com o ID da organização piloto e `ASAAS_LIVE_ENABLED=true`. O piloto controla novas contratações. Convide apenas a organização autorizada e confira a primeira cobrança e a liberação do acesso nos dois sistemas.

Para interromper novas adesões preservando a conciliação, esvazie `ASAAS_PILOT_ORGANIZATION_IDS`. Se for necessário desativar as chamadas do módulo ao Asaas, use `ASAAS_LIVE_ENABLED=false`. Verifique no Asaas as operações já enviadas, incluindo cobranças recorrentes ativas, e trate cada caso antes de reabrir o piloto.
