# API de Chamadas

Documentação do endpoint para gerenciar chamadas WhatsApp.

## 📋 Índice

- [Rejeitar Chamada](#rejeitar-chamada)
- [Iniciar Chamada de Voz](#iniciar-chamada-de-voz)

---

## Rejeitar Chamada

Rejeita uma chamada recebida no WhatsApp.

**Endpoint**: `POST /call/reject`

**Headers**:
```
Content-Type: application/json
apikey: SUA-CHAVE-API
```

**Body**:
```json
{
  "callCreator": "5511999999999@s.whatsapp.net",
  "callId": "ABC123XYZ"
}
```

**Parâmetros**:

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `callCreator` | string (JID) | ✅ Sim | JID de quem está ligando |
| `callId` | string | ✅ Sim | ID da chamada |

**Nota**: Os dados da chamada (`callCreator` e `callId`) são recebidos via webhook quando uma chamada chega. Você deve capturar esses dados do evento `call` para usar este endpoint.

**Resposta de Sucesso (200)**:
```json
{
  "message": "success"
}
```

**Resposta de Erro (400)**:
```json
{
  "error": "invalid request body"
}
```

**Resposta de Erro (500)**:
```json
{
  "error": "instance not found"
}
```

**Exemplo cURL**:
```bash
curl -X POST http://localhost:4000/call/reject \
  -H "Content-Type: application/json" \
  -H "apikey: SUA-CHAVE-API" \
  -d '{
    "callCreator": "5511999999999@s.whatsapp.net",
    "callId": "ABC123XYZ"
  }'
```

---

## Iniciar Chamada de Voz

Inicia uma chamada de voz experimental para um contato individual. Quando `audioUrl` é informado, o áudio é reproduzido depois que a chamada é atendida e a chamada é encerrada ao final da reprodução.

**Endpoint**: `POST /call/ring`

**Headers**:
```text
Content-Type: application/json
apikey: SUA-CHAVE-API
```

**Body**:
```json
{
  "number": "5511999999999@s.whatsapp.net",
  "durationSeconds": 15,
  "audioUrl": "https://cdn.exemplo.com/audios/saudacao.mp3"
}
```

**Parâmetros**:

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `number` | string (JID) | ✅ Sim | JID individual `@s.whatsapp.net` ou `@lid` |
| `durationSeconds` | inteiro | Não | Prazo para a chamada ser atendida, de 1 a 60 segundos; padrão: 10 |
| `audioUrl` | string (URL) | Não | URL HTTP(S) do áudio; formatos: MP3, WAV, OGG ou Opus |

O arquivo de áudio pode ter no máximo 20 MiB. A URL precisa terminar com uma extensão suportada, mesmo quando contém query string. O host e todos os redirecionamentos devem resolver exclusivamente para endereços IP públicos; loopback, redes privadas, link-local e endpoints de metadata são bloqueados.

**Comportamento**:

1. Se ninguém atender dentro de `durationSeconds`, a chamada é encerrada.
2. Sem `audioUrl`, a chamada é encerrada assim que o contato atende.
3. Com `audioUrl`, o prazo de toque é cancelado quando o contato atende, o áudio é reproduzido e a chamada é encerrada ao final.

**Resposta de Sucesso (200)**:
```json
{
  "message": "success"
}
```

**Exemplo cURL**:
```bash
curl -X POST http://localhost:4000/call/ring \
  -H "Content-Type: application/json" \
  -H "apikey: SUA-CHAVE-API" \
  -d '{
    "number": "5511999999999@s.whatsapp.net",
    "durationSeconds": 15,
    "audioUrl": "https://cdn.exemplo.com/audios/saudacao.mp3"
  }'
```

> **Atenção**: chamadas para contatos desconhecidos podem acionar proteções antiabuso do WhatsApp e colocar a conta em risco. Não use este endpoint em campanhas ou disparos automatizados.

---

## Fluxo Completo de Rejeição Automática

### 1. Receber Evento de Chamada via Webhook

Quando alguém liga para sua instância, você recebe um webhook:

```json
{
  "event": "call",
  "instance": "minha-instancia",
  "data": {
    "id": "ABC123XYZ",
    "from": "5511999999999@s.whatsapp.net",
    "timestamp": "2025-11-11T10:30:00Z",
    "isVideo": false,
    "isGroup": false
  }
}
```

### 2. Rejeitar Automaticamente

No seu servidor que recebe webhooks, quando chegar um evento de chamada:
1. Capture o `id` e o `from` do evento
2. Faça uma requisição POST para `/call/reject`
3. Use os dados capturados como `callId` e `callCreator`

Dessa forma, chamadas são rejeitadas automaticamente assim que chegam.

### 3. Rejeição Seletiva

Para rejeitar apenas chamadas de números não autorizados:
1. Mantenha uma lista de números permitidos
2. Ao receber evento de chamada, verifique se o número está na lista
3. Se não estiver, rejeite a chamada usando o endpoint `/call/reject`
4. Se estiver autorizado, não faça nada (deixe tocar)

---

## Casos de Uso

### 1. Rejeitar Todas as Chamadas

Útil para contas de atendimento que só respondem via mensagens:
1. Configure seu webhook para receber eventos de chamada
2. Quando receber evento `call`, rejeite imediatamente
3. Envie uma mensagem de texto explicando que não atende chamadas

### 2. Horário Comercial

Rejeitar chamadas fora do horário de trabalho:
1. Ao receber evento de chamada, verifique o horário atual
2. Se estiver fora do horário comercial (ex: Segunda a Sexta, 9h-18h), rejeite
3. Envie mensagem informando o horário de atendimento

### 3. Rejeitar Chamadas de Vídeo

Aceitar apenas chamadas de áudio:
1. Verifique o campo `isVideo` no evento de chamada
2. Se for `true`, rejeite a chamada
3. Envie mensagem pedindo para ligar com chamada de voz

---

## Limitações e Observações

### Limitações do WhatsApp

1. **Não é possível aceitar chamadas recebidas via API**: o endpoint experimental inicia chamadas de saída, mas não atende chamadas recebidas.

2. **Chamadas em grupos**: Chamadas em grupos também disparam o evento, mas o campo `isGroup` será `true`.

3. **Timing**: A rejeição deve ser feita rapidamente. Se demorar muito, a chamada pode cair antes da rejeição.

### Boas Práticas

1. **Sempre responda ao webhook rapidamente**: Rejeite a chamada em menos de 2 segundos para evitar timeout.

2. **Envie mensagem explicativa**: Após rejeitar, informe o usuário o motivo via mensagem de texto.

3. **Log de chamadas rejeitadas**: Mantenha registro para análise, salvando data/hora, número, ID da chamada e motivo da rejeição.

4. **Tratamento de erros**: Sempre trate possíveis erros na rejeição para evitar que seu webhook trave ao falhar em rejeitar uma chamada.

---

## Códigos de Erro Comuns

| Código | Erro | Solução |
|--------|------|---------|
| 400 | `invalid request body` | Verifique formato do JSON |
| 500 | `instance not found` | Instância não conectada |
| 500 | `error reject call` | Chamada não existe ou já expirou |

---

## Estrutura do Evento de Chamada

Quando você recebe um webhook de chamada, a estrutura é:

```json
{
  "event": "call",
  "instance": "minha-instancia",
  "data": {
    "id": "ABC123XYZ",
    "from": "5511999999999@s.whatsapp.net",
    "timestamp": "2025-11-11T10:30:00Z",
    "isVideo": false,
    "isGroup": false,
    "status": "ringing"
  }
}
```

**Campos**:
- `id`: ID único da chamada (use como `callId`)
- `from`: JID de quem está ligando (use como `callCreator`)
- `timestamp`: Quando a chamada foi iniciada
- `isVideo`: Se é chamada de vídeo (true) ou áudio (false)
- `isGroup`: Se é chamada em grupo
- `status`: Status da chamada (ringing, timeout, reject)

---

## Configuração de Webhooks

Para receber eventos de chamada, configure o webhook:

```env
WEBHOOK_URL=https://seu-servidor.com/webhook
```

Certifique-se de que seu servidor:
1. Aceita requisições POST
2. Responde rapidamente (< 5 segundos)
3. Retorna status 200 para confirmar recebimento

---

## Próximos Passos

- [Sistema de Eventos](../recursos-avancados/events-system.md) - Configurar webhooks
- [API de Mensagens](./api-messages.md) - Enviar mensagem após rejeitar
- [API de Usuários](./api-user.md) - Gerenciar contatos
- [Visão Geral da API](./api-overview.md)

---

**Documentação gerada para Evolution GO v1.0**
