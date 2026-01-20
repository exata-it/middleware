# Middleware de Sincronização - Fiscalize

Middleware para sincronização em tempo real entre o banco de dados origem (AGEFIS) e o novo sistema. Utiliza o mecanismo **LISTEN/NOTIFY** do PostgreSQL para capturar eventos de INSERT, UPDATE e DELETE em tempo real.

## 📖 Índice

- [Arquitetura](#-arquitetura)
- [Tecnologias](#-tecnologias)
- [Estrutura de Arquivos](#-estrutura-de-arquivos)
- [Como Funciona](#-como-funciona)
- [Pré-requisitos](#-pré-requisitos)
- [Configuração](#-configuração)
- [Executando](#-executando)
- [Scripts Disponíveis](#-scripts-disponíveis)
- [Mapeamento de Dados](#-mapeamento-de-dados)
- [Tratamento de Erros](#-tratamento-de-erros)

---

## 🏗️ Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           FLUXO DE SINCRONIZAÇÃO                                  │
└──────────────────────────────────────────────────────────────────────────────────┘

     BANCO ORIGEM                    MIDDLEWARE                      BANCO DESTINO 
    ┌─────────────────────┐              ┌─────────────────────┐            ┌─────────────────────┐
    │                     │              │                     │            │                     │
    │  public.demanda     │──────────────│     Listener        │───────────►│ fiscalizacao.       │
    │  public.fiscaldemanda│  NOTIFY     │  (postgres.js)      │  INSERT/   │   demandas          │
    │                     │              │                     │  UPDATE    │   demandas_fiscais  │
    │      TRIGGER        │              │     Handlers        │            │                     │
    │         ▼           │              │        │            │            └─────────────────────┘
    │  notificar_sync()   │              │        ▼            │                      ▲
    │         │           │              │    Mappers          │                      │
    │         ▼           │              │        │            │                      │
    │  pg_notify(         │              │        ▼            │                      │
    │   'sync_channel',   │              │  Bun SQL Driver     │──────────────────────┘
    │   payload           │              │                     │
    │  )                  │              │     CRON JOB        │
    │                     │              │  (Reconciliação)    │
    └─────────────────────┘              │  a cada 10 min      │
                                         └─────────────────────┘
```

### Fluxo Detalhado

1. **Trigger Dispara**: Quando há INSERT/UPDATE/DELETE nas tabelas `demanda` ou `fiscaldemanda`
2. **Notificação Enviada**: A função `notificar_sync()` envia payload JSON via `pg_notify`
3. **Listener Recebe**: O middleware escuta o canal `sync_channel`
4. **Busca Registro Completo**: O middleware consulta o registro completo na origem
5. **Mapeamento**: Os dados são mapeados para o schema do destino
6. **Sincronização**: INSERT/UPDATE/DELETE é executado no banco destino
7. **Reconciliação**: Cron job verifica gaps a cada 10 minutos

---

## 🚀 Tecnologias

| Tecnologia | Uso |
|------------|-----|
| **Bun** | Runtime JavaScript ultra-rápido |
| **postgres.js** | Driver PostgreSQL com suporte a LISTEN/NOTIFY |
| **Bun SQL** | Driver nativo do Bun para escrita no destino |
| **node-cron** | Agendamento de tarefas de reconciliação |
| **PostgreSQL LISTEN/NOTIFY** | Sincronização em tempo real |

---

## 📁 Estrutura de Arquivos

```
middleware/
├── package.json                    # Dependências e scripts
├── .env                            # Variáveis de ambiente (não commitado)
├── .env.example                    # Template de configuração
├── erros_sincronizacao.json        # Log de erros de constraint
├── sync-pessoas.js                 # Script para sincronizar pessoas ausentes
│
├── src/
│   ├── index.js                    # Ponto de entrada principal
│   │
│   ├── config/
│   │   ├── index.js                # Configurações centralizadas (URLs, cron, etc)
│   │   └── database.js             # Conexões com bancos origem/destino
│   │
│   ├── services/
│   │   ├── listener.js             # LISTEN/NOTIFY - escuta notificações
│   │   ├── cronManager.js          # Gerencia cron jobs de reconciliação
│   │   └── reconciliation.js       # Verifica e corrige gaps entre bancos
│   │
│   ├── handlers/
│   │   ├── index.js                # Registro central de handlers por tabela
│   │   ├── demandaHandler.js       # Sincroniza tabela demandas
│   │   └── fiscalDemandaHandler.js # Sincroniza tabela demandas_fiscais
│   │
│   ├── mappers/
│   │   ├── index.js                # Exporta todos os mappers
│   │   ├── demandaMapper.js        # Mapeia campos demanda → demandas
│   │   └── fiscalDemandaMapper.js  # Mapeia campos fiscaldemanda → demandas_fiscais
│   │
│   ├── scripts/
│   │   ├── syncFiscalDemanda.js    # Sincroniza fiscais de demandas
│   │   ├── syncFiscalizadoId.js    # Sincroniza IDs de fiscalizados
│   │   └── syncPessoasMissing.js   # Sincroniza pessoas ausentes (resolve FK errors)
│   │
│   ├── scripts/
│   │   └── syncFiscalDemanda.js    # Script manual de sync em lote
│   │
│   └── utils/
│       └── errorLogger.js          # Salva erros de constraint em JSON
│
└── sql-origem/
    ├── 01_funcao_notificar.sql     # Função trigger genérica
    ├── 02_trigger_demanda.sql      # Trigger na tabela demanda
    └── 02_trigger_fiscal_demanda.sql # Trigger na tabela fiscaldemanda
```

---

## ⚙️ Como Funciona

### 1. Conexões de Banco de Dados

O middleware mantém duas conexões separadas:

```javascript
// config/database.js
import postgres from "postgres";
import { SQL } from "bun";

// postgres.js - suporta LISTEN/NOTIFY (leitura + escuta)
export const dbOrigem = postgres(CONFIG.origem.url);

// Bun SQL - driver nativo para escrita
export const dbDestino = new SQL(CONFIG.destino.url);
```

### 2. Trigger no Banco de Origem

A função genérica `notificar_sync()` é chamada por triggers e envia:

```sql
-- Payload enviado via pg_notify
{
  "id": 12345,
  "table": "public.demanda",
  "event_type": "INSERT" -- ou UPDATE, DELETE
}
```

### 3. Listener de Notificações

```javascript
// services/listener.js
await dbOrigem.listen(CONFIG.canal, async (payload) => {
    const { id, table, event_type } = JSON.parse(payload);
    
    // Busca handler para a tabela
    const handler = HANDLERS[table];
    
    // Busca registro completo na origem
    const registro = await buscarRegistroOrigem(table, id);
    
    // Executa sincronização
    await handler(event_type, registro);
});
```

### 4. Handlers por Tabela

Cada tabela tem seu handler específico:

```javascript
// handlers/index.js
export const HANDLERS = {
    "public.demanda": sincronizarDemanda,
    "public.fiscaldemanda": sincronizarFiscalDemanda,
};
```

### 5. Sistema de Reconciliação

O cron job roda a cada 10 minutos e verifica:

- **Gaps de Demandas**: Compara últimos 5000 IDs entre origem e destino
- **Gaps de Fiscal-Demanda**: Sincroniza relações N:N faltantes

```javascript
// Configuração do cron
cronReconciliacao: "*/10 * * * *" // A cada 10 minutos
timezone: "America/Fortaleza"
```

---

## 📋 Pré-requisitos

- [Bun](https://bun.sh) >= 1.0
- PostgreSQL >= 12 (origem e destino)
- Acesso de leitura ao banco origem
- Acesso de escrita ao banco destino

---

## ⚙️ Configuração

### 1. Variáveis de Ambiente

```bash
cp .env.example .env
```

```env
# Banco de origem (origem)
DATABASE_ORIGEM_URL=postgres://user:pass@host:5432/db_legado

# Banco de destino (Fiscalize)
DATABASE_DESTINO_URL=postgres://user:pass@host:5432/db_fiscalize

# Intervalo do cron de reconciliação (opcional)
CRON_RECONCILIACAO="*/10 * * * *"
```

### 2. Configurar Triggers no Banco de Origem

Execute os scripts SQL na pasta `sql-origem/`:

```bash
# 1. Criar função de notificação genérica
psql -h host -U user -d db_legado -f sql-origem/01_funcao_notificar.sql

# 2. Criar trigger na tabela demanda
psql -h host -U user -d db_legado -f sql-origem/02_trigger_demanda.sql

# 3. Criar trigger na tabela fiscaldemanda
psql -h host -U user -d db_legado -f sql-origem/02_trigger_fiscal_demanda.sql
```

### 3. Verificar Triggers

```sql
-- Listar triggers ativos
SELECT tgname, tgrelid::regclass 
FROM pg_trigger 
WHERE tgname LIKE 'trigger_sync%';
```

---

## 🏃 Executando

```bash
# Instalar dependências
bun install

# Executar em produção
bun run start

# Executar em desenvolvimento (com hot reload)
bun run dev
```

### Saída Esperada

```
🚀 Middleware de Sincronização de Demandas
=========================================
📡 Iniciando listener para sincronização de demandas...
🔌 Canal: sync_channel
✅ Listener ativo e aguardando notificações...
⏰ Agendando reconciliação com cron: "*/10 * * * *"
✅ Cron job de reconciliação ativo
```

---

## 📜 Scripts Disponíveis

| Script | Comando | Descrição |
|--------|---------|-----------|
| `start` | `bun run start` | Inicia o middleware em produção |
| `dev` | `bun run dev` | Inicia com hot reload |
| `sync:fiscal-demanda` | `bun run sync:fiscal-demanda` | Sincroniza fiscal-demanda em lote |
| `sync:pessoas` | `bun sync-pessoas.js` | Sincroniza pessoas ausentes (resolve FK errors) |
| `reprocessar:erros` | `bun reprocessar-erros.js` | Sincroniza pessoas E reprocessa demandas falhadas |

### Script de Reprocessamento Completo (RECOMENDADO)

O script `reprocessar-erros.js` é a solução completa para resolver erros de FK:

```bash
bun reprocessar-erros.js
```

Este script executa automaticamente:
1. **Sincroniza pessoas ausentes** do arquivo `erros_sincronizacao.json`
2. **Reprocessa demandas falhadas** que tinham erro de FK
3. **Atualiza o arquivo de erros** removendo os resolvidos
4. **Exibe relatório completo** de sucessos e erros

**Exemplo de saída:**

```
╔══════════════════════════════════════════════════════════╗
║  REPROCESSAR DEMANDAS COM ERROS DE FK                   ║
║  1. Sincroniza pessoas ausentes                         ║
║  2. Retenta inserir demandas falhadas                   ║
╚══════════════════════════════════════════════════════════╝

📦 PASSO 1: Sincronizando pessoas ausentes...
✅ 12 pessoas sincronizadas

🔄 PASSO 2: Reprocessando demandas falhadas...
📋 Encontradas 12 demandas com erro de FK

✅ Demanda 8182121 sincronizada com sucesso
✅ Demanda 8182130 sincronizada com sucesso
...

============================================================
📊 RESULTADO DO REPROCESSAMENTO
============================================================
👥 Pessoas sincronizadas: 12
📋 Demandas reprocessadas: 12
❌ Erros persistentes: 0
============================================================

🎉 Reprocessamento concluído com sucesso!
```

### Script de Sincronização de Pessoas Ausentes

O script `sync-pessoas.js` resolve erros de foreign key relacionados a pessoas:

```bash
bun sync-pessoas.js
```

Este script:
1. Lê o arquivo `erros_sincronizacao.json`
2. Extrai IDs de pessoas ausentes (constraint `demandas_fiscalizado_id_fkey`)
3. Busca essas pessoas no banco de origem
4. Verifica quais ainda não existem no destino
5. Insere as pessoas faltantes no destino
6. Exibe relatório de sucesso/erros

**Exemplo de saída:**

```
╔══════════════════════════════════════════════════════════╗
║  SINCRONIZAÇÃO DE PESSOAS AUSENTES                      ║
║  Resolve erros de FK em demandas.fiscalizado_id         ║
╚══════════════════════════════════════════════════════════╝

🔄 Iniciando sincronização de pessoas ausentes...

📋 Encontrados 12 IDs de pessoas ausentes
📊 Status:
   Total de IDs: 12
   Já existentes: 0
   Faltantes: 12

🔍 Buscando 12 pessoas na origem...
✅ Encontradas 12 pessoas na origem
📝 Inserindo 12 pessoas no destino...

✅ Pessoa 8182101 sincronizada
✅ Pessoa 8182128 sincronizada
...

============================================================
📊 RESULTADO DA SINCRONIZAÇÃO
============================================================
✅ Sucessos: 12
❌ Erros: 0
📋 Total: 12
============================================================

🎉 Pessoas sincronizadas com sucesso!
💡 Agora você pode tentar reprocessar as demandas que falharam
```

### Script de Sincronização Manual

O script `syncFiscalDemanda.js` realiza uma sincronização em lote:

```bash
bun run sync:fiscal-demanda
```

Este script:
1. Busca demandas existentes no destino
2. Busca fiscais existentes no destino
3. Busca relações fiscal-demanda da origem
4. Filtra apenas relações válidas (demanda + fiscal existem)
5. Insere em lotes de 500 registros

---

## 📊 Mapeamento de Dados

### Tabela: `demanda` → `demandas`

| Campo Origem | Campo Destino | Observação |
|--------------|---------------|------------|
| `id` | `id` | Mantém o mesmo ID |
| `situacao` | `situacao_id` | FK para situações |
| `descricao` / `protocolo` | `fiscalizado_demanda` | Identificação |
| `logradouro` | `fiscalizado_logradouro` | |
| `numero` | `fiscalizado_numero` | |
| `complemento` | `fiscalizado_complemento` | |
| `bairro` | `fiscalizado_bairro` | |
| `municipio` | `fiscalizado_municipio` | |
| `uf` | `fiscalizado_uf` | |
| `latitude` | `fiscalizado_lat` | |
| `longitude` | `fiscalizado_lng` | |
| `data_criacao` | `data_criacao` | |
| `datafiscalizacao` / `dataexecucao` | `data_realizacao` | |
| `ativo` | `ativo` | Soft delete |
| `grupodemanda_id` | `grupo_ocorrencia_id` | FK para grupos |
| `os_direta` | `tipo_rota` | `direta` ou `ordinaria` |

### Tabela: `fiscaldemanda` → `demandas_fiscais`

| Campo Origem | Campo Destino | Observação |
|--------------|---------------|------------|
| `demanda_id` | `demanda_id` | PK composta |
| `usuario_id` | `fiscal_id` | PK composta |
| `ativo` | `ativo` | (não persistido ainda) |
| `data_criacao` | `data_criacao` | (não persistido ainda) |

---

## ⚠️ Tratamento e Prevenção de Erros

### Sistema Automático de Sincronização de Pessoas

**Nova funcionalidade:** O middleware agora sincroniza automaticamente pessoas ausentes!

Quando uma demanda referencia um `fiscalizado_id` que não existe no destino:

1. **Detecção Automática**: O handler verifica se a pessoa existe no destino
2. **Sincronização Just-in-Time**: Busca a pessoa na origem e insere no destino
3. **Processamento em Lote**: Na reconciliação, sincroniza todas as pessoas necessárias de uma vez
4. **Zero Interrupção**: A demanda é inserida sem erros de FK

**Arquivos envolvidos:**
- [`src/utils/pessoaSync.js`](src/utils/pessoaSync.js) - Utilitário de sincronização
- [`src/handlers/demandaHandler.js`](src/handlers/demandaHandler.js) - Integração no handler
- [`src/services/reconciliation.js`](src/services/reconciliation.js) - Sincronização em lote

**Exemplo de log:**

```
⚠️  Pessoa 8182101 não encontrada no destino, sincronizando...
✅ Pessoa 8182101 sincronizada automaticamente
✅ INSERT demanda ID 8182121
```

### Erros de Foreign Key (Legado)

Erros antigos ainda podem estar em `erros_sincronizacao.json`:

```json
{
  "constraint_errors": [
    {
      "id": 12345,
      "table": "demandas",
      "tipo_erro": "foreign_key_constraint",
      "constraint_name": "demandas_fiscalizado_id_fkey",
      "mensagem": "violates foreign key constraint",
      "dados": { ... },
      "timestamp": "2024-12-03T10:30:00Z"
    }
  ],
  "total": 1,
  "ultima_atualizacao": "2024-12-03T10:30:00Z"
}
```

### Validação de Dados

O handler de `fiscaldemanda` valida os dados antes de sincronizar:

- Verifica se `demanda_id` > 0
- Verifica se `fiscal_id` > 0
- Verifica se a demanda existe no destino
- Verifica se o fiscal existe no destino

---

## 🔄 Graceful Shutdown

O middleware suporta encerramento gracioso:

```javascript
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
    pararCronReconciliacao();
    await fecharConexoes();
    process.exit(0);
}
```

Pressione `Ctrl+C` para encerrar de forma segura.

---

## 📝 Logs

### Formato de Log

```
📨 Recebido: INSERT na tabela public.demanda (ID: 12345)
🔍 DEBUG INSERT ID 12345: { ... dados mapeados ... }
✅ INSERT/UPSERT demanda ID 12345
```

### Logs de Reconciliação

```
🔍 Verificando inconsistências (Reconciliação)...
📋 Reconciliando DEMANDAS...
⚠️ Encontrados 5 demandas faltando! Sincronizando...
✅ Demandas: 5 sincronizadas, 0 erros
```

---

## 🛠️ Extensibilidade

### Adicionando Nova Tabela

1. **Criar Mapper** em `src/mappers/novaTabela.js`
2. **Criar Handler** em `src/handlers/novaTabelaHandler.js`
3. **Registrar Handler** em `src/handlers/index.js`:

```javascript
export const HANDLERS = {
    "public.demanda": sincronizarDemanda,
    "public.fiscaldemanda": sincronizarFiscalDemanda,
    "public.nova_tabela": sincronizarNovaTabela, // ← adicionar
};
```

4. **Criar Trigger** no banco de origem:

```sql
CREATE TRIGGER trigger_sync_nova_tabela
AFTER INSERT OR UPDATE OR DELETE ON public.nova_tabela
FOR EACH ROW 
EXECUTE FUNCTION public.notificar_sync();
```

---

## 📝 TODO

- [ ] Implementar Dead Letter Queue para mensagens com falha
- [ ] Adicionar sincronização de outras tabelas (pessoa, fiscalizado, etc.)
- [ ] Implementar retry com backoff exponencial
- [ ] Adicionar métricas e monitoramento
- [ ] Testes automatizados

---

## 📚 Referências

- [PostgreSQL LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
- [postgres.js](https://github.com/porsager/postgres)
- [Bun SQL](https://bun.sh/docs/api/sql)
