import postgres from "postgres";
import { SQL } from "bun";
import cron from "node-cron";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";

// ============================================================
// CONFIGURAÇÃO
// ============================================================

const CONFIG = {
    origem: {
        url: process.env.DATABASE_ORIGEM_URL || "postgres://postgres:postgres@localhost:5432/fiscalize",
    },
    destino: {
        url: process.env.DATABASE_DESTINO_URL || "postgres://postgres:postgres@localhost:5432/agefis",
    },
    canal: "sync_channel",
    reconnectDelay: 5000, // 5 segundos para reconexão
    cronReconciliacao: process.env.CRON_RECONCILIACAO || "*/10 * * * *", // A cada 10 minutos
};

// Conexão com banco de origem usando postgres.js (suporta LISTEN/NOTIFY)
const dbOrigem = postgres(CONFIG.origem.url);

// Conexão com banco de destino usando Bun SQL (para escrita)
const dbDestino = new SQL(CONFIG.destino.url);

// ============================================================
// LOGGING DE ERROS DE CONSTRAINT
// ============================================================

const ERROS_FILE = "./erros_sincronizacao.json";

function carregarErros() {
    if (existsSync(ERROS_FILE)) {
        try {
            const conteudo = readFileSync(ERROS_FILE, "utf-8");
            return JSON.parse(conteudo);
        } catch (error) {
            console.error("❌ Erro ao ler arquivo de erros:", error.message);
            return { constraint_errors: [], total: 0, ultima_atualizacao: new Date().toISOString() };
        }
    }
    return { constraint_errors: [], total: 0, ultima_atualizacao: new Date().toISOString() };
}

function extrairConstraintInfo(mensagemErro) {
    // Extrai o nome da constraint do erro
    const constraintMatch = mensagemErro.match(/constraint "([^"]+)"/);
    const constraintName = constraintMatch ? constraintMatch[1] : null;

    // Determina qual campo causou o erro baseado no nome da constraint
    let campo = null;
    if (constraintName) {
        // Pattern: tabela_campo_fkey
        const campoMatch = constraintName.match(/demandas_(.+)_fkey/);
        campo = campoMatch ? campoMatch[1] : constraintName;
    }

    return { constraintName, campo };
}

function salvarErro(id, table, tipo_erro, mensagem_erro, dadosCompletos = null) {
    const erros = carregarErros();

    // Verificar se já existe este erro exato
    const jaExiste = erros.constraint_errors.some(e => e.id === id && e.table === table && e.mensagem_erro === mensagem_erro);

    if (!jaExiste) {
        const { constraintName, campo } = extrairConstraintInfo(mensagem_erro);

        // Extrair valor do campo que causou o erro
        let valorProblematico = null;
        if (campo && dadosCompletos) {
            valorProblematico = dadosCompletos[campo];
        }

        const erroDetalhado = {
            id,
            table,
            tipo_erro,
            constraint_name: constraintName,
            campo_erro: campo,
            valor_problematico: valorProblematico,
            dados_completos: dadosCompletos,
            mensagem_erro,
            timestamp: new Date().toISOString(),
        };

        erros.constraint_errors.push(erroDetalhado);

        // Agregar valores únicos faltando por tipo de constraint
        if (!erros.valores_faltando) {
            erros.valores_faltando = {};
        }

        if (campo && valorProblematico !== null && valorProblematico !== undefined) {
            if (!erros.valores_faltando[campo]) {
                erros.valores_faltando[campo] = [];
            }
            if (!erros.valores_faltando[campo].includes(valorProblematico)) {
                erros.valores_faltando[campo].push(valorProblematico);
            }
        }

        erros.total = erros.constraint_errors.length;
        erros.ultima_atualizacao = new Date().toISOString();

        writeFileSync(ERROS_FILE, JSON.stringify(erros, null, 2));
        console.log(`📝 Erro registrado: ${constraintName} - Campo: ${campo}, Valor: ${valorProblematico}`);
    }
}

// ============================================================
// MAPEAMENTO DE CAMPOS: demanda (origem) -> demandas (destino)
// ============================================================

/**
 * Mapeia os campos da tabela `demanda` (origem) para `demandas` (destino Prisma)
 * Apenas campos que existem no schema do destino são mapeados
 */
function mapearDemanda(dataOrigem) {
    // Converte ID para número (a origem pode retornar como string)
    const id = typeof dataOrigem.id === 'string' ? parseInt(dataOrigem.id, 10) : dataOrigem.id;
    const grupo_ocorrencia_id = typeof dataOrigem.grupodemanda_id === 'string' ? parseInt(dataOrigem.grupodemanda_id, 10) : dataOrigem.grupodemanda_id;

    return {
        // Campo ID
        id: id,

        // Situação e motivo
        situacao_id: dataOrigem.situacao,
        motivo_id: null, // Não existe na origem 
        fiscal_id: null, // Será preenchido depois se necessário

        // Identificação da demanda
        fiscalizado_demanda: dataOrigem.descricao || dataOrigem.protocolo || `DEMANDA-${dataOrigem.id}`,

        // Dados do fiscalizado (deixar vazio por enquanto)
        fiscalizado_cpf_cnpj: "",
        fiscalizado_nome: "",

        // Endereço do fiscalizado
        fiscalizado_logradouro: dataOrigem.logradouro || "",
        fiscalizado_numero: dataOrigem.numero || "",
        fiscalizado_complemento: dataOrigem.complemento || "",
        fiscalizado_bairro: dataOrigem.bairro || "",
        fiscalizado_municipio: dataOrigem.municipio || null,
        fiscalizado_uf: dataOrigem.uf || null,

        // Localização geográfica
        fiscalizado_lat: dataOrigem.latitude || "",
        fiscalizado_lng: dataOrigem.longitude || "",

        // Classificação da demanda
        classificacao: dataOrigem.os_direta ? "direta" : "ordinaria",

        // Datas importantes
        data_criacao: dataOrigem.data_criacao,
        data_realizacao: dataOrigem.datafiscalizacao || dataOrigem.dataexecucao || dataOrigem.data_criacao,

        // Status
        ativo: dataOrigem.ativo,

        // Tipo de rota
        tipo_rota: dataOrigem.tipo_rota || null,

        // Relacionamentos
        grupo_ocorrencia_id: grupo_ocorrencia_id || 1,
    };
}

// ============================================================
// SINCRONIZAÇÃO
// ============================================================

async function sincronizarDemanda(event_type, data) {
    const demandaMapeada = mapearDemanda(data);

    try {
        console.log(`\n🔍 DEBUG ${event_type} ID ${data.id}:`, JSON.stringify(demandaMapeada, null, 2));

        if (event_type === "INSERT") {
            await dbDestino`
        INSERT INTO fiscalizacao.demandas (
          id, situacao_id, motivo_id, fiscal_id,
          fiscalizado_demanda, fiscalizado_cpf_cnpj, fiscalizado_nome,
          fiscalizado_logradouro, fiscalizado_numero, fiscalizado_complemento,
          fiscalizado_bairro, fiscalizado_municipio, fiscalizado_uf,
          fiscalizado_lat, fiscalizado_lng,
          data_criacao, data_realizacao, ativo, tipo_rota, grupo_ocorrencia_id
        )
        VALUES (
          ${demandaMapeada.id},
          ${demandaMapeada.situacao_id},
          ${demandaMapeada.motivo_id},
          ${demandaMapeada.fiscal_id},
          ${demandaMapeada.fiscalizado_demanda},
          ${demandaMapeada.fiscalizado_cpf_cnpj},
          ${demandaMapeada.fiscalizado_nome},
          ${demandaMapeada.fiscalizado_logradouro},
          ${demandaMapeada.fiscalizado_numero},
          ${demandaMapeada.fiscalizado_complemento},
          ${demandaMapeada.fiscalizado_bairro},
          ${demandaMapeada.fiscalizado_municipio},
          ${demandaMapeada.fiscalizado_uf},
          ${demandaMapeada.fiscalizado_lat},
          ${demandaMapeada.fiscalizado_lng},
          ${demandaMapeada.data_criacao},
          ${demandaMapeada.data_realizacao},
          ${demandaMapeada.ativo},
          ${demandaMapeada.tipo_rota},
          ${demandaMapeada.grupo_ocorrencia_id}
        )
        ON CONFLICT (id) DO UPDATE SET
          situacao_id = EXCLUDED.situacao_id,
          fiscalizado_demanda = EXCLUDED.fiscalizado_demanda,
          fiscalizado_logradouro = EXCLUDED.fiscalizado_logradouro,
          fiscalizado_numero = EXCLUDED.fiscalizado_numero,
          fiscalizado_complemento = EXCLUDED.fiscalizado_complemento,
          fiscalizado_bairro = EXCLUDED.fiscalizado_bairro,
          fiscalizado_lat = EXCLUDED.fiscalizado_lat,
          fiscalizado_lng = EXCLUDED.fiscalizado_lng,
          data_realizacao = EXCLUDED.data_realizacao,
          ativo = EXCLUDED.ativo
      `;
            console.log(`✅ INSERT/UPSERT demanda ID ${data.id}`);
        }

        if (event_type === "UPDATE") {
            // Primeiro verifica se o registro existe
            const existe = await dbDestino`SELECT id FROM fiscalizacao.demandas WHERE id = ${demandaMapeada.id}`;

            if (existe.length === 0) {
                console.warn(`⚠️ Registro não encontrado no destino ID ${demandaMapeada.id}, fazendo INSERT...`);
                // Se não existir, faz INSERT
                await dbDestino`
            INSERT INTO fiscalizacao.demandas (
              id, situacao_id, motivo_id, fiscal_id,
              fiscalizado_demanda, fiscalizado_cpf_cnpj, fiscalizado_nome,
              fiscalizado_logradouro, fiscalizado_numero, fiscalizado_complemento,
              fiscalizado_bairro, fiscalizado_municipio, fiscalizado_uf,
              fiscalizado_lat, fiscalizado_lng,
              data_criacao, data_realizacao, ativo, tipo_rota, grupo_ocorrencia_id
            )
            VALUES (
              ${demandaMapeada.id},
              ${demandaMapeada.situacao_id},
              ${demandaMapeada.motivo_id},
              ${demandaMapeada.fiscal_id},
              ${demandaMapeada.fiscalizado_demanda},
              ${demandaMapeada.fiscalizado_cpf_cnpj},
              ${demandaMapeada.fiscalizado_nome},
              ${demandaMapeada.fiscalizado_logradouro},
              ${demandaMapeada.fiscalizado_numero},
              ${demandaMapeada.fiscalizado_complemento},
              ${demandaMapeada.fiscalizado_bairro},
              ${demandaMapeada.fiscalizado_municipio},
              ${demandaMapeada.fiscalizado_uf},
              ${demandaMapeada.fiscalizado_lat},
              ${demandaMapeada.fiscalizado_lng},
              ${demandaMapeada.data_criacao},
              ${demandaMapeada.data_realizacao},
              ${demandaMapeada.ativo},
              ${demandaMapeada.tipo_rota},
              ${demandaMapeada.grupo_ocorrencia_id}
            )`;
                console.log(`✅ INSERT (por UPDATE) demanda ID ${demandaMapeada.id}`);
            } else {
                // Se existir, faz UPDATE
                const resultado = await dbDestino`
            UPDATE fiscalizacao.demandas SET
              situacao_id = ${demandaMapeada.situacao_id},
              fiscalizado_demanda = ${demandaMapeada.fiscalizado_demanda},
              fiscalizado_logradouro = ${demandaMapeada.fiscalizado_logradouro},
              fiscalizado_numero = ${demandaMapeada.fiscalizado_numero},
              fiscalizado_complemento = ${demandaMapeada.fiscalizado_complemento},
              fiscalizado_bairro = ${demandaMapeada.fiscalizado_bairro},
              fiscalizado_lat = ${demandaMapeada.fiscalizado_lat},
              fiscalizado_lng = ${demandaMapeada.fiscalizado_lng},
              data_realizacao = ${demandaMapeada.data_realizacao},
              ativo = ${demandaMapeada.ativo}
            WHERE id = ${demandaMapeada.id}
          `;
                console.log(`✅ UPDATE demanda ID ${demandaMapeada.id}, registros afetados: ${resultado.count}`);
            }
        }

        if (event_type === "DELETE") {
            await dbDestino`
        UPDATE fiscalizacao.demandas SET ativo = false WHERE id = ${data.id}
      `;
            console.log(`🗑️ SOFT DELETE demanda ID ${data.id}`);
        }
    } catch (error) {
        console.error(`❌ Erro ao sincronizar demanda ID ${data.id}:`, error.message);

        // Verificar se é erro de constraint (FK)
        if (error.message.includes("violates foreign key constraint") || error.message.includes("violates")) {
            salvarErro(
                data.id,
                "demandas",
                "foreign_key_constraint",
                error.message,
                demandaMapeada // Passa todos os dados mapeados para debug
            );
        }

        // TODO: Implementar Dead Letter Queue ou retry
        throw error;
    }
}

// ============================================================
// BUSCAR REGISTRO COMPLETO NO BANCO DE ORIGEM
// ============================================================

/**
 * Busca o registro completo no banco de origem pelo ID
 * @param {string} table - Nome da tabela no formato "schema.tabela"
 * @param {number} id - ID do registro
 * @returns {Promise<object|null>} Registro completo ou null se não encontrado
 */
async function buscarRegistroOrigem(table, id) {
    try {

        // Executa query usando postgres.js com template literal
        // Nota: não podemos usar ${table} diretamente, precisamos de uma solução
        // Vamos usar a conexão existente com a query correta
        let resultado;

        // Para segurança e compatibilidade, vamos fazer assim:
        if (table === "public.demanda") {
            resultado = await dbOrigem`SELECT * FROM public.demanda WHERE id = ${id}`;
        } else if (table === "public.fiscaldemanda") {
            resultado = await dbOrigem`SELECT * FROM public.fiscaldemanda WHERE id = ${id}`;
        } else {
            console.error(`❌ Tabela não suportada: ${table}`);
            return null;
        }

        return resultado[0] || null;
    } catch (error) {
        console.error(`❌ Erro ao buscar registro ${table} ID ${id}:`, error.message);
        return null;
    }
}

// ============================================================
// SINCRONIZAÇÃO FISCAL-DEMANDA (relação N:N)
// ============================================================

async function sincronizarFiscalDemanda(event_type, data) {
    console.log(`\n👤 FISCAL-DEMANDA ${event_type}:`);
    console.log(`   📋 Dados recebidos:`, JSON.stringify(data, null, 2));

    // Por enquanto apenas loga - implementar lógica depois
    // A tabela fiscaldemanda relaciona fiscal com demanda
    // Campos esperados: id, fiscal_id, demanda_id, data_criacao, etc.

    if (event_type === "INSERT") {
        console.log(`   ✅ Nova relação fiscal-demanda registrada`);
        // TODO: Implementar INSERT em fiscalizacao.demandas_fiscais ou similar
    }

    if (event_type === "UPDATE") {
        console.log(`   🔄 Relação fiscal-demanda atualizada`);
        // TODO: Implementar UPDATE
    }

    if (event_type === "DELETE") {
        console.log(`   🗑️ Relação fiscal-demanda removida`);
        // TODO: Implementar DELETE/soft delete
    }
}

// ============================================================
// HANDLERS POR TABELA
// ============================================================

const HANDLERS = {
    "public.demanda": sincronizarDemanda,
    "public.fiscaldemanda": sincronizarFiscalDemanda,
    // Adicionar mais handlers conforme necessário:
    // "public.pessoa": sincronizarPessoa,
};

// ============================================================
// VERIFICAÇÃO DE GAPS - RECONCILIAÇÃO
// ============================================================

async function verificarGaps() {
    console.log("\n🔍 Verificando inconsistências (Reconciliação)...");

    try {
        // Pega os últimos 1000 IDs da origem
        const origemIds = await dbOrigem`
            SELECT id FROM public.demanda 
            ORDER BY id DESC LIMIT 5000
        `;

        if (origemIds.length === 0) {
            console.log("📭 Nenhum registro na origem para verificar");
            return;
        }

        const minId = origemIds[origemIds.length - 1].id;
        const maxId = origemIds[0].id;

        // Pega o que temos no destino nesse range
        const destinoIds = await dbDestino`
            SELECT id FROM fiscalizacao.demandas 
            WHERE id BETWEEN ${minId} AND ${maxId}
        `;

        // Cria Sets para comparação rápida
        const setDestino = new Set(destinoIds.map(d => d.id));

        // Filtra quem está na origem mas NÃO no destino
        const faltantes = origemIds.filter(d => !setDestino.has(d.id));

        if (faltantes.length > 0) {
            console.warn(`⚠️ Encontrados ${faltantes.length} registros faltando! Sincronizando...`);

            let sincronizados = 0;
            let erros = 0;

            for (const item of faltantes) {
                try {
                    console.log(`🔄 Recuperando ID perdido: ${item.id}`);
                    // Reutiliza sua lógica existente
                    const registro = await buscarRegistroOrigem("public.demanda", item.id);
                    if (registro) {
                        await sincronizarDemanda("INSERT", registro);
                        sincronizados++;
                    }
                } catch (error) {
                    console.error(`❌ Erro ao sincronizar ID ${item.id}:`, error.message);
                    // Erro já registrado dentro de sincronizarDemanda
                    erros++;
                }
            }

            console.log(`✅ Reconciliação concluída: ${sincronizados} sincronizados, ${erros} erros`);
        } else {
            console.log("✅ Nenhuma inconsistência recente encontrada.");
        }
    } catch (error) {
        console.error("❌ Erro ao verificar gaps:", error.message);
    }
}

// ============================================================
// AGENDAMENTO - CRON JOB
// ============================================================

let cronJob = null;

function iniciarCronReconciliacao() {
    console.log(`⏰ Agendando reconciliação com cron: "${CONFIG.cronReconciliacao}"`);

    cronJob = cron.schedule(CONFIG.cronReconciliacao, async () => {
        console.log(`\n⏱️  [${new Date().toISOString()}] Executando reconciliação agendada...`);
        await verificarGaps();
    }, {
        runOnInit: false, // Não executar na inicialização
        timezone: "America/Fortaleza" // Usar fuso horário local
    });

    console.log("✅ Cron job de reconciliação ativo");
}

function pararCronReconciliacao() {
    if (cronJob) {
        cronJob.stop();
        cronJob.destroy();
        console.log("🛑 Cron job de reconciliação parado");
    }
}

// ============================================================
// LISTENER - USANDO LISTEN/NOTIFY DO POSTGRES
// ============================================================

async function processarNotificacao(payload) {
    try {
        const { id, table, event_type } = JSON.parse(payload);

        console.log(`📨 Recebido: ${event_type} na tabela ${table} (ID: ${id})`);

        // Verifica se temos um handler para esta tabela
        const handler = HANDLERS[table];
        if (!handler) {
            console.warn(`⚠️ Nenhum handler configurado para tabela: ${table}`);
            return;
        }

        // Para DELETE, não precisamos buscar o registro (já foi deletado)
        if (event_type === "DELETE") {
            await handler(event_type, { id });
            return;
        }

        // Busca o registro completo no banco de origem
        const registroCompleto = await buscarRegistroOrigem(table, id);

        if (!registroCompleto) {
            console.error(`❌ Registro não encontrado: ${table} ID ${id}`);
            return;
        }

        console.log(JSON.stringify(registroCompleto, null, 2))

        // Processa a sincronização com dados completos
        await handler(event_type, registroCompleto);

    } catch (error) {
        console.error("❌ Erro ao processar notificação:", error.message);
        console.error("Payload recebido:", payload);
    }
}

async function iniciarListener() {
    console.log("📡 Iniciando listener para sincronização de demandas...");
    console.log(`🔌 Canal: ${CONFIG.canal}`);

    try {
        // Usa LISTEN/NOTIFY do postgres.js
        await dbOrigem.listen(CONFIG.canal, async (payload) => {
            await processarNotificacao(payload);
        });

        console.log("✅ Listener ativo e aguardando notificações...");


    } catch (error) {
        console.error("❌ Erro no listener:", error.message);
        console.log(`🔄 Reconectando em ${CONFIG.reconnectDelay / 1000} segundos...`);

        setTimeout(() => {
            iniciarListener();
        }, CONFIG.reconnectDelay);
    }
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function shutdown() {
    console.log("\n🛑 Encerrando middleware...");

    pararCronReconciliacao();

    try {
        await dbOrigem.end(); // postgres.js usa .end()
        await dbDestino.close(); // Bun SQL usa .close()
        console.log("✅ Conexões fechadas com sucesso");
    } catch (error) {
        console.error("❌ Erro ao fechar conexões:", error.message);
    }

    process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ============================================================
// INICIALIZAÇÃO
// ============================================================

console.log("🚀 Middleware de Sincronização de Demandas");
console.log("=========================================");

iniciarListener();
iniciarCronReconciliacao();

// Trigger imediato da reconciliação
// console.log("⚡ Executando reconciliação imediata na inicialização...");
// verificarGaps();