// ============================================================
// SERVIÇO DE RECONCILIAÇÃO (OTIMIZADO)
// ============================================================

import { dbOrigem, dbDestino } from "../config/database.js";
import { sincronizarDemanda } from "../handlers/demandaHandler.js";
// A função sincronizarFiscalDemanda ainda é importada caso seja usada unitariamente,
// mas o bulk insert agora é feito via SQL puro.
import { sincronizarFiscalDemanda } from "../handlers/fiscalDemandaHandler.js"; 

/**
 * Busca o registro completo no banco de origem pelo ID
 * * @param {string} table - Nome da tabela no formato "schema.tabela"
 * @param {number} id - ID do registro
 * @returns {Promise<object|null>} Registro completo ou null se não encontrado
 */
export async function buscarRegistroOrigem(table, id) {
    try {
        let resultado;

        // Para segurança e compatibilidade, switch case por tabela
        switch (table) {
            case "public.demanda":
                resultado = await dbOrigem`SELECT * FROM public.demanda WHERE id = ${id}`;
                break;
            case "public.fiscaldemanda":
                resultado = await dbOrigem`SELECT * FROM public.fiscaldemanda WHERE id = ${id}`;
                break;
            default:
                console.error(`❌ Tabela não suportada: ${table}`);
                return null;
        }

        return resultado[0] || null;
    } catch (error) {
        console.error(`❌ Erro ao buscar registro ${table} ID ${id}:`, error.message);
        return null;
    }
}

/**
 * Verifica gaps entre origem e destino e sincroniza registros faltantes
 */
export async function verificarGaps() {
    console.log("\n🔍 Verificando inconsistências (Reconciliação)...");

    await verificarGapsDemandas();
    await verificarGapsFiscalDemanda();
}

/**
 * Verifica gaps de demandas
 * (Mantido a lógica original pois demandas possuem complexidade de relacionamentos que o handler trata)
 */
async function verificarGapsDemandas() {
    console.log("\n📋 Reconciliando DEMANDAS...");

    try {
        // Pega os últimos 5000 IDs da origem
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
        const setDestino = new Set(destinoIds.map((d) => d.id));

        // Filtra quem está na origem mas NÃO no destino
        const faltantes = origemIds.filter((d) => !setDestino.has(d.id));

        if (faltantes.length > 0) {
            console.warn(`⚠️ Encontrados ${faltantes.length} demandas faltando! Sincronizando...`);

            let sincronizados = 0;
            let erros = 0;

            for (const item of faltantes) {
                try {
                    console.log(`🔄 Recuperando demanda ID: ${item.id}`);
                    const registro = await buscarRegistroOrigem("public.demanda", item.id);
                    if (registro) {
                        await sincronizarDemanda("INSERT", registro);
                        sincronizados++;
                    }
                } catch (error) {
                    console.error(`❌ Erro ao sincronizar demanda ID ${item.id}:`, error.message);
                    erros++;
                }
            }

            console.log(`✅ Demandas: ${sincronizados} sincronizadas, ${erros} erros`);
        } else {
            console.log("✅ Demandas: nenhuma inconsistência encontrada.");
        }
    } catch (error) {
        console.error("❌ Erro ao verificar gaps de demandas:", error.message);
    }
}

/**
 * Verifica gaps de fiscal-demanda (VERSÃO ALTA PERFORMANCE)
 * Utiliza Tabela Temporária e Bulk Insert para evitar processamento linha-a-linha
 */
async function verificarGapsFiscalDemanda() {
    console.log("\n👤 Reconciliando FISCAL-DEMANDA (Fast Mode)...");
    const start = performance.now();

    try {
        // 1. Busca os últimos 10000 registros da origem (limite razoável para não perder tempo)
        const origemRegistros = await dbOrigem`
            SELECT id, demanda_id, usuario_id 
            FROM public.fiscaldemanda 
            WHERE ativo = true
            ORDER BY id DESC 
            LIMIT 10000
        `;

        if (origemRegistros.length === 0) {
            console.log("📭 Nenhum registro de fiscal-demanda na origem");
            return;
        }

        console.log(`📊 Encontrados ${origemRegistros.length} registros na origem (ID ${origemRegistros[0].id} até ${origemRegistros[origemRegistros.length - 1].id})`);

        // 2. Executar tudo em uma única transação
        const resultado = await dbDestino.begin(async (tx) => {
            // Criar tabela temporária
            await tx`
                CREATE TEMP TABLE tmp_reconcile_fiscal (
                    demanda_id INT,
                    fiscal_id INT
                ) ON COMMIT DROP
            `;

            // Mapear usuario_id para fiscal_id
            const dadosParaInserir = origemRegistros.map(r => ({
                demanda_id: r.demanda_id,
                fiscal_id: r.usuario_id
            }));

            // Inserção em massa na tabela temporária
            await tx`
                INSERT INTO tmp_reconcile_fiscal ${tx(dadosParaInserir, 'demanda_id', 'fiscal_id')}
            `;

            console.log(`🔍 Validando registros (demandas e fiscais devem existir no destino)...`);

            // Sincronização inteligente via SQL
            return await tx`
                INSERT INTO fiscalizacao.demandas_fiscais (demanda_id, fiscal_id)
                SELECT 
                    t.demanda_id, 
                    t.fiscal_id
                FROM tmp_reconcile_fiscal t
                INNER JOIN fiscalizacao.demandas d ON d.id = t.demanda_id
                INNER JOIN fiscalizacao.fiscais f ON f.id = t.fiscal_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM fiscalizacao.demandas_fiscais df 
                    WHERE df.demanda_id = t.demanda_id AND df.fiscal_id = t.fiscal_id
                )
                ON CONFLICT (demanda_id, fiscal_id) DO NOTHING
                RETURNING demanda_id, fiscal_id
            `;
        });

        const end = performance.now();
        const duration = ((end - start) / 1000).toFixed(2);

        if (resultado.length > 0) {
            console.log(`✅ Fiscal-Demanda: ${resultado.length} registros restaurados em ${duration}s.`);
        } else {
            console.log(`✅ Fiscal-Demanda: Nenhuma inconsistência encontrada (Verificados ${origemRegistros.length} registros em ${duration}s).`);
        }

    } catch (error) {
        console.error("❌ Erro ao verificar gaps de fiscal-demanda:", error.message);
        console.error(error.stack);
    }
}