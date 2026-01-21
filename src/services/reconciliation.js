// ============================================================
// SERVIÇO DE RECONCILIAÇÃO (OTIMIZADO)
// ============================================================

import { dbOrigem, dbDestino } from "../config/database.js";
import { sincronizarDemanda, sincronizarDemandasBulk } from "../handlers/demandaHandler.js";
// A função sincronizarFiscalDemanda ainda é importada caso seja usada unitariamente,
// mas o bulk insert agora é feito via SQL puro.
import { sincronizarFiscalDemanda } from "../handlers/fiscalDemandaHandler.js";
import { garantirPessoasExistem } from "../utils/pessoaSync.js";
import { mapearDemanda } from "../mappers/demandaMapper.js";

/**
 * Sincroniza pessoas faltantes em bulk antes de inserir demandas
 */
async function sincronizarPessoasFaltantes(fiscalizadosIds) {
    if (!fiscalizadosIds || fiscalizadosIds.length === 0) return;
    
    // Remover nulls e duplicatas
    const idsUnicos = [...new Set(fiscalizadosIds.filter(id => id != null))];
    if (idsUnicos.length === 0) return;

    // Verificar quais pessoas já existem no destino
    const pessoasExistentes = await dbDestino`
        SELECT id FROM fiscalizacao.pessoas WHERE id = ANY(${idsUnicos})
    `;
    const idsExistentes = new Set(pessoasExistentes.map(p => p.id));
    const idsFaltantes = idsUnicos.filter(id => !idsExistentes.has(id));

    if (idsFaltantes.length === 0) return;

    if (idsFaltantes.length > 0) {
        console.log(`  🔄 Pré-sincronizando ${idsFaltantes.length} pessoas faltantes...`);
    }
    await garantirPessoasExistem(idsFaltantes);
} 

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
 * @param {boolean} verbose - Se true, mostra logs detalhados. Se false, só erros (para cron)
 */
export async function verificarGaps(verbose = false) {
    if (verbose) {
        console.log("\n🔍 Verificando inconsistências (Reconciliação)...");
    }

    await verificarGapsDemandas(verbose);
    await sincronizarDemandasExistentes(verbose);
    await verificarGapsFiscalDemanda(verbose);
}

/**
 * Verifica gaps de demandas
 * (Mantido a lógica original pois demandas possuem complexidade de relacionamentos que o handler trata)
 * @param {boolean} verbose - Se true, mostra logs detalhados
 */
async function verificarGapsDemandas(verbose = false) {
    if (verbose) {
        console.log("\n📋 Reconciliando DEMANDAS...");
    }

    try {
        // Pega os últimos 5000 IDs da origem
        const origemIds = await dbOrigem`
            SELECT id FROM public.demanda 
            WHERE situacao IN (2, 11, 12)
            ORDER BY id DESC LIMIT 20000
        `;

        if (origemIds.length === 0) {
            if (verbose) {
                console.log("📭 Nenhum registro na origem para verificar");
            }
            return;
        }

        const minId = origemIds[origemIds.length - 1].id;
        const maxId = origemIds[0].id;

        if (verbose) {
            console.log(`🔍 Verificando range: ${minId} até ${maxId}`);
        }

        // Pega o que temos no destino nesse range
        const destinoIds = await dbDestino`
            SELECT id FROM fiscalizacao.demandas 
            WHERE id BETWEEN ${minId} AND ${maxId}
        `;

        // Cria Sets para comparação rápida (converter IDs da origem para número se necessário)
        const origemIdsNumeros = origemIds.map(d => typeof d.id === 'string' ? parseInt(d.id, 10) : d.id);
        const setDestino = new Set(destinoIds.map((d) => d.id));

        // Filtra quem está na origem mas NÃO no destino
        const faltantes = origemIdsNumeros.filter((id) => !setDestino.has(id)).map(id => ({id}));

        if (faltantes.length > 0) {
            if (verbose) {
                console.warn(`⚠️ Encontrados ${faltantes.length} demandas faltando!`);
            }
            
            // Processar em lotes de 100 para otimizar
            const LOTE_SIZE = 200;
            const totalLotes = Math.ceil(faltantes.length / LOTE_SIZE);
            
            if (verbose) {
                console.log(`📦 Processando em ${totalLotes} lotes de até ${LOTE_SIZE} demandas...\n`);
            }

            let sincronizadosTotal = 0;
            let errosTotal = 0;

            for (let i = 0; i < faltantes.length; i += LOTE_SIZE) {
                const lote = faltantes.slice(i, i + LOTE_SIZE);
                const loteNum = Math.floor(i / LOTE_SIZE) + 1;
                
                if (verbose) {
                    console.log(`📋 Lote ${loteNum}/${totalLotes} (${lote.length} demandas)...`);
                }

                // Buscar TODAS as demandas do lote de uma vez (OTIMIZADO)
                // Normalizar IDs para evitar problemas de tipo
                const idsLote = lote.map(item => {
                    const id = item.id;
                    return typeof id === 'string' ? parseInt(id, 10) : id;
                });
                
                if (verbose) {
                    console.log(`  📝 IDs do lote:`, idsLote.slice(0, 5)); // Mostrar primeiros 5 IDs
                }
                
                const demandasOrigem = await dbOrigem`
                    SELECT * FROM public.demanda WHERE id = ANY(${idsLote})
                `;

                if (demandasOrigem.length === 0) continue;

                if (verbose) {
                    console.log(`  📦 Encontradas ${demandasOrigem.length} demandas na origem`);
                    // Verificar se algum ID tem problema
                    const primeiroId = demandasOrigem[0]?.id;
                    console.log(`  🔍 Primeiro ID tipo: ${typeof primeiroId}, valor:`, JSON.stringify(primeiroId));
                }

                // PRÉ-SINCRONIZAR pessoas faltantes para evitar FK errors no bulk
                // Normalizar IDs de fiscalizados para número
                const fiscalizadosIds = demandasOrigem
                    .map(d => d.fiscalizado_id)
                    .filter(id => id != null)
                    .map(id => typeof id === 'string' ? parseInt(id, 10) : id);
                
                if (verbose && fiscalizadosIds.length > 0) {
                    console.log(`  👥 Fiscalizados IDs (primeiros 3):`, fiscalizadosIds.slice(0, 3));
                }
                await sincronizarPessoasFaltantes(fiscalizadosIds);

                // Preparar dados para bulk (dados já mapeados e validados)
                const demandasParaBulk = demandasOrigem.map(d => ({
                    data: d,
                    dadosPessoa: null // Pessoas já foram sincronizadas acima
                }));

                if (verbose) {
                    console.log(`  📦 Chamando sincronizarDemandasBulk com ${demandasParaBulk.length} demandas...`);
                }

                // Inserir em BULK (muito mais rápido)
                try {
                    const resultado = await sincronizarDemandasBulk(demandasParaBulk);
                    
                    sincronizadosTotal += resultado.sucesso;
                    errosTotal += resultado.erros;

                    if (verbose) {
                        console.log(`  ✅ ${resultado.sucesso} OK, ${resultado.erros} erros\n`);
                    }
                } catch (bulkError) {
                    console.error(`  ❌ Erro no bulk insert:`, bulkError.message);
                    errosTotal += demandasParaBulk.length;
                }
            }

            if (verbose) {
                console.log(`\n${"=".repeat(60)}`);
                console.log(`📊 TOTAL: ${sincronizadosTotal} sincronizadas, ${errosTotal} erros`);
                console.log("=".repeat(60));
            } else if (sincronizadosTotal > 0 || errosTotal > 0) {
                // Em modo silencioso, mostrar apenas resumo se houve mudanças
                console.log(`✅ Reconciliação: ${sincronizadosTotal} sincronizadas, ${errosTotal} erros`);
            }
        } else {
            if (verbose) {
                console.log("✅ Demandas: nenhuma inconsistência encontrada.");
            }
        }
    } catch (error) {
        console.error("❌ Erro ao verificar gaps de demandas:", error.message);
    }
}

/**
 * Sincroniza demandas existentes no destino com dados atualizados da origem
 * Atualiza situacao_id e fiscalizado_id das demandas com situação 2, 11 ou 12
 * @param {boolean} verbose - Se true, mostra logs detalhados
 */
async function sincronizarDemandasExistentes(verbose = false) {
    if (verbose) {
        console.log("\n🔄 Sincronizando demandas existentes no destino...");
    }

    try {
        // 1. Buscar demandas RECENTES no destino com situação 2, 11 ou 12
        // LIMIT para evitar sobrecarga de memória e focar nas mais relevantes
        const demandasDestino = await dbDestino`
            SELECT id FROM fiscalizacao.demandas
            WHERE situacao_id IN (2, 11, 12)
            ORDER BY id DESC
            LIMIT 10000
        `;

        if (demandasDestino.length === 0) {
            if (verbose) {
                console.log("📭 Nenhuma demanda no destino para sincronizar");
            }
            return;
        }

        if (verbose) {
            console.log(`✅ Encontradas ${demandasDestino.length} demandas no destino (últimas 10k)`);
        }

        // 2. Processar em lotes para otimizar
        const LOTE_SIZE = 100; // Reduzido para evitar queries SQL muito grandes
        const totalLotes = Math.ceil(demandasDestino.length / LOTE_SIZE);
        
        if (verbose) {
            console.log(`📦 Processando ${demandasDestino.length} demandas em ${totalLotes} lotes de até ${LOTE_SIZE}...\n`);
        }

        let sincronizadosTotal = 0;
        let errosTotal = 0;
        let naoEncontradosOrigem = 0;

        for (let i = 0; i < demandasDestino.length; i += LOTE_SIZE) {
            const lote = demandasDestino.slice(i, i + LOTE_SIZE);
            const loteNum = Math.floor(i / LOTE_SIZE) + 1;
            
            if (verbose) {
                console.log(`📋 Lote ${loteNum}/${totalLotes} (${lote.length} demandas)...`);
            }

            // Buscar dados das demandas na ORIGEM
            const idsLote = lote.map(item => item.id);
            const demandasOrigem = await dbOrigem`
                SELECT * FROM public.demanda 
                WHERE id = ANY(${idsLote})
            `;

            if (demandasOrigem.length === 0) {
                if (verbose) {
                    console.log(`  ⚠️  Nenhuma demanda encontrada na origem para este lote`);
                }
                naoEncontradosOrigem += lote.length;
                continue;
            }

            // Contar quantas não foram encontradas na origem
            const idsEncontradosOrigem = new Set(demandasOrigem.map(d => typeof d.id === 'string' ? parseInt(d.id, 10) : d.id));
            const naoEncontrados = lote.filter(d => !idsEncontradosOrigem.has(d.id));
            naoEncontradosOrigem += naoEncontrados.length;

            if (verbose && naoEncontrados.length > 0) {
                console.log(`  ⚠️  ${naoEncontrados.length} demandas não encontradas na origem`);
            }

            // Processar demandas em BULK UPDATE usando tabela temporária
            let sucessoLote = 0;
            let errosLote = 0;

            if (demandasOrigem.length > 0) {
                try {
                    // Mapear todas as demandas
                    const dadosMapeados = demandasOrigem.map(d => {
                        const mapped = mapearDemanda(d, null);
                        return {
                            id: parseInt(mapped.id, 10),
                            situacao_id: parseInt(mapped.situacao_id, 10),
                            fiscalizado_id: mapped.fiscalizado_id ? parseInt(mapped.fiscalizado_id, 10) : null
                        };
                    });

                    // PRÉ-SINCRONIZAR pessoas faltantes para evitar FK errors
                    const fiscalizadosIds = dadosMapeados
                        .map(d => d.fiscalizado_id)
                        .filter(id => id != null);
                    await sincronizarPessoasFaltantes(fiscalizadosIds);

                    // UPDATE usando tabela temporária - sem limites de protocolo
                    await dbDestino.begin(async (tx) => {
                        // Criar tabela temporária
                        await tx`
                            CREATE TEMP TABLE tmp_update_demandas (
                                id INT,
                                situacao_id INT,
                                fiscalizado_id INT
                            ) ON COMMIT DROP
                        `;

                        // Inserir dados em lotes pequenos na temp table
                        const CHUNK_SIZE = 100;
                        for (let j = 0; j < dadosMapeados.length; j += CHUNK_SIZE) {
                            const chunk = dadosMapeados.slice(j, j + CHUNK_SIZE);
                            await tx`
                                INSERT INTO tmp_update_demandas 
                                ${tx(chunk, 'id', 'situacao_id', 'fiscalizado_id')}
                            `;
                        }

                        // UPDATE em massa usando a temp table
                        await tx`
                            UPDATE fiscalizacao.demandas d
                            SET 
                                situacao_id = t.situacao_id,
                                fiscalizado_id = t.fiscalizado_id
                            FROM tmp_update_demandas t
                            WHERE d.id = t.id
                        `;
                    });

                    sucessoLote = demandasOrigem.length;
                } catch (error) {
                    console.error(`  ❌ Erro no bulk update:`, error.message);
                    errosLote = demandasOrigem.length;
                }
            }

            sincronizadosTotal += sucessoLote;
            errosTotal += errosLote;

            if (verbose) {
                console.log(`  ✅ ${sucessoLote} atualizadas, ${errosLote} erros\n`);
            }
        }

        if (verbose) {
            console.log(`\n${"=".repeat(60)}`);
            console.log(`📊 SINCRONIZAÇÃO DEMANDAS EXISTENTES:`);
            console.log(`   ✅ Atualizadas: ${sincronizadosTotal}`);
            console.log(`   ⚠️  Não encontradas na origem: ${naoEncontradosOrigem}`);
            console.log(`   ❌ Erros: ${errosTotal}`);
            console.log("=".repeat(60));
        } else if (sincronizadosTotal > 0 || errosTotal > 0) {
            console.log(`✅ Sync Demandas Existentes: ${sincronizadosTotal} atualizadas, ${errosTotal} erros`);
        }

    } catch (error) {
        console.error("❌ Erro ao sincronizar demandas existentes:", error.message);
    }
}

/**
 * Verifica gaps de fiscal-demanda (VERSÃO ALTA PERFORMANCE)
 * Busca fiscal-demandas baseado nas demandas que JÁ EXISTEM NO DESTINO
 * com situação 2, 11 ou 12, garantindo sincronização completa
 */
async function verificarGapsFiscalDemanda(verbose = false) {
    if (verbose) {
        console.log("\n👤 Reconciliando FISCAL-DEMANDA (Fast Mode)...");
    }
    const start = performance.now();

    try {
        // 1. Buscar demandas no DESTINO com situação 2, 11 ou 12
        const demandasDestino = await dbDestino`
            SELECT id 
            FROM fiscalizacao.demandas
            WHERE situacao_id IN (2, 11, 12) AND ativo = true
            ORDER BY id DESC
            LIMIT 5000
        `;

        if (demandasDestino.length === 0) {
            if (verbose) {
                console.log("📭 Nenhuma demanda no destino com situação 2, 11, 12");
            }
            return;
        }

        const demandasIds = demandasDestino.map(d => d.id);

        // 2. Buscar fiscal-demandas da ORIGEM para essas demandas
        const origemRegistros = await dbOrigem`
            SELECT fd.id, fd.demanda_id, fd.usuario_id 
            FROM public.fiscaldemanda fd
            WHERE fd.ativo = true 
              AND fd.demanda_id = ANY(${demandasIds})
            ORDER BY fd.id DESC
        `;

        if (origemRegistros.length === 0) {
            if (verbose) {
                console.log("📭 Nenhum registro de fiscal-demanda na origem para essas demandas");
            }
            return;
        }

        if (verbose) {
            console.log(`📊 ${demandasDestino.length} demandas no destino (situação 2,11,12)`);
            console.log(`📊 ${origemRegistros.length} fiscal-demandas encontradas na origem`);
        }

        // 2. Executar tudo em uma única transação com tabela temporária
        const resultado = await dbDestino.begin(async (tx) => {
            // Criar tabela temporária
            await tx`
                CREATE TEMP TABLE tmp_reconcile_fiscal (
                    demanda_id INT,
                    fiscal_id INT
                ) ON COMMIT DROP
            `;

            // Mapear usuario_id para fiscal_id e converter para INT
            const dadosParaInserir = origemRegistros.map(r => ({
                demanda_id: typeof r.demanda_id === 'string' ? parseInt(r.demanda_id, 10) : r.demanda_id,
                fiscal_id: typeof r.usuario_id === 'string' ? parseInt(r.usuario_id, 10) : r.usuario_id
            }));

            // Inserção em massa na tabela temporária
            await tx`
                INSERT INTO tmp_reconcile_fiscal ${tx(dadosParaInserir, 'demanda_id', 'fiscal_id')}
            `;

            if (verbose) {
                console.log(`🔍 Validando registros (demandas e fiscais devem existir)...`);
            }

            // Sincronização inteligente via SQL - valida que demanda E fiscal existem
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
            if (verbose) {
                console.log(`✅ Fiscal-Demanda: ${resultado.length} registros restaurados em ${duration}s.`);
            } else {
                console.log(`✅ Reconciliação Fiscal-Demanda: ${resultado.length} registros em ${duration}s`);
            }
        } else {
            if (verbose) {
                console.log(`✅ Fiscal-Demanda: Nenhuma inconsistência encontrada (Verificados ${origemRegistros.length} registros em ${duration}s).`);
            }
        }

    } catch (error) {
        console.error("❌ Erro ao verificar gaps de fiscal-demanda:", error.message);
        console.error(error.stack);
    }
}