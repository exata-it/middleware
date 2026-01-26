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
 * Verifica gaps de operações
 * Sincroniza operações da origem (public.operacao) para destino (fiscalizacao.operacao)
 * @param {boolean} verbose - Se true, mostra logs detalhados
 */
async function verificarGapsOperacoes(verbose = false) {
    if (verbose) {
        console.log("\n🎯 Reconciliando OPERAÇÕES...");
    }

    try {
        // Buscar todas as operações ativas da origem
        const origemOperacoes = await dbOrigem`
            SELECT * FROM public.operacao 
            WHERE ativo = true
            ORDER BY id DESC
        `;

        if (origemOperacoes.length === 0) {
            if (verbose) {
                console.log("📭 Nenhuma operação na origem para verificar");
            }
            return;
        }

        if (verbose) {
            console.log(`📊 Encontradas ${origemOperacoes.length} operações na origem`);
        }

        // Buscar operações já existentes no destino
        const destinoOperacoes = await dbDestino`
            SELECT id FROM fiscalizacao.operacao
        `;

        const setDestino = new Set(destinoOperacoes.map((op) => op.id));
        const faltantes = origemOperacoes.filter((op) => !setDestino.has(op.id));

        if (faltantes.length > 0) {
            if (verbose) {
                console.warn(`⚠️ Encontradas ${faltantes.length} operações faltando no destino!`);
            }

            let sincronizados = 0;
            let erros = 0;

            // Inserir em bulk usando transação
            try {
                await dbDestino.begin(async sql => {
                    for (const op of faltantes) {
                        await sql`
                            INSERT INTO fiscalizacao.operacao (
                                id, ativo, data_criacao, usuarioalteracao, descricao, 
                                nome, requeracompanhamento, grupodemanda_id, 
                                pessoageradora_id, unidadeoperacional_id
                            )
                            VALUES (
                                ${op.id},
                                ${op.ativo ?? true},
                                ${op.datacriacao || new Date()},
                                ${op.usuarioalteracao || null},
                                ${op.descricao || null},
                                ${op.nome || ''},
                                ${op.requeracompanhamento ?? false},
                                ${op.grupodemanda_id || null},
                                ${op.pessoageradora_id || null},
                                ${op.unidadeoperacional_id || null}
                            )
                            ON CONFLICT (id) DO NOTHING
                        `;
                    }
                });
                sincronizados = faltantes.length;

                if (verbose) {
                    console.log(`✅ ${sincronizados} operações sincronizadas com sucesso`);
                }
            } catch (error) {
                console.error(`❌ Erro ao sincronizar operações:`, error.message);
                erros = faltantes.length;
            }

            if (!verbose && sincronizados > 0) {
                console.log(`✅ Operações: ${sincronizados} sincronizadas`);
            }
        } else {
            if (verbose) {
                console.log("✅ Operações: nenhuma inconsistência encontrada.");
            }
        }
    } catch (error) {
        console.error("❌ Erro ao verificar gaps de operações:", error.message);
    }
}

/**
 * Verifica gaps de fiscal-operação
 * Sincroniza relações fiscal-operação da origem (public.fiscaloperacao) para destino (fiscalizacao.fiscaloperacao)
 * @param {boolean} verbose - Se true, mostra logs detalhados
 */
async function verificarGapsFiscalOperacao(verbose = false) {
    if (verbose) {
        console.log("\n👥 Reconciliando FISCAL-OPERAÇÃO...");
    }

    try {
        // Buscar todos os registros ativos da origem
        const origemRegistros = await dbOrigem`
            SELECT * FROM public.fiscaloperacao 
            WHERE ativo = true
            ORDER BY id DESC
        `;

        if (origemRegistros.length === 0) {
            if (verbose) {
                console.log("📭 Nenhum registro de fiscal-operação na origem");
            }
            return;
        }

        if (verbose) {
            console.log(`📊 Encontrados ${origemRegistros.length} registros na origem`);
        }

        // Buscar registros já existentes no destino
        const destinoRegistros = await dbDestino`
            SELECT id FROM fiscalizacao.fiscaloperacao
        `;

        const setDestino = new Set(destinoRegistros.map((r) => r.id));
        const faltantes = origemRegistros.filter((r) => !setDestino.has(r.id));

        if (faltantes.length > 0) {
            if (verbose) {
                console.warn(`⚠️ Encontrados ${faltantes.length} registros de fiscal-operação faltando!`);
            }

            let sincronizados = 0;
            let erros = 0;

            // Inserir em bulk usando tabela temporária
            try {
                // Criar tabela temporária
                await dbDestino`
                    CREATE TEMP TABLE IF NOT EXISTS temp_fiscaloperacao_sync (
                        id INT PRIMARY KEY,
                        ativo BOOLEAN,
                        data_criacao TIMESTAMP,
                        datainicio TIMESTAMP,
                        datafim TIMESTAMP,
                        usuario_id INT,
                        operacao_id INT
                    )
                `;

                // Limpar tabela temporária
                await dbDestino`TRUNCATE temp_fiscaloperacao_sync`;

                // Bulk insert na temp table (ignorar usuarioalteracao pois é string na origem)
                await dbDestino`
                    INSERT INTO temp_fiscaloperacao_sync ${dbDestino(
                        faltantes.map(r => ({
                            id: parseInt(r.id, 10),
                            ativo: r.ativo ?? true,
                            data_criacao: r.data_criacao || new Date(),
                            datainicio: r.datainicio,
                            datafim: r.datafim,
                            usuario_id: parseInt(r.usuario_id, 10),
                            operacao_id: parseInt(r.operacao_id, 10)
                        }))
                    )}
                `;

                // Inserir da temp para a real filtrando FKs válidas
                const result = await dbDestino`
                    INSERT INTO fiscalizacao.fiscaloperacao (
                        id, ativo, data_criacao,
                        datainicio, datafim, usuario_id, operacao_id
                    )
                    SELECT 
                        t.id, t.ativo, t.data_criacao,
                        t.datainicio, t.datafim, t.usuario_id, t.operacao_id
                    FROM temp_fiscaloperacao_sync t
                    WHERE EXISTS (SELECT 1 FROM seguranca.usuarios WHERE id = t.usuario_id)
                      AND EXISTS (SELECT 1 FROM fiscalizacao.operacao WHERE id = t.operacao_id)
                    ON CONFLICT (id) DO NOTHING
                `;

                sincronizados = result.count || faltantes.length;

                if (verbose) {
                    console.log(`✅ ${sincronizados} registros de fiscal-operação sincronizados`);
                }
            } catch (error) {
                console.error(`❌ Erro ao sincronizar fiscal-operação:`, error.message);
                erros = faltantes.length;
            }

            if (!verbose && sincronizados > 0) {
                console.log(`✅ Fiscal-Operação: ${sincronizados} sincronizados`);
            }
        } else {
            if (verbose) {
                console.log("✅ Fiscal-Operação: nenhuma inconsistência encontrada.");
            }
        }
    } catch (error) {
        console.error("❌ Erro ao verificar gaps de fiscal-operação:", error.message);
    }
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
            case "public.operacao":
                resultado = await dbOrigem`SELECT * FROM public.operacao WHERE id = ${id}`;
                break;
            case "public.fiscaloperacao":
                resultado = await dbOrigem`SELECT * FROM public.fiscaloperacao WHERE id = ${id}`;
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

    await verificarGapsOperacoes(verbose);
    await verificarGapsFiscalOperacao(verbose);
    await sincronizarDemandasOrigem(verbose);
    await verificarGapsFiscalDemanda(verbose);
}

/**
 * Sincroniza TODAS as demandas com situação 2, 11, 12 da origem para o destino
 * Faz UPSERT: insere se não existir, atualiza se existir
 * @param {boolean} verbose - Se true, mostra logs detalhados
 */
async function sincronizarDemandasOrigem(verbose = false) {
    if (verbose) {
        console.log("\n📋 Sincronizando DEMANDAS da origem (situação 2, 11, 12)...");
    }

    try {
        // Buscar TODAS as demandas com situação 2, 11, 12 na origem
        const demandasOrigem = await dbOrigem`
            SELECT * FROM public.demanda 
            WHERE situacao IN (2, 11, 12)
            ORDER BY id DESC LIMIT 20000
        `;

        if (demandasOrigem.length === 0) {
            if (verbose) {
                console.log("📭 Nenhuma demanda na origem com situação 2, 11, 12");
            }
            return;
        }

        if (verbose) {
            console.log(`📊 Encontradas ${demandasOrigem.length} demandas na origem`);
        }

        // Processar em lotes
        const LOTE_SIZE = 100;
        const totalLotes = Math.ceil(demandasOrigem.length / LOTE_SIZE);
        
        if (verbose) {
            console.log(`📦 Processando em ${totalLotes} lotes de até ${LOTE_SIZE} demandas...\n`);
        }

        let sincronizadosTotal = 0;
        let errosTotal = 0;

        for (let i = 0; i < demandasOrigem.length; i += LOTE_SIZE) {
            const lote = demandasOrigem.slice(i, i + LOTE_SIZE);
            const loteNum = Math.floor(i / LOTE_SIZE) + 1;
            
            if (verbose) {
                console.log(`📋 Lote ${loteNum}/${totalLotes} (${lote.length} demandas)...`);
            }

            // PRÉ-SINCRONIZAR pessoas faltantes para evitar FK errors
            const fiscalizadosIds = lote
                .map(d => d.fiscalizado_id)
                .filter(id => id != null)
                .map(id => typeof id === 'string' ? parseInt(id, 10) : id);
            
            await sincronizarPessoasFaltantes(fiscalizadosIds);

            // Mapear todas as demandas do lote
            const dadosMapeados = lote.map(d => {
                const mapped = mapearDemanda(d, null);
                return {
                    id: parseInt(mapped.id, 10),
                    situacao_id: parseInt(mapped.situacao_id, 10),
                    fiscalizado_id: mapped.fiscalizado_id ? parseInt(mapped.fiscalizado_id, 10) : null,
                    operacao_id: mapped.operacao_id ? parseInt(mapped.operacao_id, 10) : null,
                    classificacao: mapped.classificacao,
                    fiscalizado_demanda: mapped.fiscalizado_demanda,
                    fiscalizado_cpf_cnpj: mapped.fiscalizado_cpf_cnpj,
                    fiscalizado_nome: mapped.fiscalizado_nome,
                    fiscalizado_logradouro: mapped.fiscalizado_logradouro,
                    fiscalizado_numero: mapped.fiscalizado_numero,
                    fiscalizado_complemento: mapped.fiscalizado_complemento,
                    fiscalizado_bairro: mapped.fiscalizado_bairro,
                    fiscalizado_municipio: mapped.fiscalizado_municipio,
                    fiscalizado_uf: mapped.fiscalizado_uf,
                    fiscalizado_lat: mapped.fiscalizado_lat,
                    fiscalizado_lng: mapped.fiscalizado_lng,
                    data_criacao: mapped.data_criacao,
                    data_realizacao: mapped.data_realizacao,
                    ativo: mapped.ativo,
                    tipo_rota: mapped.tipo_rota,
                    grupo_ocorrencia_id: mapped.grupo_ocorrencia_id
                };
            });

            try {
                // UPSERT em bulk usando uma única query com múltiplos valores
                // Primeiro, criar uma tabela temporária
                await dbDestino`
                    CREATE TEMP TABLE IF NOT EXISTS temp_demandas_sync (
                        id INT PRIMARY KEY,
                        situacao_id INT,
                        fiscalizado_id INT,
                        operacao_id INT,
                        classificacao TEXT,
                        fiscalizado_demanda TEXT,
                        fiscalizado_cpf_cnpj TEXT,
                        fiscalizado_nome TEXT,
                        fiscalizado_logradouro TEXT,
                        fiscalizado_numero TEXT,
                        fiscalizado_complemento TEXT,
                        fiscalizado_bairro TEXT,
                        fiscalizado_municipio TEXT,
                        fiscalizado_uf TEXT,
                        fiscalizado_lat FLOAT,
                        fiscalizado_lng FLOAT,
                        data_criacao TIMESTAMP,
                        data_realizacao TIMESTAMP,
                        ativo BOOLEAN,
                        tipo_rota TEXT,
                        grupo_ocorrencia_id INT
                    )
                `;

                // Limpar tabela temporária
                await dbDestino`TRUNCATE temp_demandas_sync`;

                // Inserir todos os dados na temp table de uma vez
                await dbDestino`
                    INSERT INTO temp_demandas_sync ${dbDestino(
                        dadosMapeados.map(d => ({
                            id: d.id,
                            situacao_id: d.situacao_id,
                            fiscalizado_id: d.fiscalizado_id,
                            operacao_id: d.operacao_id,
                            classificacao: d.classificacao,
                            fiscalizado_demanda: d.fiscalizado_demanda,
                            fiscalizado_cpf_cnpj: d.fiscalizado_cpf_cnpj,
                            fiscalizado_nome: d.fiscalizado_nome,
                            fiscalizado_logradouro: d.fiscalizado_logradouro,
                            fiscalizado_numero: d.fiscalizado_numero,
                            fiscalizado_complemento: d.fiscalizado_complemento,
                            fiscalizado_bairro: d.fiscalizado_bairro,
                            fiscalizado_municipio: d.fiscalizado_municipio,
                            fiscalizado_uf: d.fiscalizado_uf,
                            fiscalizado_lat: d.fiscalizado_lat,
                            fiscalizado_lng: d.fiscalizado_lng,
                            data_criacao: d.data_criacao,
                            data_realizacao: d.data_realizacao,
                            ativo: d.ativo,
                            tipo_rota: d.tipo_rota,
                            grupo_ocorrencia_id: d.grupo_ocorrencia_id
                        }))
                    )}
                `;

                // Fazer UPSERT de uma vez da temp table para a real
                await dbDestino`
                    INSERT INTO fiscalizacao.demandas (
                        id, situacao_id, motivo_id, fiscal_id, fiscalizado_id, operacao_id,
                        fiscalizado_demanda, fiscalizado_cpf_cnpj, fiscalizado_nome,
                        fiscalizado_logradouro, fiscalizado_numero, fiscalizado_complemento,
                        fiscalizado_bairro, fiscalizado_municipio, fiscalizado_uf,
                        fiscalizado_lat, fiscalizado_lng, classificacao,
                        data_criacao, data_realizacao, ativo, tipo_rota, grupo_ocorrencia_id
                    )
                    SELECT 
                        id, situacao_id, NULL, NULL, fiscalizado_id, operacao_id,
                        fiscalizado_demanda, fiscalizado_cpf_cnpj, fiscalizado_nome,
                        fiscalizado_logradouro, fiscalizado_numero, fiscalizado_complemento,
                        fiscalizado_bairro, fiscalizado_municipio, fiscalizado_uf,
                        fiscalizado_lat, fiscalizado_lng, classificacao::fiscalizacao.classificacao_os,
                        data_criacao, data_realizacao, ativo, tipo_rota, grupo_ocorrencia_id
                    FROM temp_demandas_sync
                    ON CONFLICT (id) DO UPDATE SET
                        situacao_id = EXCLUDED.situacao_id,
                        fiscalizado_id = EXCLUDED.fiscalizado_id,
                        operacao_id = EXCLUDED.operacao_id,
                        fiscalizado_nome = EXCLUDED.fiscalizado_nome,
                        fiscalizado_cpf_cnpj = EXCLUDED.fiscalizado_cpf_cnpj,
                        fiscalizado_demanda = EXCLUDED.fiscalizado_demanda,
                        fiscalizado_logradouro = EXCLUDED.fiscalizado_logradouro,
                        fiscalizado_numero = EXCLUDED.fiscalizado_numero,
                        fiscalizado_complemento = EXCLUDED.fiscalizado_complemento,
                        fiscalizado_bairro = EXCLUDED.fiscalizado_bairro,
                        fiscalizado_lat = EXCLUDED.fiscalizado_lat,
                        fiscalizado_lng = EXCLUDED.fiscalizado_lng,
                        classificacao = EXCLUDED.classificacao,
                        data_realizacao = EXCLUDED.data_realizacao,
                        ativo = EXCLUDED.ativo
                `;

                sincronizadosTotal += dadosMapeados.length;

                if (verbose) {
                    console.log(`  ✅ Lote processado\n`);
                }
            } catch (error) {
                console.error(`  ❌ Erro no lote:`, error.message);
                errosTotal += lote.length;
            }
        }

        if (verbose) {
            console.log(`\n${"=".repeat(60)}`);
            console.log(`📊 SINCRONIZAÇÃO DEMANDAS ORIGEM:`);
            console.log(`   ✅ Sincronizadas: ${sincronizadosTotal}`);
            console.log(`   ❌ Erros: ${errosTotal}`);
            console.log("=".repeat(60));
        } else if (sincronizadosTotal > 0 || errosTotal > 0) {
            console.log(`✅ Demandas: ${sincronizadosTotal} sincronizadas, ${errosTotal} erros`);
        }
    } catch (error) {
        console.error("❌ Erro ao sincronizar demandas da origem:", error.message);
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

        // Mapear usuario_id para fiscal_id e converter para INT
        const dadosParaInserir = origemRegistros.map(r => ({
            demanda_id: typeof r.demanda_id === 'string' ? parseInt(r.demanda_id, 10) : r.demanda_id,
            fiscal_id: typeof r.usuario_id === 'string' ? parseInt(r.usuario_id, 10) : r.usuario_id
        }));

        if (verbose) {
            console.log(`🔍 Validando registros (demandas e fiscais devem existir)...`);
        }

        // Processar em lotes menores para evitar "insufficient data left in message"
        const CHUNK_SIZE = 100;
        let totalInseridos = 0;

        for (let i = 0; i < dadosParaInserir.length; i += CHUNK_SIZE) {
            const chunk = dadosParaInserir.slice(i, i + CHUNK_SIZE);
            
            // Extrair arrays de IDs
            const demandasIds = chunk.map(d => d.demanda_id);
            const fiscaisIds = chunk.map(d => d.fiscal_id);

            // Sincronização via UNNEST ao invés de temp table
            const resultado = await dbDestino`
                INSERT INTO fiscalizacao.demandas_fiscais (demanda_id, fiscal_id)
                SELECT 
                    v.demanda_id, 
                    v.fiscal_id
                FROM (
                    SELECT 
                        UNNEST(${demandasIds}::int[]) as demanda_id,
                        UNNEST(${fiscaisIds}::int[]) as fiscal_id
                ) v
                INNER JOIN fiscalizacao.demandas d ON d.id = v.demanda_id
                INNER JOIN fiscalizacao.fiscais f ON f.id = v.fiscal_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM fiscalizacao.demandas_fiscais df 
                    WHERE df.demanda_id = v.demanda_id AND df.fiscal_id = v.fiscal_id
                )
                ON CONFLICT (demanda_id, fiscal_id) DO NOTHING
                RETURNING demanda_id, fiscal_id
            `;
            
            totalInseridos += resultado.length;
        }

        const end = performance.now();
        const duration = ((end - start) / 1000).toFixed(2);

        if (totalInseridos > 0) {
            if (verbose) {
                console.log(`✅ Fiscal-Demanda: ${totalInseridos} registros restaurados em ${duration}s.`);
            } else {
                console.log(`✅ Reconciliação Fiscal-Demanda: ${totalInseridos} registros em ${duration}s`);
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