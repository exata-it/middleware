// ============================================================
// SINCRONIZAR DEMANDAS BASEADO NO DESTINO
// Busca todas as demandas no destino e atualiza com dados da origem
// ============================================================

import { dbOrigem, dbDestino } from "../src/config/database.js";
import { mapearDemanda } from "../src/mappers/demandaMapper.js";
import { garantirPessoasExistem } from "../src/utils/pessoaSync.js";

console.log("🔄 Sincronizando demandas do DESTINO com dados da ORIGEM...\n");

/**
 * Sincroniza pessoas faltantes em bulk antes do update
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

    console.log(`  🔄 Sincronizando ${idsFaltantes.length} pessoas faltantes...`);
    await garantirPessoasExistem(idsFaltantes);
}

async function sincronizarDemandasFromDestino() {
    try {
        // 1. Buscar todas as demandas no destino com situação 2, 11 ou 12
        console.log("📊 Buscando demandas no destino (situação 2, 11, 12)...");
        const demandasDestino = await dbDestino`
            SELECT id FROM fiscalizacao.demandas
            WHERE situacao_id IN (2, 11, 12)
            ORDER BY id DESC
        `;

        console.log(`✅ Encontradas ${demandasDestino.length} demandas no destino\n`);

        if (demandasDestino.length === 0) {
            console.log("📭 Nenhuma demanda no destino para sincronizar");
            return;
        }

        // 2. Processar em lotes para otimizar
        const LOTE_SIZE = 200;
        const totalLotes = Math.ceil(demandasDestino.length / LOTE_SIZE);
        
        console.log(`📦 Processando ${demandasDestino.length} demandas em ${totalLotes} lotes de até ${LOTE_SIZE}...\n`);

        let sincronizadosTotal = 0;
        let errosTotal = 0;
        let naoEncontradosOrigem = 0;

        for (let i = 0; i < demandasDestino.length; i += LOTE_SIZE) {
            const lote = demandasDestino.slice(i, i + LOTE_SIZE);
            const loteNum = Math.floor(i / LOTE_SIZE) + 1;
            
            console.log(`📋 Lote ${loteNum}/${totalLotes} (${lote.length} demandas)...`);

            // Buscar dados das demandas na ORIGEM
            const idsLote = lote.map(item => item.id);
            const demandasOrigem = await dbOrigem`
                SELECT * FROM public.demanda 
                WHERE id = ANY(${idsLote})
            `;

            if (demandasOrigem.length === 0) {
                console.log(`  ⚠️  Nenhuma demanda encontrada na origem para este lote`);
                naoEncontradosOrigem += lote.length;
                continue;
            }

            // Contar quantas não foram encontradas na origem (converter IDs para número para comparação)
            const idsEncontradosOrigem = new Set(demandasOrigem.map(d => typeof d.id === 'string' ? parseInt(d.id, 10) : d.id));
            const naoEncontrados = lote.filter(d => !idsEncontradosOrigem.has(d.id));
            naoEncontradosOrigem += naoEncontrados.length;

            if (naoEncontrados.length > 0) {
                console.log(`  ⚠️  ${naoEncontrados.length} demandas não encontradas na origem`);
            }

            // Processar demandas em BULK UPDATE (alta performance)
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

                    // Criar arrays para BULK UPDATE
                    const ids = dadosMapeados.map(d => d.id);
                    const situacoes = dadosMapeados.map(d => d.situacao_id);
                    const fiscalizados = dadosMapeados.map(d => d.fiscalizado_id);

                    // UPDATE em massa - criar cases SQL dinamicamente
                    const caseSituacao = ids.map((id, idx) => `WHEN ${id} THEN ${situacoes[idx]}`).join(' ');
                    const caseFiscalizado = ids.map((id, idx) => 
                        fiscalizados[idx] !== null ? `WHEN ${id} THEN ${fiscalizados[idx]}` : `WHEN ${id} THEN NULL`
                    ).join(' ');

                    await dbDestino.unsafe(`
                        UPDATE fiscalizacao.demandas
                        SET 
                            situacao_id = CASE id ${caseSituacao} END,
                            fiscalizado_id = CASE id ${caseFiscalizado} END
                        WHERE id IN (${ids.join(',')})
                    `);

                    sucessoLote = demandasOrigem.length;
                } catch (error) {
                    console.error(`  ❌ Erro no bulk update:`, error.message);
                    errosLote = demandasOrigem.length;
                }
            }

            sincronizadosTotal += sucessoLote;
            errosTotal += errosLote;

            console.log(`  ✅ ${sucessoLote} atualizadas, ${errosLote} erros\n`);
        }

        console.log(`\n${"=".repeat(60)}`);
        console.log(`📊 RESUMO:`);
        console.log(`   ✅ Sincronizadas: ${sincronizadosTotal}`);
        console.log(`   ⚠️  Não encontradas na origem: ${naoEncontradosOrigem}`);
        console.log(`   ❌ Erros: ${errosTotal}`);
        console.log("=".repeat(60));

    } catch (error) {
        console.error("\n❌ Erro fatal ao sincronizar demandas:", error.message);
        console.error(error.stack);
    } finally {
        process.exit(0);
    }
}

// Executar
sincronizarDemandasFromDestino();
