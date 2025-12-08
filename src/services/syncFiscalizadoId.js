// ============================================================
// SINCRONIZAÇÃO DE FISCALIZADO_ID
// ============================================================

import { dbOrigem, dbDestino } from "../config/database.js";

/**
 * Sincroniza o fiscalizado_id de todas as demandas que já existem no destino
 */
export async function sincronizarFiscalizadoId() {
    console.log("\n🔄 Iniciando sincronização de fiscalizado_id...");

    try {
        // 1. Busca amostra de IDs do destino para verificar se estão preservados
        console.log("\n🔍 Verificando IDs...");
        const amostraDestino = await dbDestino`
            SELECT id FROM fiscalizacao.demandas ORDER BY id LIMIT 10
        `;
        console.log("   IDs no destino:", amostraDestino.map(d => d.id).join(", "));

        const amostraOrigem = await dbOrigem`
            SELECT id FROM public.demanda ORDER BY id LIMIT 10
        `;
        console.log("   IDs na origem:", amostraOrigem.map(d => d.id).join(", "));

        // 2. Busca demandas com fiscalizado_id na origem
        const demandasComFiscalizado = await dbOrigem`
            SELECT id, fiscalizado_id 
            FROM public.demanda 
            WHERE fiscalizado_id IS NOT NULL 
            AND ativo = true
            LIMIT 10
        `;

        console.log(`\n📊 Amostra de demandas com fiscalizado_id na origem (10 primeiras):`);
        demandasComFiscalizado.forEach(d => {
            console.log(`   ID: ${d.id}, fiscalizado_id: ${d.fiscalizado_id}`);
        });

        // 3. Verifica se esses IDs existem no destino
        if (demandasComFiscalizado.length > 0) {
            const ids = demandasComFiscalizado.map(d => d.id);
            
            console.log(`\n🔍 Verificando se os IDs ${ids.slice(0, 3).join(", ")}... existem no destino:`);
            
            for (const id of ids.slice(0, 3)) {
                const existe = await dbDestino`
                    SELECT id, fiscalizado_id, fiscalizado_nome 
                    FROM fiscalizacao.demandas 
                    WHERE id = ${id}
                `;
                
                if (existe.length > 0) {
                    console.log(`   ✅ ID ${id} existe no destino - fiscalizado_id: ${existe[0].fiscalizado_id || 'NULL'}`);
                } else {
                    console.log(`   ❌ ID ${id} NÃO existe no destino`);
                }
            }
        }

        // 4. Agora sincroniza de verdade
        console.log("\n🔄 Iniciando sincronização...");
        
        const todasDemandasOrigem = await dbOrigem`
            SELECT id, fiscalizado_id 
            FROM public.demanda 
            WHERE fiscalizado_id IS NOT NULL 
            AND ativo = true
        `;

        console.log(`📊 Total de demandas com fiscalizado_id na origem: ${todasDemandasOrigem.length}`);

        // 5. Busca todas as pessoas do destino
        const pessoasDestino = await dbDestino`
            SELECT id, nomefantasia, razaosocial, cpfcnpj
            FROM fiscalizacao.pessoas
        `;
        
        const mapaPessoas = new Map(
            pessoasDestino.map(p => [p.id, p])
        );
        console.log(`📊 Pessoas no destino: ${pessoasDestino.length}`);

        // 6. Atualiza uma por uma
        let atualizadas = 0;
        let erros = 0;
        let semPessoaDestino = 0;
        let demandaNaoExiste = 0;

        console.log(`\n🔄 Processando ${todasDemandasOrigem.length} demandas...`);

        for (const demandaOrigem of todasDemandasOrigem) {
            try {
                // Verifica se a pessoa existe
                const pessoa = mapaPessoas.get(demandaOrigem.fiscalizado_id);
                if (!pessoa) {
                    semPessoaDestino++;
                    continue;
                }

                // Determina nome e CPF/CNPJ
                const nomeFiscalizado = pessoa.nomefantasia || pessoa.razaosocial || '';
                const cpfCnpj = pessoa.cpfcnpj || '';

                // Atualiza
                const resultado = await dbDestino`
                    UPDATE fiscalizacao.demandas 
                    SET 
                        fiscalizado_id = ${demandaOrigem.fiscalizado_id},
                        fiscalizado_nome = ${nomeFiscalizado},
                        fiscalizado_cpf_cnpj = ${cpfCnpj}
                    WHERE id = ${demandaOrigem.id}
                `;

                if (resultado.count > 0) {
                    atualizadas++;
                } else {
                    demandaNaoExiste++;
                }
                
                if (atualizadas % 100 === 0) {
                    console.log(`   📝 Processadas: ${atualizadas}/${todasDemandasOrigem.length}`);
                }
            } catch (error) {
                console.error(`   ❌ Erro demanda ID ${demandaOrigem.id}:`, error.message);
                erros++;
            }
        }

        console.log("\n📊 Resumo da sincronização:");
        console.log(`   ✅ Atualizadas: ${atualizadas}`);
        console.log(`   ⚠️ Sem pessoa no destino: ${semPessoaDestino}`);
        console.log(`   ⚠️ Demanda não existe no destino: ${demandaNaoExiste}`);
        console.log(`   ❌ Erros: ${erros}`);
        console.log(`   📊 Total processadas: ${todasDemandasOrigem.length}`);

        // 7. Verificação final
        console.log("\n🔍 Verificação final - 15 registros com fiscalizado_id:");
        const verificacao = await dbDestino`
            SELECT id, fiscalizado_id, fiscalizado_nome, fiscalizado_cpf_cnpj
            FROM fiscalizacao.demandas 
            WHERE fiscalizado_id IS NOT NULL 
            AND ativo = true
            ORDER BY id DESC
            LIMIT 15
        `;

        if (verificacao.length > 0) {
            console.log("\n   ID      | Fisc ID | Nome              | CPF/CNPJ");
            console.log("   " + "─".repeat(70));
            verificacao.forEach(v => {
                const id = String(v.id).padEnd(7);
                const fiscId = String(v.fiscalizado_id).padEnd(7);
                const nome = (v.fiscalizado_nome || '').substring(0, 16).padEnd(16);
                const cpf = (v.fiscalizado_cpf_cnpj || '').substring(0, 18);
                console.log(`   ${id} | ${fiscId} | ${nome} | ${cpf}`);
            });
        } else {
            console.log("   ⚠️ Nenhum registro com fiscalizado_id encontrado!");
        }

    } catch (error) {
        console.error("❌ Erro ao sincronizar fiscalizado_id:", error.message);
        throw error;
    }
}

/**
 * Relatório de demandas sem fiscalizado_id
 * Útil para diagnóstico
 */
export async function relatorioFiscalizadoId() {
    console.log("\n📋 Gerando relatório de fiscalizado_id...");

    try {
        // Demandas no destino sem fiscalizado_id
        const semFiscalizadoDestino = await dbDestino`
            SELECT COUNT(*) as total
            FROM fiscalizacao.demandas 
            WHERE fiscalizado_id IS NULL AND ativo = true
        `;

        // Demandas na origem com fiscalizado_id
        const comFiscalizadoOrigem = await dbOrigem`
            SELECT COUNT(*) as total
            FROM public.demanda 
            WHERE fiscalizado_id IS NOT NULL AND ativo = true
        `;

        // Total de demandas
        const totalDestino = await dbDestino`
            SELECT COUNT(*) as total
            FROM fiscalizacao.demandas 
            WHERE ativo = true
        `;

        console.log("\n📊 Estatísticas:");
        console.log(`   Total de demandas no destino: ${totalDestino[0].total}`);
        console.log(`   Demandas sem fiscalizado_id no destino: ${semFiscalizadoDestino[0].total}`);
        console.log(`   Demandas com fiscalizado_id na origem: ${comFiscalizadoOrigem[0].total}`);

    } catch (error) {
        console.error("❌ Erro ao gerar relatório:", error.message);
    }
}
