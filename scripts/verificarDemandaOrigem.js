// ============================================================
// Verificar se demanda existe com ID próximo ou em range específico
// ============================================================

import { dbOrigem, dbDestino } from "../src/config/database.js";

const DEMANDA_ID = 7832608;

console.log(`\n🔍 Buscando demanda ${DEMANDA_ID} ou próximas na origem...\n`);

try {
    // Buscar demandas próximas ao ID
    console.log("📊 Demandas próximas na ORIGEM (±100):");
    const proximasOrigem = await dbOrigem`
        SELECT id, situacao, ativo, protocolo, descricao
        FROM public.demanda 
        WHERE id BETWEEN ${DEMANDA_ID - 100} AND ${DEMANDA_ID + 100}
        ORDER BY id
        LIMIT 20
    `;
    
    if (proximasOrigem.length > 0) {
        console.log(`Encontradas ${proximasOrigem.length} demandas:`);
        proximasOrigem.forEach(d => {
            const marker = d.id === DEMANDA_ID ? ' <<<< ESTA!' : '';
            console.log(`  ID: ${d.id}, situacao: ${d.situacao}, ativo: ${d.ativo}${marker}`);
        });
    } else {
        console.log("❌ Nenhuma demanda encontrada nesse range");
    }

    // Verificar se existe na origem com ativo=false
    console.log("\n📊 Verificando demanda INATIVA na origem:");
    const demandaInativa = await dbOrigem`
        SELECT id, situacao, ativo, protocolo 
        FROM public.demanda 
        WHERE id = ${DEMANDA_ID}
    `;
    
    if (demandaInativa.length > 0) {
        console.log("✅ Demanda encontrada (pode estar inativa):");
        console.log(demandaInativa[0]);
        
        // Buscar fiscal-demandas (incluindo inativas)
        const fiscalDemandasInativas = await dbOrigem`
            SELECT id, demanda_id, usuario_id, ativo 
            FROM public.fiscaldemanda 
            WHERE demanda_id = ${DEMANDA_ID}
        `;
        console.log(`\nFiscal-demandas (incluindo inativas): ${fiscalDemandasInativas.length}`);
        fiscalDemandasInativas.forEach(fd => {
            console.log(`  ID: ${fd.id}, usuario_id: ${fd.usuario_id}, ativo: ${fd.ativo}`);
        });
    } else {
        console.log("❌ Demanda NÃO existe na origem (nem inativa)");
    }

    // Verificar o último ID de demanda na origem
    console.log("\n📊 Último ID de demanda na origem:");
    const ultimaDemanda = await dbOrigem`
        SELECT MAX(id) as max_id FROM public.demanda
    `;
    console.log(`Último ID: ${ultimaDemanda[0]?.max_id}`);
    
    if (ultimaDemanda[0]?.max_id && DEMANDA_ID > ultimaDemanda[0].max_id) {
        console.log(`\n⚠️  DEMANDA ${DEMANDA_ID} é MAIOR que o último ID na origem!`);
        console.log(`   Isso significa que foi criada DIRETAMENTE NO DESTINO.`);
    }

} catch (error) {
    console.error("\n❌ Erro:", error.message);
}

process.exit(0);
