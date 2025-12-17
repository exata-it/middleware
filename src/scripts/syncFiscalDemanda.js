import { dbOrigem, dbDestino, fecharConexoes } from "../config/database.js";

async function syncFiscalDemanda() {
    console.log("🚀 Iniciando Sincronização de Alta Performance...");

    try {
        // 1. Pega o último ID ou Data sincronizada no destino para busca incremental
        // Isso evita carregar a origem inteira toda vez
        const lastSync = await dbDestino`
            SELECT COALESCE(MAX(id_origem), 0) as last_id 
            FROM fiscalizacao.demandas_fiscais
        `;
        const lastId = lastSync[0].last_id;

        // 2. Busca apenas o que é NOVO na origem
        console.log(`📥 Buscando novos registros a partir do ID: ${lastId}`);
        const novosRegistros = await dbOrigem`
            SELECT id, demanda_id, usuario_id 
            FROM public.fiscaldemanda 
            WHERE ativo = true AND id > ${lastId}
            ORDER BY id ASC
            LIMIT 5000
        `;

        if (novosRegistros.length === 0) {
            console.log("✅ Tudo sincronizado. Sem novos registros.");
            return;
        }

        // 3. Criação de Tabela Temporária no Destino para validação ultra-rápida
        // Isso evita múltiplos SELECTs e Sets no Node.js
        await dbDestino`CREATE TEMP TABLE tmp_sync_fiscal (
            id_origem INT,
            demanda_id INT,
            fiscal_id INT
        ) ON COMMIT DROP`;

        // 4. Inserção em massa na tabela temporária
        await dbDestino`
            INSERT INTO tmp_sync_fiscal ${dbDestino(novosRegistros, 'id', 'demanda_id', 'usuario_id')}
        `;

        // 5. O PULO DO GATO: Sync via SQL puro
        // Validamos existência de demanda e fiscal e ausência de duplicata em uma única transação
        const resultado = await dbDestino`
            INSERT INTO fiscalizacao.demandas_fiscais (demanda_id, fiscal_id, id_origem)
            SELECT 
                t.demanda_id, 
                t.fiscal_id, 
                t.id_origem
            FROM tmp_sync_fiscal t
            INNER JOIN fiscalizacao.demandas d ON d.id = t.demanda_id
            INNER JOIN fiscalizacao.fiscais f ON f.id = t.fiscal_id
            WHERE NOT EXISTS (
                SELECT 1 FROM fiscalizacao.demandas_fiscais df 
                WHERE df.demanda_id = t.demanda_id AND df.fiscal_id = t.fiscal_id
            )
            ON CONFLICT (demanda_id, fiscal_id) DO NOTHING
            RETURNING id;
        `;

        console.log(`✅ Sincronizados ${resultado.length} novos vínculos com sucesso.`);

    } catch (error) {
        console.error("❌ Erro na sincronização:", error.message);
    } finally {
        // Se for rodar constante, talvez não queira fechar a conexão aqui
        // await fecharConexoes(); 
    }
}

// Para atualizações constantes (ex: a cada 30 segundos)
const RUN_INTERVAL = 15 * 1000; 

(async function loop() {
    await syncFiscalDemanda();
    console.log(`Sleeping for ${RUN_INTERVAL/1000}s...`);
    setTimeout(loop, RUN_INTERVAL);
})();