import { dbOrigem, dbDestino, fecharConexoes } from "../config/database.js";

async function syncFiscalDemanda() {
    console.log("🚀 Iniciando Sincronização de Alta Performance...");

    try {
        // 1. Busca os registros mais recentes da origem (do final para o começo)
        console.log(`📥 Buscando registros mais recentes da origem...`);
        const novosRegistros = await dbOrigem`
            SELECT id, demanda_id, usuario_id 
            FROM public.fiscaldemanda 
            WHERE ativo = true
            ORDER BY id DESC
            LIMIT 5000
        `;

        if (novosRegistros.length === 0) {
            console.log("✅ Nenhum registro na origem.");
            return;
        }

        console.log(`📊 Encontrados ${novosRegistros.length} registros na origem (ID ${novosRegistros[0].id} até ${novosRegistros[novosRegistros.length - 1].id})`);

        // 2. Executar tudo em uma única transação
        const resultado = await dbDestino.begin(async (tx) => {
            // Criar tabela temporária
            await tx`
                CREATE TEMP TABLE tmp_sync_fiscal (
                    demanda_id INT,
                    fiscal_id INT
                ) ON COMMIT DROP
            `;

            // Mapear usuario_id para fiscal_id
            const dadosParaInserir = novosRegistros.map(r => ({
                demanda_id: r.demanda_id,
                fiscal_id: r.usuario_id
            }));

            // Inserção em massa na tabela temporária
            await tx`
                INSERT INTO tmp_sync_fiscal ${tx(dadosParaInserir, 'demanda_id', 'fiscal_id')}
            `;

            console.log(`🔍 Validando registros (demandas e fiscais devem existir)...`);

            // Sincronização inteligente via SQL
            // Só insere se: demanda existe, fiscal existe, e relação ainda não existe
            return await tx`
                INSERT INTO fiscalizacao.demandas_fiscais (demanda_id, fiscal_id)
                SELECT 
                    t.demanda_id, 
                    t.fiscal_id
                FROM tmp_sync_fiscal t
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

        if (resultado.length > 0) {
            console.log(`✅ Sincronizados ${resultado.length} novos vínculos com sucesso.`);
        } else {
            console.log(`✅ Nenhum vínculo novo (todos já existem ou demandas/fiscais não encontrados no destino).`);
        }

    } catch (error) {
        console.error("❌ Erro na sincronização:", error.message);
        console.error(error.stack);
    } finally {
        // Se for rodar constante, talvez não queira fechar a conexão aqui
        // await fecharConexoes(); 
    }
}

// Para atualizações constantes (ex: a cada 30 segundos)
const RUN_INTERVAL = 15 * 1000; 

(async function loop() {
    await syncFiscalDemanda();
    console.log(`⏳ Aguardando ${RUN_INTERVAL/1000}s até próxima execução...`);
    setTimeout(loop, RUN_INTERVAL);
})();