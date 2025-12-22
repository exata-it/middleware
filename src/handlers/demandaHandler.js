// ============================================================
// HANDLER: Sincronização de Demandas
// ============================================================

import { dbDestino } from "../config/database.js";
import { mapearDemanda, buscarDadosPessoa } from "../mappers/demandaMapper.js";
import { salvarErro } from "../utils/errorLogger.js";

/**
 * Sincroniza uma demanda do banco origem para o destino
 * 
 * @param {string} event_type - Tipo de evento: INSERT, UPDATE, DELETE
 * @param {object} data - Dados da demanda da origem
 */
export async function sincronizarDemanda(event_type, data) {
    // Busca dados da pessoa se houver fiscalizado_id
    let dadosPessoa = null;
    if (data.fiscalizado_id) {
        dadosPessoa = await buscarDadosPessoa(data.fiscalizado_id);
    }

    const demandaMapeada = mapearDemanda(data, dadosPessoa);

    try {
        console.log(`\n🔍 DEBUG ${event_type} ID ${data.id}:`, JSON.stringify(demandaMapeada, null, 2));

        if (event_type === "INSERT") {
            await inserirDemanda(demandaMapeada);
            console.log(`✅ INSERT/UPSERT demanda ID ${data.id}`);
        }

        if (event_type === "UPDATE") {
            await atualizarDemanda(demandaMapeada);
        }

        if (event_type === "DELETE") {
            await deletarDemanda(data.id);
            console.log(`🗑️ SOFT DELETE demanda ID ${data.id}`);
        }
    } catch (error) {
        console.error(`❌ Erro ao sincronizar demanda ID ${data.id}:`, error.message);

        // Verificar se é erro de constraint (FK)
        if (error.message.includes("violates foreign key constraint") || error.message.includes("violates")) {
            salvarErro(data.id, "demandas", "foreign_key_constraint", error.message, demandaMapeada);
        }

        throw error;
    }
}

/**
 * Insere uma nova demanda (com UPSERT)
 */
async function inserirDemanda(demanda) {
    await dbDestino`
        INSERT INTO fiscalizacao.demandas (
            id, situacao_id, motivo_id, fiscal_id, fiscalizado_id,
            fiscalizado_demanda, fiscalizado_cpf_cnpj, fiscalizado_nome,
            fiscalizado_logradouro, fiscalizado_numero, fiscalizado_complemento,
            fiscalizado_bairro, fiscalizado_municipio, fiscalizado_uf,
            fiscalizado_lat, fiscalizado_lng, classificacao,
            data_criacao, data_realizacao, ativo, tipo_rota, grupo_ocorrencia_id
        )
        VALUES (
            ${demanda.id},
            ${demanda.situacao_id},
            ${demanda.motivo_id},
            ${demanda.fiscal_id},
            ${demanda.fiscalizado_id},
            ${demanda.fiscalizado_demanda},
            ${demanda.fiscalizado_cpf_cnpj},
            ${demanda.fiscalizado_nome},
            ${demanda.fiscalizado_logradouro},
            ${demanda.fiscalizado_numero},
            ${demanda.fiscalizado_complemento},
            ${demanda.fiscalizado_bairro},
            ${demanda.fiscalizado_municipio},
            ${demanda.fiscalizado_uf},
            ${demanda.fiscalizado_lat},
            ${demanda.fiscalizado_lng},
            ${demanda.classificacao},
            ${demanda.data_criacao},
            ${demanda.data_realizacao},
            ${demanda.ativo},
            ${demanda.tipo_rota},
            ${demanda.grupo_ocorrencia_id}
        )
        ON CONFLICT (id) DO UPDATE SET
            situacao_id = EXCLUDED.situacao_id,
            fiscalizado_id = EXCLUDED.fiscalizado_id,
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
}

/**
 * Atualiza uma demanda existente (ou insere se não existir)
 */
async function atualizarDemanda(demanda) {
    // Primeiro verifica se o registro existe
    const existe = await dbDestino`SELECT id FROM fiscalizacao.demandas WHERE id = ${demanda.id}`;

    if (existe.length === 0) {
        console.warn(`⚠️ Registro não encontrado no destino ID ${demanda.id}, fazendo INSERT...`);
        await inserirDemanda(demanda);
        console.log(`✅ INSERT (por UPDATE) demanda ID ${demanda.id}`);
    } else {
        const resultado = await dbDestino`
            UPDATE fiscalizacao.demandas SET
                situacao_id = ${demanda.situacao_id},
                fiscalizado_id = ${demanda.fiscalizado_id},
                fiscalizado_nome = ${demanda.fiscalizado_nome},
                fiscalizado_cpf_cnpj = ${demanda.fiscalizado_cpf_cnpj},
                fiscalizado_demanda = ${demanda.fiscalizado_demanda},
                fiscalizado_logradouro = ${demanda.fiscalizado_logradouro},
                fiscalizado_numero = ${demanda.fiscalizado_numero},
                fiscalizado_complemento = ${demanda.fiscalizado_complemento},
                fiscalizado_bairro = ${demanda.fiscalizado_bairro},
                fiscalizado_lat = ${demanda.fiscalizado_lat},
                fiscalizado_lng = ${demanda.fiscalizado_lng},
                classificacao = ${demanda.classificacao},
                data_realizacao = ${demanda.data_realizacao},
                ativo = ${demanda.ativo}
            WHERE id = ${demanda.id}
        `;
        console.log(`✅ UPDATE demanda ID ${demanda.id}, registros afetados: ${resultado.count}`);
    }
}

/**
 * Soft delete de uma demanda
 */
async function deletarDemanda(id) {
    await dbDestino`
        UPDATE fiscalizacao.demandas SET ativo = false WHERE id = ${id}
    `;
}
