// ============================================================
// MAPEAMENTO: demanda (origem) -> demandas (destino)
// ============================================================

import { dbDestino } from "../config/database.js";

/**
 * Mapeia os campos da tabela `demanda` (origem) para `demandas` (destino Prisma)
 * Apenas campos que existem no schema do destino são mapeados
 * 
 * @param {object} dataOrigem - Dados da tabela demanda (origem)
 * @param {object} dadosPessoa - Dados da pessoa/fiscalizado (opcional, se já tiver sido buscado)
 * @returns {object} Dados mapeados para tabela demandas (destino)
 */
export function mapearDemanda(dataOrigem, dadosPessoa = null) {
    // Função helper para normalizar IDs
    const normalizarId = (valor) => {
        if (valor === null || valor === undefined || valor === '') return null;
        if (typeof valor === 'string') return parseInt(valor, 10);
        return valor;
    };

    // Converte IDs para número (a origem pode retornar como string)
    const id = normalizarId(dataOrigem.id);
    const grupo_ocorrencia_id = normalizarId(dataOrigem.grupodemanda_id);
    const fiscalizado_id = normalizarId(dataOrigem.fiscalizado_id);
    const situacao_id = normalizarId(dataOrigem.situacao);
    const operacao_id = normalizarId(dataOrigem.operacao_id);

    // Dados do fiscalizado
    let fiscalizado_nome = "";
    let fiscalizado_cpf_cnpj = "";

    if (dadosPessoa) {
        fiscalizado_nome = dadosPessoa.nomefantasia || dadosPessoa.razaosocial || "";
        fiscalizado_cpf_cnpj = dadosPessoa.cpfcnpj || "";
    }



    return {
        // Campo ID
        id: id,

        // Situação e motivo
        situacao_id: situacao_id,
        motivo_id: null, // Não existe na origem
        fiscal_id: null, // Será preenchido depois se necessário
        fiscalizado_id: fiscalizado_id,
        operacao_id: operacao_id, // ID da operação (se for demanda de operação)

        // Identificação da demanda
        fiscalizado_demanda: dataOrigem.descricao || dataOrigem.protocolo || `DEMANDA-${dataOrigem.id}`,

        // Dados do fiscalizado
        fiscalizado_cpf_cnpj: fiscalizado_cpf_cnpj,
        fiscalizado_nome: fiscalizado_nome,

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
        classificacao: operacao_id ? "operacao" : (situacao_id === 12 ? "direta" : situacao_id === 2 || situacao_id === 7 ? "ordinaria" : "N/A"),

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

/**
 * Busca dados da pessoa no DESTINO para complementar a demanda
 * @param {number} pessoaId - ID da pessoa
 * @returns {Promise<object|null>} Dados da pessoa ou null
 */
export async function buscarDadosPessoa(pessoaId) {
    if (!pessoaId) return null;
    
    try {
        const resultado = await dbDestino`
            SELECT id, nomefantasia, razaosocial, cpfcnpj
            FROM fiscalizacao.pessoas
            WHERE id = ${pessoaId}
        `;
        
        return resultado[0] || null;
    } catch (error) {
        console.error(`❌ Erro ao buscar pessoa ID ${pessoaId}:`, error.message);
        return null;
    }
}
