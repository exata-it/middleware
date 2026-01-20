// ============================================================
// UTILITÁRIO: Sincronização Automática de Pessoas
// ============================================================
// Garante que pessoas existam no destino antes de criar demandas

import { dbOrigem, dbDestino } from "../config/database.js";

/**
 * Garante que uma pessoa existe no destino
 * Se não existir, busca da origem e insere automaticamente
 * 
 * @param {number} pessoaId - ID da pessoa
 * @returns {Promise<boolean>} true se a pessoa existe ou foi criada com sucesso
 */
export async function garantirPessoaExiste(pessoaId) {
    if (!pessoaId) {
        return true; // Sem pessoa para validar
    }

    try {
        // Verificar se já existe no destino
        const existe = await dbDestino`
            SELECT id FROM fiscalizacao.pessoas WHERE id = ${pessoaId}
        `;

        if (existe.length > 0) {
            return true; // Já existe
        }

        console.log(`⚠️  Pessoa ${pessoaId} não encontrada no destino, sincronizando...`);

        // Buscar da origem
        const pessoa = await dbOrigem`
            SELECT 
                id, ativo, data_criacao, bairro, complemento, contato, cpfcnpj,
                latitude, logradouro, longitude, nomefantasia, numero, razaosocial,
                telefone, ramoatividade_id, regional_id, usuarioalteracao, email,
                cep, codigo, senha, pushtoken, regionalid, nacionalidade,
                naturalidade, rg, selo_agefis, abordargem_fiscal, atuacao_agefis,
                data_aprovacao_selo, data_solicitacao_selo, licencas_agefis,
                passaporte_sanitario, regras_agefis, taxas_agefis, estado_civil,
                data_nascimento
            FROM public.pessoa
            WHERE id = ${pessoaId}
        `;

        if (pessoa.length === 0) {
            console.error(`❌ Pessoa ${pessoaId} não encontrada na origem!`);
            return false;
        }

        // Inserir no destino
        await inserirPessoa(pessoa[0]);
        console.log(`✅ Pessoa ${pessoaId} sincronizada automaticamente`);
        return true;

    } catch (error) {
        console.error(`❌ Erro ao garantir pessoa ${pessoaId}:`, error.message);
        return false;
    }
}

/**
 * Garante que múltiplas pessoas existem no destino
 * Versão otimizada para lotes
 * 
 * @param {number[]} pessoaIds - Array de IDs de pessoas
 * @returns {Promise<Set<number>>} Set de IDs que existem ou foram criados
 */
export async function garantirPessoasExistem(pessoaIds) {
    if (!pessoaIds || pessoaIds.length === 0) {
        return new Set();
    }

    try {
        // Remover duplicados, nulls e converter strings para números
        const idsLimpos = pessoaIds
            .filter(id => id)
            .map(id => {
                // Remove aspas e converte para número
                const idLimpo = typeof id === 'string' ? id.replace(/["']/g, '') : id;
                return parseInt(idLimpo);
            })
            .filter(id => !isNaN(id)); // Remove valores inválidos
        
        const idsUnicos = [...new Set(idsLimpos)];

        if (idsUnicos.length === 0) {
            return new Set();
        }

        // Verificar quais já existem
        const existentes = await dbDestino`
            SELECT id FROM fiscalizacao.pessoas WHERE id = ANY(${idsUnicos})
        `;

        const existentesSet = new Set(existentes.map(p => p.id));
        const faltantes = idsUnicos.filter(id => !existentesSet.has(id));

        if (faltantes.length === 0) {
            return existentesSet;
        }

        console.log(`⚠️  ${faltantes.length} pessoas não encontradas, sincronizando em lote...`);

        // Buscar faltantes da origem
        const pessoas = await dbOrigem`
            SELECT 
                id, ativo, data_criacao, bairro, complemento, contato, cpfcnpj,
                latitude, logradouro, longitude, nomefantasia, numero, razaosocial,
                telefone, ramoatividade_id, regional_id, usuarioalteracao, email,
                cep, codigo, senha, pushtoken, regionalid, nacionalidade,
                naturalidade, rg, selo_agefis, abordargem_fiscal, atuacao_agefis,
                data_aprovacao_selo, data_solicitacao_selo, licencas_agefis,
                passaporte_sanitario, regras_agefis, taxas_agefis, estado_civil,
                data_nascimento
            FROM public.pessoa
            WHERE id = ANY(${faltantes})
        `;

        // Inserir em bulk (mais rápido)
        if (pessoas.length > 0) {
            try {
                // Preparar dados para bulk insert
                const pessoasParaInserir = pessoas.map(p => ({
                    id: p.id,
                    ativo: p.ativo ?? true,
                    data_criacao: p.data_criacao || new Date(),
                    bairro: p.bairro || null,
                    complemento: p.complemento || null,
                    contato: p.contato || null,
                    cpfcnpj: p.cpfcnpj || null,
                    latitude: p.latitude || null,
                    logradouro: p.logradouro || null,
                    longitude: p.longitude || null,
                    nomefantasia: p.nomefantasia || null,
                    numero: p.numero || null,
                    razaosocial: p.razaosocial || null,
                    telefone: p.telefone || null,
                    ramoatividade_id: p.ramoatividade_id || null,
                    regional_id: p.regional_id || null,
                    usuarioalteracao: p.usuarioalteracao || null,
                    email: p.email || null,
                    cep: p.cep || null,
                    codigo: p.codigo || null,
                    senha: p.senha || null,
                    pushtoken: p.pushtoken || null,
                    regionalid: p.regionalid || null,
                    nacionalidade: p.nacionalidade || null,
                    naturalidade: p.naturalidade || null,
                    rg: p.rg || null,
                    selo_agefis: p.selo_agefis ?? null,
                    abordargem_fiscal: p.abordargem_fiscal || null,
                    atuacao_agefis: p.atuacao_agefis || null,
                    data_aprovacao_selo: p.data_aprovacao_selo || null,
                    data_solicitacao_selo: p.data_solicitacao_selo || null,
                    licencas_agefis: p.licencas_agefis || null,
                    passaporte_sanitario: p.passaporte_sanitario || null,
                    regras_agefis: p.regras_agefis || null,
                    taxas_agefis: p.taxas_agefis || null,
                    estado_civil: p.estado_civil || null,
                    data_nascimento: p.data_nascimento || null,
                    fiscalize_id: p.id
                }));

                // Bulk insert em lotes de 50
                const BATCH_SIZE = 50;
                for (let i = 0; i < pessoasParaInserir.length; i += BATCH_SIZE) {
                    const batch = pessoasParaInserir.slice(i, i + BATCH_SIZE);
                    
                    await dbDestino`
                        INSERT INTO fiscalizacao.pessoas ${
                            dbDestino(batch,
                                'id', 'ativo', 'data_criacao', 'bairro', 'complemento', 'contato', 'cpfcnpj',
                                'latitude', 'logradouro', 'longitude', 'nomefantasia', 'numero', 'razaosocial',
                                'telefone', 'ramoatividade_id', 'regional_id', 'usuarioalteracao', 'email',
                                'cep', 'codigo', 'senha', 'pushtoken', 'regionalid', 'nacionalidade',
                                'naturalidade', 'rg', 'selo_agefis', 'abordargem_fiscal', 'atuacao_agefis',
                                'data_aprovacao_selo', 'data_solicitacao_selo', 'licencas_agefis',
                                'passaporte_sanitario', 'regras_agefis', 'taxas_agefis', 'estado_civil',
                                'data_nascimento', 'fiscalize_id'
                            )
                        }
                        ON CONFLICT (id) DO NOTHING
                    `;
                }

                pessoas.forEach(p => existentesSet.add(p.id));
                console.log(`✅ ${pessoas.length} pessoas sincronizadas`);
            } catch (error) {
                console.error("❌ Erro no bulk insert de pessoas:", error.message);
                // Fallback: inserir uma por uma
                let sucesso = 0;
                for (const pessoa of pessoas) {
                    try {
                        await inserirPessoa(pessoa);
                        existentesSet.add(pessoa.id);
                        sucesso++;
                    } catch (error) {
                        console.error(`❌ Erro ao inserir pessoa ${pessoa.id}:`, error.message);
                    }
                }
                if (sucesso > 0) {
                    console.log(`✅ ${sucesso} pessoas sincronizadas (fallback)`);
                }
            }
        }

        return existentesSet;

    } catch (error) {
        console.error("❌ Erro ao garantir pessoas em lote:", error.message);
        return new Set();
    }
}

/**
 * Insere uma pessoa no destino
 * @private
 */
async function inserirPessoa(pessoa) {
    await dbDestino`
        INSERT INTO fiscalizacao.pessoas (
            id, ativo, data_criacao, bairro, complemento, contato, cpfcnpj,
            latitude, logradouro, longitude, nomefantasia, numero, razaosocial,
            telefone, ramoatividade_id, regional_id, usuarioalteracao, email,
            cep, codigo, senha, pushtoken, regionalid, nacionalidade,
            naturalidade, rg, selo_agefis, abordargem_fiscal, atuacao_agefis,
            data_aprovacao_selo, data_solicitacao_selo, licencas_agefis,
            passaporte_sanitario, regras_agefis, taxas_agefis, estado_civil,
            data_nascimento, fiscalize_id
        ) VALUES (
            ${pessoa.id},
            ${pessoa.ativo ?? true},
            ${pessoa.data_criacao || new Date()},
            ${pessoa.bairro || null},
            ${pessoa.complemento || null},
            ${pessoa.contato || null},
            ${pessoa.cpfcnpj || null},
            ${pessoa.latitude || null},
            ${pessoa.logradouro || null},
            ${pessoa.longitude || null},
            ${pessoa.nomefantasia || null},
            ${pessoa.numero || null},
            ${pessoa.razaosocial || null},
            ${pessoa.telefone || null},
            ${pessoa.ramoatividade_id || null},
            ${pessoa.regional_id || null},
            ${pessoa.usuarioalteracao || null},
            ${pessoa.email || null},
            ${pessoa.cep || null},
            ${pessoa.codigo || null},
            ${pessoa.senha || null},
            ${pessoa.pushtoken || null},
            ${pessoa.regionalid || null},
            ${pessoa.nacionalidade || null},
            ${pessoa.naturalidade || null},
            ${pessoa.rg || null},
            ${pessoa.selo_agefis ?? null},
            ${pessoa.abordargem_fiscal || null},
            ${pessoa.atuacao_agefis || null},
            ${pessoa.data_aprovacao_selo || null},
            ${pessoa.data_solicitacao_selo || null},
            ${pessoa.licencas_agefis || null},
            ${pessoa.passaporte_sanitario || null},
            ${pessoa.regras_agefis || null},
            ${pessoa.taxas_agefis || null},
            ${pessoa.estado_civil || null},
            ${pessoa.data_nascimento || null},
            ${pessoa.id}
        )
        ON CONFLICT (id) DO NOTHING
    `;
}
