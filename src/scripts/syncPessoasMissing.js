// ============================================================
// SCRIPT: Sincronizar Pessoas Ausentes
// ============================================================
// Resolve erros de FK sincronizando pessoas que existem na 
// origem mas não no destino

import { dbOrigem, dbDestino } from "../config/database.js";
import fs from "fs";
import path from "path";

/**
 * Lê os erros de sincronização e extrai IDs de pessoas ausentes
 */
async function extrairPessoasAusentes() {
    try {
        const errosPath = path.join(process.cwd(), "erros_sincronizacao.json");
        
        if (!fs.existsSync(errosPath)) {
            console.log("⚠️  Arquivo erros_sincronizacao.json não encontrado");
            return new Set();
        }

        const dados = JSON.parse(fs.readFileSync(errosPath, "utf-8"));
        const pessoasIds = new Set();

        // Procurar por erros de FK relacionados a fiscalizado_id
        if (dados.constraint_errors) {
            for (const erro of dados.constraint_errors) {
                if (
                    erro.constraint_name === "demandas_fiscalizado_id_fkey" &&
                    erro.valor_problematico
                ) {
                    pessoasIds.add(parseInt(erro.valor_problematico));
                }
            }
        }

        console.log(`📋 Encontrados ${pessoasIds.size} IDs de pessoas ausentes`);
        return pessoasIds;
    } catch (error) {
        console.error("❌ Erro ao extrair pessoas ausentes:", error);
        throw error;
    }
}

/**
 * Busca pessoas na origem por IDs
 */
async function buscarPessoasOrigem(ids) {
    if (ids.size === 0) {
        return [];
    }

    const idsArray = Array.from(ids);
    console.log(`🔍 Buscando ${idsArray.length} pessoas na origem...`);

    const pessoas = await dbOrigem`
        SELECT 
            id,
            ativo,
            data_criacao,
            bairro,
            complemento,
            contato,
            cpfcnpj,
            latitude,
            logradouro,
            longitude,
            nomefantasia,
            numero,
            razaosocial,
            telefone,
            ramoatividade_id,
            regional_id,
            usuarioalteracao,
            email,
            cep,
            codigo,
            senha,
            pushtoken,
            regionalid,
            nacionalidade,
            naturalidade,
            rg,
            selo_agefis,
            abordargem_fiscal,
            atuacao_agefis,
            data_aprovacao_selo,
            data_solicitacao_selo,
            licencas_agefis,
            passaporte_sanitario,
            regras_agefis,
            taxas_agefis,
            estado_civil,
            data_nascimento
        FROM public.pessoa
        WHERE id = ANY(${idsArray})
    `;

    console.log(`✅ Encontradas ${pessoas.length} pessoas na origem`);
    return pessoas;
}

/**
 * Verifica quais pessoas já existem no destino
 */
async function verificarPessoasExistentes(ids) {
    if (ids.size === 0) {
        return new Set();
    }

    const idsArray = Array.from(ids);
    
    const existentes = await dbDestino`
        SELECT id 
        FROM fiscalizacao.pessoas 
        WHERE id = ANY(${idsArray})
    `;

    return new Set(existentes.map(p => p.id));
}

/**
 * Insere uma pessoa no destino
 */
async function inserirPessoa(pessoa) {
    try {
        await dbDestino`
            INSERT INTO fiscalizacao.pessoas (
                id,
                ativo,
                data_criacao,
                bairro,
                complemento,
                contato,
                cpfcnpj,
                latitude,
                logradouro,
                longitude,
                nomefantasia,
                numero,
                razaosocial,
                telefone,
                ramoatividade_id,
                regional_id,
                usuarioalteracao,
                email,
                cep,
                codigo,
                senha,
                pushtoken,
                regionalid,
                nacionalidade,
                naturalidade,
                rg,
                selo_agefis,
                abordargem_fiscal,
                atuacao_agefis,
                data_aprovacao_selo,
                data_solicitacao_selo,
                licencas_agefis,
                passaporte_sanitario,
                regras_agefis,
                taxas_agefis,
                estado_civil,
                data_nascimento,
                fiscalize_id
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
        `;

        return true;
    } catch (error) {
        console.error(`❌ Erro ao inserir pessoa ${pessoa.id}:`, error.message);
        return false;
    }
}

/**
 * Sincroniza pessoas ausentes
 */
async function sincronizarPessoasAusentes() {
    console.log("\n🔄 Iniciando sincronização de pessoas ausentes...\n");

    try {
        // 1. Extrair IDs das pessoas ausentes dos erros
        const pessoasIds = await extrairPessoasAusentes();

        if (pessoasIds.size === 0) {
            console.log("✅ Nenhuma pessoa ausente encontrada nos erros");
            return { sucesso: 0, erros: 0, total: 0 };
        }

        // 2. Verificar quais já existem no destino
        const existentes = await verificarPessoasExistentes(pessoasIds);
        const faltantes = new Set([...pessoasIds].filter(id => !existentes.has(id)));

        console.log(`📊 Status:`);
        console.log(`   Total de IDs: ${pessoasIds.size}`);
        console.log(`   Já existentes: ${existentes.size}`);
        console.log(`   Faltantes: ${faltantes.size}\n`);

        if (faltantes.size === 0) {
            console.log("✅ Todas as pessoas já estão sincronizadas");
            return { sucesso: 0, erros: 0, total: 0 };
        }

        // 3. Buscar pessoas na origem
        const pessoas = await buscarPessoasOrigem(faltantes);

        if (pessoas.length === 0) {
            console.log("⚠️  Nenhuma pessoa encontrada na origem");
            return { sucesso: 0, erros: faltantes.size, total: faltantes.size };
        }

        // 4. Inserir pessoas no destino
        console.log(`📝 Inserindo ${pessoas.length} pessoas no destino...\n`);
        
        let sucesso = 0;
        let erros = 0;

        for (const pessoa of pessoas) {
            const resultado = await inserirPessoa(pessoa);
            if (resultado) {
                sucesso++;
                console.log(`✅ Pessoa ${pessoa.id} sincronizada`);
            } else {
                erros++;
            }
        }

        console.log("\n" + "=".repeat(60));
        console.log("📊 RESULTADO DA SINCRONIZAÇÃO");
        console.log("=".repeat(60));
        console.log(`✅ Sucessos: ${sucesso}`);
        console.log(`❌ Erros: ${erros}`);
        console.log(`📋 Total: ${pessoas.length}`);
        console.log("=".repeat(60) + "\n");

        return { sucesso, erros, total: pessoas.length };

    } catch (error) {
        console.error("\n❌ Erro na sincronização:", error);
        throw error;
    }
}

// Executar se chamado diretamente
if (import.meta.url === `file://${process.argv[1]}`) {
    sincronizarPessoasAusentes()
        .then(() => {
            console.log("\n✅ Sincronização concluída!");
            process.exit(0);
        })
        .catch((error) => {
            console.error("\n❌ Falha na sincronização:", error);
            process.exit(1);
        });
}

export { sincronizarPessoasAusentes, extrairPessoasAusentes };
