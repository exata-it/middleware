#!/usr/bin/env bun
// ============================================================
// SCRIPT PRINCIPAL: Resolver Erros de FK de Pessoas
// ============================================================

import { sincronizarPessoasAusentes } from "./syncPessoasMissing.js";

console.log(`
╔══════════════════════════════════════════════════════════╗
║  SINCRONIZAÇÃO DE PESSOAS AUSENTES                      ║
║  Resolve erros de FK em demandas.fiscalizado_id         ║
╚══════════════════════════════════════════════════════════╝
`);

try {
    const resultado = await sincronizarPessoasAusentes();
    
    if (resultado.sucesso > 0) {
        console.log("\n🎉 Pessoas sincronizadas com sucesso!");
        console.log("💡 Agora você pode tentar reprocessar as demandas que falharam");
    }
    
    process.exit(resultado.erros > 0 ? 1 : 0);
} catch (error) {
    console.error("\n💥 Erro fatal:", error);
    process.exit(1);
}
