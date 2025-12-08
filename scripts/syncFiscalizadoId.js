// ============================================================
// SCRIPT: Sincronização de fiscalizado_id
// ============================================================
// Execute com: bun run scripts/syncFiscalizadoId.js
// ============================================================

import { sincronizarFiscalizadoId, relatorioFiscalizadoId } from "../src/services/syncFiscalizadoId.js";
import { dbDestino } from "../src/config/database.js";

async function main() {
    console.log("╔════════════════════════════════════════════════════════╗");
    console.log("║  SINCRONIZAÇÃO DE FISCALIZADO_ID                       ║");
    console.log("╚════════════════════════════════════════════════════════╝");

    try {
        // Primeiro, gera um relatório
        await relatorioFiscalizadoId();

        // Depois sincroniza
        await sincronizarFiscalizadoId();

        console.log("\n✅ Sincronização concluída com sucesso!");
        process.exit(0);
    } catch (error) {
        console.error("\n❌ Erro na sincronização:", error);
        process.exit(1);
    }
}

main();
