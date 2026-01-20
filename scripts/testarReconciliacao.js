// ============================================================
// TESTE DE RECONCILIAÇÃO OTIMIZADA
// ============================================================

import { verificarGaps } from "../src/services/reconciliation.js";

console.log("🔍 Iniciando teste de reconciliação...\n");

const startTotal = performance.now();

try {
    await verificarGaps(true); // verbose = true para ver detalhes
    
    const endTotal = performance.now();
    const durationTotal = ((endTotal - startTotal) / 1000).toFixed(2);
    
    console.log(`\n${"=".repeat(60)}`);
    console.log(`⏱️  TEMPO TOTAL: ${durationTotal}s`);
    console.log("=".repeat(60));
} catch (error) {
    console.error("\n❌ Erro no teste:", error.message);
    console.error(error.stack);
}

process.exit(0);
