#!/usr/bin/env bun
// ============================================================
// SCRIPT: Executar Reconciliação Manualmente
// ============================================================

import { verificarGaps } from "./src/services/reconciliation.js";

console.log(`
╔══════════════════════════════════════════════════════════╗
║  RECONCILIAÇÃO MANUAL                                   ║
║  Verifica e sincroniza gaps entre origem e destino      ║
╚══════════════════════════════════════════════════════════╝
`);

try {
    await verificarGaps(true); // true = modo verbose (manual)
    console.log("\n✅ Reconciliação concluída com sucesso!");
    process.exit(0);
} catch (error) {
    console.error("\n❌ Erro na reconciliação:", error);
    process.exit(1);
}
