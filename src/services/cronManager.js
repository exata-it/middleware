// ============================================================
// GERENCIADOR DE CRON JOBS
// ============================================================

import cron from "node-cron";
import { CONFIG } from "../config/index.js";
import { verificarGaps } from "./reconciliation.js";

let cronJob = null;

/**
 * Inicia o cron job de reconciliação
 */
export function iniciarCronReconciliacao() {
    console.log(`⏰ Agendando reconciliação com cron: "${CONFIG.cronReconciliacao}"`);

    cronJob = cron.schedule(
        CONFIG.cronReconciliacao,
        async () => {
            console.log(`\n⏱️  [${new Date().toISOString()}] Executando reconciliação agendada...`);
            await verificarGaps(false); // false = modo silencioso (apenas erros)
        },
        {
            runOnInit: false, // Não executar na inicialização
            timezone: CONFIG.timezone,
        }
    );

    console.log("✅ Cron job de reconciliação ativo");
}

/**
 * Para o cron job de reconciliação
 */
export function pararCronReconciliacao() {
    if (cronJob) {
        cronJob.stop();
        cronJob.destroy();
        console.log("🛑 Cron job de reconciliação parado");
    }
}
