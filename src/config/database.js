// ============================================================
// CONEXÕES DE BANCO DE DADOS
// ============================================================

import postgres from "postgres";
import { CONFIG } from "./index.js";

// Conexão com banco de origem usando postgres.js (suporta LISTEN/NOTIFY)
export const dbOrigem = postgres(CONFIG.origem.url, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10
});

// Conexão com banco de destino usando postgres.js com buffer maior
// Fix para "insufficient data left in message" em bulk operations
export const dbDestino = postgres(CONFIG.destino.url, {
    max: 10,
    idle_timeout: 20,
    connect_timeout: 10,
    max_lifetime: 60 * 30, // 30 minutos
    prepare: false, // Evita prepared statements que podem causar buffer issues
});

/**
 * Fecha todas as conexões de banco de dados
 */
export async function fecharConexoes() {
    try {
        await dbOrigem.end();
        await dbDestino.end();
        console.log("✅ Conexões fechadas com sucesso");
    } catch (error) {
        console.error("❌ Erro ao fechar conexões:", error.message);
        throw error;
    }
}
