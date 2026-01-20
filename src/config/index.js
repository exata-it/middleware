// ============================================================
// CONFIGURAÇÃO CENTRAL
// ============================================================

// Debug: verificar se as variáveis estão sendo carregadas
console.log("🔧 Variáveis de ambiente:");
console.log("   DATABASE_ORIGEM_URL:", process.env.DATABASE_ORIGEM_URL ? "✅ Definida" : "❌ Não definida");
console.log("   DATABASE_DESTINO_URL:", process.env.DATABASE_DESTINO_URL ? "✅ Definida" : "❌ Não definida");

export const CONFIG = {
    origem: {
        url: process.env.DATABASE_ORIGEM_URL || "postgres://postgres:postgres@localhost:5432/fiscalize",
    },
    destino: {
        url: process.env.DATABASE_DESTINO_URL || "postgres://postgres:postgres@localhost:5432/agefis",
    },
    canal: "sync_channel",
    reconnectDelay: 5000, // 5 segundos para reconexão
    cronReconciliacao: process.env.CRON_RECONCILIACAO || "*/5 * * * *", // A cada 10 minutos
    timezone: "America/Fortaleza",
};
