#!/usr/bin/env bun

// ============================================================
// HEALTH CHECK - Verifica saúde do middleware
// ============================================================

import postgres from "postgres";
import { CONFIG } from "./src/config/index.js";

async function healthCheck() {
  const startTime = Date.now();
  const results = {
    status: "healthy",
    timestamp: new Date().toISOString(),
    checks: {},
    responseTime: 0,
  };

  try {
    // ============================================================
    // 1. TESTE DE CONEXÃO COM BANCO DE ORIGEM
    // ============================================================
    try {
      const dbOrigem = postgres(CONFIG.origem.url);
      const testOrigem = await dbOrigem`SELECT NOW() as timestamp`;
      
      results.checks.origem = {
        status: "ok",
        timestamp: testOrigem[0].timestamp,
      };
      
      await dbOrigem.end();
    } catch (error) {
      results.checks.origem = {
        status: "error",
        error: error.message,
      };
      results.status = "unhealthy";
    }

    // ============================================================
    // 2. TESTE DE CONEXÃO COM BANCO DE DESTINO
    // ============================================================
    try {
      const dbDestino = postgres(CONFIG.destino.url);
      const testDestino = await dbDestino`SELECT NOW() as timestamp`;
      
      results.checks.destino = {
        status: "ok",
        timestamp: testDestino[0].timestamp,
      };
      
      await dbDestino.end();
    } catch (error) {
      results.checks.destino = {
        status: "error",
        error: error.message,
      };
      results.status = "unhealthy";
    }

    // ============================================================
    // 3. CÁLCULO DO TEMPO DE RESPOSTA
    // ============================================================
    results.responseTime = `${Date.now() - startTime}ms`;

    // ============================================================
    // 4. OUTPUT
    // ============================================================
    console.log(JSON.stringify(results, null, 2));

    // Exit code baseado no status
    if (results.status === "healthy") {
      process.exit(0);
    } else {
      process.exit(1);
    }
  } catch (error) {
    results.status = "unhealthy";
    results.error = error.message;
    results.responseTime = `${Date.now() - startTime}ms`;

    console.log(JSON.stringify(results, null, 2));
    process.exit(1);
  }
}

// Timeout de 5 segundos para não travar
setTimeout(() => {
  console.log(
    JSON.stringify({
      status: "unhealthy",
      error: "Health check timeout",
      responseTime: "5000ms",
    })
  );
  process.exit(1);
}, 5000);

healthCheck();
