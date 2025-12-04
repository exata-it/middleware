# ============================================================
# MIDDLEWARE DE SINCRONIZAÇÃO - DOCKERFILE
# ============================================================
# Imagem base: Bun (runtime JavaScript rápido)
# ============================================================

FROM oven/bun:1-alpine AS base

WORKDIR /app

# ============================================================
# STAGE: DEPENDENCIES
# ============================================================
FROM base AS dependencies

# Copiar arquivos de dependências
COPY package.json bun.lockb* ./

# Instalar dependências de produção
RUN bun install --frozen-lockfile --production

# ============================================================
# STAGE: PRODUCTION
# ============================================================
FROM base AS production

# Copiar dependências instaladas
COPY --from=dependencies /app/node_modules ./node_modules

# Copiar código fonte
COPY . .

# Remover arquivos desnecessários
RUN rm -rf \
    .env.example \
    .gitignore \
    tsconfig.json \
    README.md \
    *.sql \
    dump-*.sql \
    erros_sincronizacao.json \
    bkp-lts/ \
    python/ \
    sql-origem/



# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD bun --version || exit 1

# Comando de execução
CMD ["bun", "run", "start"]
