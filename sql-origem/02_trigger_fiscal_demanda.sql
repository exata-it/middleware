-- ============================================================
-- FISCALDEMANDA
-- ============================================================

-- 1. Remove o trigger antigo, se existir
DROP TRIGGER IF EXISTS trigger_sync_fiscaldemanda ON public.fiscaldemanda;

-- 2. Cria o novo trigger
CREATE TRIGGER trigger_sync_fiscaldemanda
AFTER INSERT OR UPDATE OR DELETE ON public.fiscaldemanda
FOR EACH ROW 
EXECUTE FUNCTION public.notificar_sync();

-- 3. Adiciona um comentário
COMMENT ON TRIGGER trigger_sync_fiscaldemanda ON public.fiscaldemanda IS 
'Sincroniza tabela fiscaldemanda com Fiscalize. Payload: { id, table, event_type }';