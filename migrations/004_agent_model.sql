-- Per-agent Pi provider/model (worker reports; dispatcher can inspect)
ALTER TABLE agents ADD COLUMN pi_provider TEXT;
ALTER TABLE agents ADD COLUMN pi_model TEXT;
