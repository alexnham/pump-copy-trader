ALTER TABLE source_transactions ADD COLUMN input_asset TEXT;
ALTER TABLE source_transactions ADD COLUMN output_asset TEXT;
ALTER TABLE source_transactions ADD COLUMN observed_input_amount TEXT;
ALTER TABLE source_transactions ADD COLUMN observed_output_amount TEXT;
ALTER TABLE copy_attempts ADD COLUMN quoted_output TEXT;
ALTER TABLE copy_attempts ADD COLUMN route_latency_ms INTEGER;
