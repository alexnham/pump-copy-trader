ALTER TABLE copy_attempts ADD COLUMN landed_slot INTEGER CHECK (landed_slot >= 0);
