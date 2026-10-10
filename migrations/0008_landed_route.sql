ALTER TABLE copy_attempts ADD COLUMN landed_route TEXT;

UPDATE copy_attempts
SET landed_route = (
    SELECT route_name FROM copy_variants
    WHERE source_signature = copy_attempts.source_signature
      AND local_signature = copy_attempts.local_signature
)
WHERE landed_slot IS NOT NULL;
