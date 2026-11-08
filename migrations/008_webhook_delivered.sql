-- Marks when a submission's completion webhook was delivered, so redelivered jobs do not
-- fire it twice. Additive and nullable: older engine versions ignore the column.
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS webhook_delivered_at TIMESTAMPTZ;
