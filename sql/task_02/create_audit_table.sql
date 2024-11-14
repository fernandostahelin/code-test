DROP TABLE IF EXISTS audit_log;

-- Audit table to track all changes in monitored columns
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id INTEGER NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    modified_by VARCHAR(100) NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for better query performance
CREATE INDEX IF NOT EXISTS idx_audit_table_record
ON audit_log (table_name, record_id);
