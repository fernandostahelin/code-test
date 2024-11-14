DROP TABLE IF EXISTS target_employees;

-- Target employees table with tracking columns
CREATE TABLE IF NOT EXISTS target_employees (
    -- New primary key different from source
    target_employee_id SERIAL PRIMARY KEY,

    -- Original columns from source
    -- Original ID kept as unique constraint
    employee_id INTEGER NOT NULL UNIQUE,
    last_name VARCHAR(100) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    title VARCHAR(100),                  -- Monitored for changes
    address TEXT,                        -- Monitored for changes
    city VARCHAR(100),                   -- Monitored for changes
    postal_code VARCHAR(20),             -- Monitored for changes
    country VARCHAR(100),                -- Monitored for changes
    reports_to INTEGER,                  -- Monitored for changes

    -- Audit columns
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100) NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_target_employees_employee_id
ON target_employees (employee_id);

CREATE INDEX IF NOT EXISTS idx_target_employees_modified
ON target_employees (modified_at);

-- Foreign key for reports_to (self-referencing)
ALTER TABLE target_employees
ADD CONSTRAINT fk_reports_to
FOREIGN KEY (reports_to)
REFERENCES target_employees (employee_id)
ON DELETE SET NULL;
