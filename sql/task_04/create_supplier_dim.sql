CREATE TABLE IF NOT EXISTS supplier_dimension (
    supplier_key SERIAL PRIMARY KEY,  -- Surrogate key
    supplier_id smallint NOT NULL,    -- Natural/business key
    company_name varchar(40) NOT NULL,
    contact_name varchar(30),
    contact_title varchar(30),
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postal_code varchar(10),
    country varchar(15),
    phone varchar(24),
    fax varchar(24),
    homepage text,
    effective_date timestamp NOT NULL,
    end_date timestamp,
    is_current boolean NOT NULL,
    created_by varchar(50) NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by varchar(50) NOT NULL,
    modified_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);