CREATE TABLE IF NOT EXISTS imported_supplier (
    supplier_id smallint PRIMARY KEY,
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
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by varchar(50) NOT NULL
);
