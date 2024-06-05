CREATE DATABASE datasource;

\c datasource;

CREATE TABLE IF NOT EXISTS regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(50) UNIQUE NOT NULL,
    region_local_open TIME NOT NULL,
    region_local_close TIME NOT NULL,
    region_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS industries (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(255) NOT NULL,
    industry_sector VARCHAR(255) NOT NULL,
    industry_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_industry UNIQUE (industry_name, industry_sector)
);

CREATE TABLE IF NOT EXISTS sicindustries (
    sic_id INT PRIMARY KEY,
    sic_industry VARCHAR(255) NOT NULL,
    sic_sector VARCHAR(255) NOT NULL,
    sicindustry_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_sicindustry UNIQUE (sic_industry, sic_sector)
);

CREATE TABLE IF NOT EXISTS exchanges (
    exchange_id SERIAL PRIMARY KEY,
    exchange_region_id INT NOT NULL,
    exchange_name VARCHAR(100) UNIQUE NOT NULL,
    company_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_exchange_region_id
        FOREIGN KEY(exchange_region_id)
        REFERENCES regions(region_id)
);

CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    company_exchange_id INT NOT NULL,
    company_industry_id INT,
    company_sic_id INT,
    company_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    company_name VARCHAR(255) NOT NULL,
    company_ticket VARCHAR(10) NOT NULL,
    company_is_delisted BOOLEAN NOT NULL,
    company_category VARCHAR(255),
    company_currency VARCHAR(10),
    company_location VARCHAR(255),
    CONSTRAINT fk_company_region
        FOREIGN KEY(company_exchange_id)
        REFERENCES exchanges(exchange_id),
    CONSTRAINT fk_company_industry_id
        FOREIGN KEY(company_industry_id)
        REFERENCES industries(industry_id),
    CONSTRAINT fk_company_sic_id
        FOREIGN KEY(company_sic_id)
        REFERENCES sicindustries(sic_id),
    CONSTRAINT unique_company_delisted UNIQUE (company_ticket, company_is_delisted)
);

CREATE INDEX idx_company_time_stamp ON companies(company_update_time_stamp);
CREATE INDEX idx_company_exchange_id ON companies(company_exchange_id);
CREATE INDEX idx_exchange_region_id ON exchanges(exchange_region_id);
CREATE INDEX idx_company_industry_id ON companies(company_industry_id);
CREATE INDEX idx_company_sic_id ON companies(company_sic_id);