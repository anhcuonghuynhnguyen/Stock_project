CREATE DATABASE Datasource;

CREATE TABLE IF NOT EXISTS Regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL,
    region_local_open TIME NOT NULL,
    region_local_close TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS Industries (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(100) NOT NULL,
    industry_sector VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS SicIndustries (
    sic_id INT PRIMARY KEY,
    sic_industry VARCHAR(100) NOT NULL,
    sic_sector VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Exchanges (
    exchange_id SERIAL PRIMARY KEY,
    exchange_region_id INT NOT NULL,
    exchange_name VARCHAR(10) NOT NULL,
    CONSTRAINT fk_exchange_region_id
        FOREIGN KEY(exchange_region_id)
        REFERENCES Regions(region_id)
);

CREATE TABLE IF NOT EXISTS Companies (
    company_id SERIAL PRIMARY KEY,
    company_exchange_id INT NOT NULL,
    company_industry_id INT,
    company_sic_id INT,
    company_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    company_name VARCHAR(100) NOT NULL,
    company_ticket VARCHAR(10) NOT NULL,
    company_is_delisted BOOLEAN NOT NULL,
    company_category VARCHAR(100) NOT NULL,
    company_currency VARCHAR(10) NOT NULL,
    company_location VARCHAR(100) NOT NULL,
    CONSTRAINT fk_company_region
        FOREIGN KEY(company_exchange_id)
        REFERENCES Exchanges(exchange_id),
    CONSTRAINT fk_company_industry_id
        FOREIGN KEY(company_industry_id)
        REFERENCES Industries(industry_id),
    CONSTRAINT fk_company_sic_id
        FOREIGN KEY(company_sic_id)
        REFERENCES SicIndustries(sic_id)
);
