SELECT 
    c.company_name,
    c.company_ticket,
    c.company_is_delisted,
    c.company_category,
    c.company_currency,
    c.company_location,
    e.exchange_name as company_exchange_name,
    r.region_name as company_region_name,
    i.industry_name as company_industry_name,
    i.industry_sector as company_industry_sector,
    s.sic_industry as company_sic_industry,
    s.sic_sector as company_sic_sector
FROM 
    companies c
LEFT JOIN exchanges e 
	ON c.company_exchange_id = e.exchange_id
LEFT JOIN regions r 
	ON e.exchange_region_id = r.region_id
LEFT JOIN industries i 
	ON c.company_industry_id = i.industry_id
LEFT JOIN sicindustries s 
	ON c.company_sic_id = s.sic_id
WHERE 
    DATE_TRUNC('month', c.company_update_time_stamp) >= DATE_TRUNC('month', CURRENT_DATE)
	AND DATE_TRUNC('year', c.company_update_time_stamp) = DATE_TRUNC('year', CURRENT_DATE)