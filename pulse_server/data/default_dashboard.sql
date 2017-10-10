SELECT DISTINCT type, source, version, sync_id, status, sync_version, le_company_data, company, address, vendor, company_type, classic_mode, le_mode, le_address_type, last_seen,
	le_flags, le_rssi 
FROM log
WHERE type = 'bluetooth'
	AND status = 'online'
	AND last_seen != -1
GROUP BY address
ORDER BY last_seen DESC


