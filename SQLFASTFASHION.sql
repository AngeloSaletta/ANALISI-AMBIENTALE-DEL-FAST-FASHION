ALTER TABLE uniqlo_dim_products 
ADD COLUMN material_id VARCHAR(50); 
-- Usa lo stesso tipo di dato della Primary Key in hm_ref_materials

-- 1. Disabilita la modalità di sicurezza temporaneamente
SET SQL_SAFE_UPDATES = 0;

-- 2. Esegui il tuo UPDATE (che va benissimo)
UPDATE uniqlo_dim_products
JOIN uniqlo_ref_materials ON uniqlo_dim_products.main_material = uniqlo_ref_materials.material_name
SET uniqlo_dim_products.material_id = uniqlo_ref_materials.material_id;

SET SQL_SAFE_UPDATES = 1;


CREATE VIEW view_global_production AS
SELECT 'hm' AS source_brand, factory_id, product_id, production_volume, total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM hm_production_facts
UNION ALL
SELECT 'inditex', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM inditex_production_facts
UNION ALL
SELECT 'primark', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM primark_production_facts
UNION ALL
SELECT 'shein', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM shein_production_facts
UNION ALL
SELECT 'sparc', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM sparc_production_facts
UNION ALL
SELECT 'teddy', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM teddy_production_facts
UNION ALL
SELECT 'uniqlo', factory_id, product_id, production_volume,total_water_footprint, total_carbon_footprint, waste_percentage, timestamp FROM uniqlo_production_facts;





DELIMITER //
DROP PROCEDURE IF EXISTS CalcolaImpattoAziendale; //
CREATE PROCEDURE CalcolaImpattoAziendale(
    IN p_company_name VARCHAR(50),
    IN p_impact_type VARCHAR(50)
)
BEGIN
    DECLARE v_total_co2 DECIMAL(15,2) DEFAULT 0;  
    #DECLARE definisce le variabili locali come v_total_co2 che esistono solo durante l'esecuzione della procedura.
    DECLARE v_total_water DECIMAL(15,2) DEFAULT 0;
    DECLARE v_microplastics_avg DECIMAL(10,2) DEFAULT 0;
    SET p_impact_type = UPPER(TRIM(p_impact_type));
    -- 1. H&M GROUP
    IF p_company_name = 'H&M Group' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
        #COALESCE serve perché se la SUM restituisce NULL, COALESCE sostituisce il null con lo 0 e la funzione puo continuare a fare il calcolo.
        SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM hm_production_facts; 
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM hm_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM hm_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM hm_production_facts pf
            JOIN hm_dim_products p ON pf.product_id = p.product_id
            JOIN hm_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 2. INDITEX
    ELSEIF p_company_name = 'Inditex' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM inditex_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM inditex_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM inditex_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM inditex_production_facts pf
            JOIN inditex_dim_products p ON pf.product_id = p.product_id
            JOIN inditex_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 3. UNIQLO
    ELSEIF p_company_name = 'Uniqlo' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM uniqlo_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM uniqlo_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM uniqlo_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM uniqlo_production_facts pf
            JOIN uniqlo_dim_products p ON pf.product_id = p.product_id
            JOIN uniqlo_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 4. PRIMARK
    ELSEIF p_company_name = 'Primark' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM primark_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM primark_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM primark_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM primark_production_facts pf
            JOIN primark_dim_products p ON pf.product_id = p.product_id
            JOIN primark_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 5. SHEIN
    ELSEIF p_company_name = 'Shein' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM shein_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM shein_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM shein_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM shein_production_facts pf
            JOIN shein_dim_products p ON pf.product_id = p.product_id
            JOIN shein_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 6. SPARC
    ELSEIF p_company_name = 'Sparc' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM sparc_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM sparc_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM sparc_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM sparc_production_facts pf
            JOIN sparc_dim_products p ON pf.product_id = p.product_id
            JOIN sparc_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    -- 7. TEDDY
    ELSEIF p_company_name = 'Teddy' THEN
        IF p_impact_type IN ('CO2', 'ALL') THEN
            SELECT COALESCE(SUM(total_carbon_footprint), 0) INTO v_total_co2 FROM teddy_production_facts;
            SELECT v_total_co2 + COALESCE(SUM(transport_co2_footprint), 0) INTO v_total_co2 FROM teddy_fact_logistics;
        END IF;
        IF p_impact_type IN ('WATER', 'ALL') THEN
            SELECT COALESCE(SUM(total_water_footprint), 0) INTO v_total_water FROM teddy_production_facts;
        END IF;
        IF p_impact_type IN ('MICROPLASTICS', 'ALL') THEN
            SELECT COALESCE(SUM(rm.microplastic_shedding_rank * pf.production_volume) / SUM(pf.production_volume), 0) 
            INTO v_microplastics_avg
            FROM teddy_production_facts pf
            JOIN teddy_dim_products p ON pf.product_id = p.product_id
            JOIN teddy_ref_materials rm ON p.main_material = rm.material_name;
        END IF;
    END IF;
    -- OUTPUT FINALE
    IF p_impact_type = 'CO2' THEN
        SELECT p_company_name AS Azienda, v_total_co2 AS Totale_CO2_Kg;
    ELSEIF p_impact_type = 'WATER' THEN
        SELECT p_company_name AS Azienda, v_total_water AS Totale_Acqua_Litri;
    ELSEIF p_impact_type = 'MICROPLASTICS' THEN
        SELECT p_company_name AS Azienda, v_microplastics_avg AS Rilascio_Medio_Microplastiche;
    ELSEIF p_impact_type = 'ALL' THEN
        SELECT p_company_name AS Azienda, 
               v_total_co2 AS Totale_CO2_Kg,
               v_total_water AS Totale_Acqua_Litri,
               v_microplastics_avg AS Rilascio_Medio_Microplastiche;
    ELSE
        SELECT 'Errore: Parametro non valido.' AS Messaggio;
    END IF;
END //
DELIMITER ;

call CalcolaImpattoAziendale('H&M Group','all');



CREATE VIEW view_produzione_brand_per_inquinamento AS
SELECT 
    source_brand,
    SUM(production_volume) AS vol_totale,
    ROUND(SUM(total_carbon_footprint) / SUM(production_volume), 2) AS co2_per_capo,
    ROUND(SUM(total_water_footprint) / SUM(production_volume), 2) AS acqua_per_capo,
    AVG(waste_percentage) AS scarto_medio
FROM view_global_production
GROUP BY source_brand
ORDER BY co2_per_capo DESC;

/*
CREATE OR REPLACE VIEW Vista_Classifica_Mensile_Sostenibilita AS
SELECT 
    Azienda,
    Anno,
    Mese,
    SUM(production_volume) AS Totale_Capi_Prodotti,
    
    -- Calcoliamo le medie per singolo articolo (Efficienza)
    ROUND(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0), 2) AS Indice_Microplastiche,

    -- ALGORITMO DI RANKING: Creiamo un "Indice di Inquinamento Globale"
    -- Più il punteggio è basso, più l'azienda è ecologica.
    ROUND(
        (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +  -- Peso CO2
        (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +   -- Peso Acqua
        AVG(waste_percentage) +                                                   -- Peso Scarti
        (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10) -- Peso Microplastiche
    , 2) AS Punteggio_Inquinamento,

    -- ASSEGNAZIONE DEL VOTO (A, B, C, D, E, F)
    CASE 
        WHEN (
            (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +
            (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +
            AVG(waste_percentage) +
            (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +
            (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +
            AVG(waste_percentage) +
            (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +
            (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +
            AVG(waste_percentage) +
            (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +
            (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +
            AVG(waste_percentage) +
            (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0) * 10) +
            (SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0) / 10) +
            AVG(waste_percentage) +
            (SUM(microplastic_shedding_rank * production_volume) / NULLIF(SUM(production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita

FROM (
    -- Uniamo tutti i dati di tutte le aziende in un'unica super-tabella temporanea
    SELECT 'H&M Group' AS Azienda, YEAR(pf.timestamp) AS Anno, MONTH(pf.timestamp) AS Mese, pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM hm_production_facts pf JOIN hm_dim_products p ON pf.product_id = p.product_id JOIN hm_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Inditex' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM inditex_production_facts pf JOIN inditex_dim_products p ON pf.product_id = p.product_id JOIN inditex_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Uniqlo' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM uniqlo_production_facts pf JOIN uniqlo_dim_products p ON pf.product_id = p.product_id JOIN uniqlo_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Primark' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM primark_production_facts pf JOIN primark_dim_products p ON pf.product_id = p.product_id JOIN primark_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Shein' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM shein_production_facts pf JOIN shein_dim_products p ON pf.product_id = p.product_id JOIN shein_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Sparc' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM sparc_production_facts pf JOIN sparc_dim_products p ON pf.product_id = p.product_id JOIN sparc_ref_materials rm ON p.main_material = rm.material_name
    UNION ALL
    SELECT 'Teddy' AS Azienda, YEAR(pf.timestamp), MONTH(pf.timestamp), pf.production_volume, pf.total_carbon_footprint, pf.total_water_footprint, pf.waste_percentage, rm.microplastic_shedding_rank FROM teddy_production_facts pf JOIN teddy_dim_products p ON pf.product_id = p.product_id JOIN teddy_ref_materials rm ON p.main_material = rm.material_name
) AS Dati_Unificati
GROUP BY 
    Azienda, 
    Anno, 
    Mese;
    */


CREATE OR REPLACE VIEW Vista_Sostenibilita_HM AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM hm_production_facts pf
JOIN hm_dim_products p ON pf.product_id = p.product_id
JOIN hm_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);
    

CREATE OR REPLACE VIEW Vista_Sostenibilita_INDITEX AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM inditex_production_facts pf
JOIN inditex_dim_products p ON pf.product_id = p.product_id
JOIN inditex_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);
    
    
    
CREATE OR REPLACE VIEW Vista_Sostenibilita_primark AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM primark_production_facts pf
JOIN primark_dim_products p ON pf.product_id = p.product_id
JOIN primark_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);
    
    
    
CREATE OR REPLACE VIEW Vista_Sostenibilita_shein AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM shein_production_facts pf
JOIN shein_dim_products p ON pf.product_id = p.product_id
JOIN shein_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);



CREATE OR REPLACE VIEW Vista_Sostenibilita_sparc AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM sparc_production_facts pf
JOIN sparc_dim_products p ON pf.product_id = p.product_id
JOIN sparc_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);



CREATE OR REPLACE VIEW Vista_Sostenibilita_teddy AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM teddy_production_facts pf
JOIN teddy_dim_products p ON pf.product_id = p.product_id
JOIN teddy_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);



CREATE OR REPLACE VIEW Vista_Sostenibilita_uniqlo AS
SELECT 
    YEAR(pf.timestamp) AS Anno,
    MONTH(pf.timestamp) AS Mese,
    SUM(pf.production_volume) AS Totale_Capi_Prodotti,
    
    -- Medie di Efficienza per singolo capo
    ROUND(SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS CO2_Media_Kg_Per_Capo,
    ROUND(SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0), 2) AS Acqua_Media_L_Per_Capo,
    ROUND(AVG(pf.waste_percentage), 2) AS Scarto_Medio_Fabbrica_Perc,
    ROUND(SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0), 2) AS Indice_Microplastiche,
    -- Punteggio di Inquinamento Globale
    ROUND(
        (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) + 
        (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +   
        AVG(pf.waste_percentage) +                                                   
        (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10) 
    , 2) AS Punteggio_Inquinamento,
    -- Assegnazione del Voto Mensile
    CASE 
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 50 THEN 'A 🏆'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 70 THEN 'B 🟢'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 90 THEN 'C 🟡'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 120 THEN 'D 🟠'
        WHEN (
            (SUM(pf.total_carbon_footprint) / NULLIF(SUM(pf.production_volume), 0) * 10) +
            (SUM(pf.total_water_footprint) / NULLIF(SUM(pf.production_volume), 0) / 10) +
            AVG(pf.waste_percentage) +
            (SUM(rm.microplastic_shedding_rank * pf.production_volume) / NULLIF(SUM(pf.production_volume), 0) * 10)
        ) <= 150 THEN 'E 🔴'
        ELSE 'F ☠️'
    END AS Voto_Sostenibilita
FROM uniqlo_production_facts pf
JOIN uniqlo_dim_products p ON pf.product_id = p.product_id
JOIN uniqlo_ref_materials rm ON p.main_material = rm.material_name
GROUP BY 
    YEAR(pf.timestamp), 
    MONTH(pf.timestamp);


CREATE OR REPLACE VIEW Vista_Sovrapproduzione_Globale AS
-- Blocco H&M
SELECT 'H&M Group' AS Azienda, p.product_id, p.category, COALESCE(prod.Totale,0) AS Capi_Prodotti, COALESCE(log.Totale,0) AS Capi_Spediti, (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) AS Capi_In_Eccesso
FROM hm_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM hm_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM hm_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Inditex
SELECT 'Inditex', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM inditex_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM inditex_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM inditex_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Uniqlo
SELECT 'Uniqlo', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM uniqlo_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM uniqlo_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM uniqlo_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Primark
SELECT 'Primark', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM primark_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM primark_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM primark_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Shein
SELECT 'Shein', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM shein_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM shein_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM shein_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Sparc
SELECT 'Sparc', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM sparc_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM sparc_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM sparc_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0

UNION ALL
-- Blocco Teddy
SELECT 'Teddy', p.product_id, p.category, COALESCE(prod.Totale,0), COALESCE(log.Totale,0), (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0))
FROM teddy_dim_products p 
LEFT JOIN (SELECT product_id, SUM(production_volume) AS Totale FROM teddy_production_facts GROUP BY product_id) prod ON p.product_id = prod.product_id 
LEFT JOIN (SELECT product_id, SUM(shipped_quantity) AS Totale FROM teddy_fact_logistics GROUP BY product_id) log ON p.product_id = log.product_id 
WHERE (COALESCE(prod.Totale,0) - COALESCE(log.Totale,0)) > 0;

DELIMITER //
DROP FUNCTION IF EXISTS CalcolaWaterFootprintProdotto; //
CREATE FUNCTION CalcolaWaterFootprintProdotto(
    p_company_name VARCHAR(100),
    p_product_id VARCHAR(50)
) 
RETURNS DECIMAL(15,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_water_per_unit DECIMAL(15,2) DEFAULT 0;

    IF p_company_name = 'H&M Group' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM hm_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Inditex' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM inditex_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Uniqlo' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM uniqlo_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Primark' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM primark_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Shein' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM shein_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Sparc' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM sparc_production_facts WHERE product_id = p_product_id;

    ELSEIF p_company_name = 'Teddy' THEN
        SELECT COALESCE(SUM(total_water_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_water_per_unit FROM teddy_production_facts WHERE product_id = p_product_id;
    END IF;
    RETURN v_water_per_unit;
END //
DELIMITER ;
SELECT CalcolaWaterFootprintProdotto ('H&M Group', 'PRD_0720');


DELIMITER //
DROP FUNCTION IF EXISTS CalcolaCarbonFootprintProdotto; //
CREATE FUNCTION CalcolaCarbonFootprintProdotto(
    p_company_name VARCHAR(100),
    p_product_id VARCHAR(50)
) 
RETURNS DECIMAL(15,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_co2_per_unit DECIMAL(15,2) DEFAULT 0;
    IF p_company_name = 'H&M Group' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM hm_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Inditex' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM inditex_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Uniqlo' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM uniqlo_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Primark' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM primark_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Shein' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM shein_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Sparc' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM sparc_production_facts WHERE product_id = p_product_id;
    ELSEIF p_company_name = 'Teddy' THEN
        SELECT COALESCE(SUM(total_carbon_footprint) / NULLIF(SUM(production_volume), 0), 0) 
        INTO v_co2_per_unit FROM teddy_production_facts WHERE product_id = p_product_id;
    END IF;
    RETURN v_co2_per_unit;
END //
DELIMITER ;
select CalcolaCarbonFootprintProdotto ('H&M Group', 'PRD_0720') ;



DELIMITER //
DROP FUNCTION IF EXISTS CalcolaMicroplasticheProdotto; //
CREATE FUNCTION CalcolaMicroplasticheProdotto(
    p_company_name VARCHAR(100),
    p_product_id VARCHAR(50)
) 
RETURNS DECIMAL(10,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_microplastics_rank DECIMAL(10,2) DEFAULT 0;
    -- Estrae l'indice di microplastiche basato 
    -- sul materiale principale del prodotto
    IF p_company_name = 'H&M Group' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM hm_dim_products p
        JOIN hm_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Inditex' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM inditex_dim_products p
        JOIN inditex_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Uniqlo' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM uniqlo_dim_products p
        JOIN uniqlo_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Primark' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM primark_dim_products p
        JOIN primark_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Shein' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM shein_dim_products p
        JOIN shein_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Sparc' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM sparc_dim_products p
        JOIN sparc_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    ELSEIF p_company_name = 'Teddy' THEN
        SELECT COALESCE(MAX(rm.microplastic_shedding_rank), 0) INTO v_microplastics_rank
        FROM teddy_dim_products p
        JOIN teddy_ref_materials rm ON p.main_material = rm.material_name
        WHERE p.product_id = p_product_id;
    END IF;
    RETURN v_microplastics_rank;
END //
DELIMITER ;

SELECT CalcolaMicroplasticheProdotto ('H&M Group', 'PRD_0720') ;

DELIMITER //
DROP FUNCTION IF EXISTS CalcolaMoltiplicatoreTrasporto; //
CREATE FUNCTION CalcolaMoltiplicatoreTrasporto(
    p_transport_id VARCHAR(50)
) 
RETURNS DECIMAL(10,3)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_multiplier DECIMAL(10,3) DEFAULT 0.000;
    -- Legge il costo ambientale reale (CO2 per Tonnellata/Km) dalla tua tabella
    SELECT COALESCE(co2_per_ton_km, 0) 
    INTO v_multiplier
    FROM dim_transport_modes
    WHERE transport_id = p_transport_id;
    RETURN v_multiplier;
END //
DELIMITER ;
SELECT CalcolaMoltiplicatoreTrasporto ('T_RAIL') ;

create or replace view totalicapiineccesso as 
SELECT 
    Azienda, 
    SUM(Capi_In_Eccesso) AS Totale_Capi_Invenduti,
    -- Calcola la CO2 sprecata totale (Volumi in eccesso * CO2 per unità)
    ROUND(SUM(Capi_In_Eccesso * CalcolaCarbonFootprintProdotto(Azienda, product_id)), 2) AS Totale_CO2_Sprecata_Kg,
    -- Calcola l'Acqua sprecata totale (Volumi in eccesso * Acqua per unità)
    ROUND(SUM(Capi_In_Eccesso * CalcolaWaterFootprintProdotto(Azienda, product_id)), 2) AS Totale_Acqua_Sprecata_Litri
FROM Vista_Sovrapproduzione_Globale
GROUP BY Azienda
ORDER BY Totale_CO2_Sprecata_Kg DESC;

DELIMITER //

DROP PROCEDURE IF EXISTS sp_report_sostenibilita_annuale; //

CREATE PROCEDURE sp_report_sostenibilita_annuale(IN p_anno INT)
BEGIN
    -- 1. Uniamo logistica filtrando per ANNO tramite product_id (SENZA DUPLICARE LE RIGHE!)
    WITH logistics_filtrata AS (
        SELECT 'H&M Group' AS brand_name, transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM hm_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM hm_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Inditex', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM inditex_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM inditex_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Primark', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM primark_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM primark_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Shein', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM shein_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM shein_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Sparc', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM sparc_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM sparc_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Teddy', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM teddy_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM teddy_production_facts WHERE YEAR(timestamp) = p_anno)
        
        UNION ALL
        SELECT 'Uniqlo', transport_id, distance_km, total_weight_tons, transport_co2_footprint, transport_nox_footprint, transport_sox_footprint 
        FROM uniqlo_fact_logistics 
        WHERE product_id IN (SELECT product_id FROM uniqlo_production_facts WHERE YEAR(timestamp) = p_anno)
    ),
    -- 2. Troviamo il mezzo più usato per quell'anno
    ranking_mezzi AS (
        SELECT 
            lf.brand_name, tm.transport_name, COUNT(*) as n_viaggi,
            ROW_NUMBER() OVER(PARTITION BY lf.brand_name ORDER BY COUNT(*) DESC) as rnk
        FROM logistics_filtrata lf
        JOIN dim_transport_modes tm ON lf.transport_id = tm.transport_id
        GROUP BY lf.brand_name, tm.transport_name
    )
    -- 3. Risultato finale
    SELECT 
        lf.brand_name AS Azienda,
        p_anno AS anno_riferimento,
        rm.transport_name AS mezzo_piu_usato,
        ROUND(SUM(lf.transport_co2_footprint), 2) AS co2_totale_kg,
        ROUND(SUM(lf.transport_nox_footprint), 2) AS nox_totale_kg,
        -- Divisione protetta da division-by-zero
        ROUND(SUM(lf.transport_co2_footprint) / NULLIF(SUM(lf.total_weight_tons * lf.distance_km), 0), 4) AS indice_inquinamento_co2,
        ROUND(COUNT(CASE WHEN lf.transport_id = 'T_AIR' THEN 1 END) * 100.0 / COUNT(*), 1) AS perc_aereo
    FROM 
        logistics_filtrata lf
    JOIN 
        ranking_mezzi rm ON lf.brand_name = rm.brand_name AND rm.rnk = 1
    GROUP BY 
        lf.brand_name, rm.transport_name
    ORDER BY 
        indice_inquinamento_co2 ASC;
END //

DELIMITER ;
Call sp_report_sostenibilita_annuale (2024);

CREATE OR REPLACE VIEW view_global_logistics AS

-- 1. BRAND: H&M
SELECT 
    'HM' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM hm_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN hm_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN hm_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 2. BRAND: Inditex
SELECT 
    'Inditex' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM inditex_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN inditex_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN inditex_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 3. BRAND: Primark
SELECT 
    'Primark' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM primark_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN primark_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN primark_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 4. BRAND: Shein
SELECT 
    'Shein' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM shein_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN shein_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN shein_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 5. BRAND: Sparc
SELECT 
    'Sparc' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM sparc_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN sparc_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN sparc_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 6. BRAND: Teddy
SELECT 
    'Teddy' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM teddy_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN teddy_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN teddy_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL

-- 7. BRAND: Uniqlo
SELECT 
    'Uniqlo' AS brand,
    f.shipment_id,
    f.transport_co2_footprint,
    f.distance_km,
    t.transport_name,
    d.city AS destination_city,
    d.latitude AS dest_latitude,
    d.longitude AS dest_longitude,
    fac.country_code AS origin_country
FROM uniqlo_fact_logistics f
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id
LEFT JOIN uniqlo_dim_destinations d ON f.destination_id = d.destination_id
LEFT JOIN uniqlo_dim_factories fac ON f.origin_factory_id = fac.factory_id;

CREATE OR REPLACE VIEW view_global_logistics AS

-- 1. H&M
SELECT 'HM' AS brand, f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city AS destination_city, d.latitude AS dest_latitude, d.longitude AS dest_longitude, fac.country_code AS origin_country,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END) AS origin_latitude,
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END) AS origin_longitude
FROM hm_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN hm_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN hm_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 2. INDITEX
SELECT 'Inditex', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM inditex_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN inditex_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN inditex_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 3. PRIMARK
SELECT 'Primark', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM primark_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN primark_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN primark_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 4. SHEIN
SELECT 'Shein', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM shein_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN shein_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN shein_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 5. SPARC
SELECT 'Sparc', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM sparc_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN sparc_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN sparc_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 6. TEDDY
SELECT 'Teddy', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM teddy_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN teddy_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN teddy_dim_factories fac ON f.origin_factory_id = fac.factory_id

UNION ALL
-- 7. UNIQLO
SELECT 'Uniqlo', f.shipment_id, f.transport_co2_footprint, t.transport_name, d.city, d.latitude, d.longitude, fac.country_code,
(CASE fac.country_code WHEN 'CN' THEN 35.8617 WHEN 'BD' THEN 23.6850 WHEN 'TR' THEN 38.9637 WHEN 'VN' THEN 14.0583 WHEN 'IN' THEN 20.5937 WHEN 'KH' THEN 12.5657 WHEN 'MM' THEN 21.9162 ELSE 0.0 END),
(CASE fac.country_code WHEN 'CN' THEN 104.1954 WHEN 'BD' THEN 90.3563 WHEN 'TR' THEN 35.2433 WHEN 'VN' THEN 108.2772 WHEN 'IN' THEN 78.9629 WHEN 'KH' THEN 104.9910 WHEN 'MM' THEN 95.9560 ELSE 0.0 END)
FROM uniqlo_fact_logistics f 
LEFT JOIN dim_transport_modes t ON f.transport_id = t.transport_id 
LEFT JOIN uniqlo_dim_destinations d ON f.destination_id = d.destination_id 
LEFT JOIN uniqlo_dim_factories fac ON f.origin_factory_id = fac.factory_id;




