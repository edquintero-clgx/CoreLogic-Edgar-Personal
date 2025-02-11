

DECLARE test_clip STRING;
SET test_clip = '8969093733';

CREATE OR REPLACE TABLE `clgx-idap-bigquery-int-278e.stg_default_dataset.eq_dbuza_fuzzy_matching` AS 

WITH db_usa AS (
    SELECT 
        'db_usa' AS source_table,
        reference_id as rec_id,
        clip,
        address_id,
        clip_fullAddress AS address,
        company,
        NULLIF(CASE 
            WHEN LENGTH(REGEXP_REPLACE(telephone_number, '[^0-9]', '')) = 11 
                AND LEFT(REGEXP_REPLACE(telephone_number, '[^0-9]', ''), 1) = '1' 
                THEN RIGHT(REGEXP_REPLACE(telephone_number, '[^0-9]', ''), 10)

            ELSE REGEXP_REPLACE(telephone_number, '[^0-9]', '')
        END,' ') AS phone_number,
        clip_clipstatus AS business_status,
        CASE WHEN SAFE_CAST(naics_1 AS INT) = 0 THEN NULL ELSE naics_1 END AS NAICS,
        CASE WHEN SAFE_CAST(sic6_1 AS INT) = 0 THEN NULL ELSE SAFE_CAST(sic6_1 AS INT) END AS SIC,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(company), r'\b(llc|inc|corp|shoppingcntr|center|plaza|limited|ltd|company|co|co.|westbrook|jrs|corporation|church)\b', ''),r'[^\w]', '')) AS clean_company,
        CASE 
            -- NAICS matches (Medical, Legal, Accounting, Engineering, Therapy, etc.)
            WHEN naics_1 IN (
            '621111', '621112', '621210', '621310', '621320', '621330', '621340', '621391', '621399', '621420', '621498', '621999', 
            '541110', '541120', '541199',  
            '541211', '541219',  
            '541310', '541320', '541330',  
            '812199', '812990'  
            ) THEN 'Y'
            WHEN sic6_1 IN (
            '8011', '8021', '8031', '8041', '8042', '8043', '8049', -- Medical and Dental Offices
            '8111', '8200', '8721', -- Legal, Education, and Accounting Services
            '8711', '8712', '8713', '8748', -- Architecture & Engineering
            '8999' -- Other Services (e.g., Consultants, Wellness)
            ) THEN 'Y'
    -- Keywords in Business Name (Medical, Legal, Accounting, Wellness, etc.)
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(doctor|physician|clinic|hospital|dentist|dental|chiropractor|optometrist|psychologist|psychiatrist|therapy|therapist|podiatrist|dermatology|neurology|oncology|pediatrics|gynecology|orthopedics|rehabilitation|nutrition|kinesiology|osteopathy|urology)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(lawyer|attorney|law firm|notary|legal|litigation|juridical|law office|barrister|solicitor)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(accountant|cpa|certified public accountant|auditor|bookkeeper|tax consultant)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(architect|engineering|civil engineer|structural engineer|mechanical engineer|electrical engineer|consulting engineer|design firm)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(massage|acupuncture|alternative therapy|reiki|homeopathy|aesthetics|wellness|spa)\b') 
            THEN 'Y'
     -- Abbreviations but avoiding common misinterpretations
            WHEN REGEXP_CONTAINS(LOWER(company), r'\b(dr|md|dds|do|dpm|phd|dc|od|np|pa|rn|lcsw|lmft|esq|cpa|jd|llm)\b') 
                AND NOT REGEXP_CONTAINS(LOWER(company), r'\b(drive|dr\.|md\s|dc\s|maryland|washington dc)\b')
            THEN 'Y'
    -- If it doesn’t match any criteria, mark as 'N'
    ELSE 'N'
  END AS is_professional_office
    FROM 
        `clgx-idap-bigquery-int-278e.land_property_commercial_prefill.cp_firmographics_dbusa_clip_v2_1` 

),
echo_analytics AS (
    SELECT
        'echo_analytics' AS source_table,
        echo_poi_id as rec_id,
        clip,
        address_id,
        clip_fullAddress AS address,
        poi_name AS company,
        CASE 
            WHEN LENGTH(REGEXP_REPLACE(phone_number, '[^0-9]', '')) = 11 
                AND LEFT(REGEXP_REPLACE(phone_number, '[^0-9]', ''), 1) = '1' 
                THEN RIGHT(REGEXP_REPLACE(phone_number, '[^0-9]', ''), 10)
            ELSE REGEXP_REPLACE(phone_number, '[^0-9]', '')
        END AS phone_number,
        business_status,
        tier5_naics_code AS NAICS,
        NULL AS SIC,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(poi_name), r'\b(llc|inc|corp|shoppingcntr|center|plaza|limited|ltd|company|co|shop|westbrook|jrs)\b', ''),r'[^\w]', '')) AS clean_company,
        CASE 
            -- NAICS matches (Medical, Legal, Accounting, Engineering, Therapy, etc.)
            WHEN tier5_naics_code IN (
            '621111', '621112', '621210', '621310', '621320', '621330', '621340', '621391', '621399', '621420', '621498', '621999', 
            '541110', '541120', '541199',  
            '541211', '541219',  
            '541310', '541320', '541330',  
            '812199', '812990'  
            ) THEN 'Y'
            
    -- Keywords in Business Name (Medical, Legal, Accounting, Wellness, etc.)
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(doctor|physician|clinic|hospital|dentist|dental|chiropractor|optometrist|psychologist|psychiatrist|therapy|therapist|podiatrist|dermatology|neurology|oncology|pediatrics|gynecology|orthopedics|rehabilitation|nutrition|kinesiology|osteopathy|urology)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(lawyer|attorney|law firm|notary|legal|litigation|juridical|law office|barrister|solicitor)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(accountant|cpa|certified public accountant|auditor|bookkeeper|tax consultant)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(architect|engineering|civil engineer|structural engineer|mechanical engineer|electrical engineer|consulting engineer|design firm)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(massage|acupuncture|alternative therapy|reiki|homeopathy|aesthetics|wellness|spa)\b') 
            THEN 'Y'
     -- Abbreviations but avoiding common misinterpretations
            WHEN REGEXP_CONTAINS(LOWER(poi_name), r'\b(dr|md|dds|do|dpm|phd|dc|od|np|pa|rn|lcsw|lmft|esq|cpa|jd|llm)\b') 
                AND NOT REGEXP_CONTAINS(LOWER(poi_name), r'\b(drive|dr\.|md\s|dc\s|maryland|washington dc)\b')
            THEN 'Y'
    -- If it doesn’t match any criteria, mark as 'N'
    ELSE 'N'
  END AS is_professional_office
    FROM 
        `clgx-idap-bigquery-int-278e.land_property_commercial_prefill.cp_firmographics_echo_clip_v2_1`  WHERE business_status NOT IN('CLOSED_PERMANENTLY', 'CLOSED_TEMPORARILY')  --AND clip = '2327788344'
),
equifax AS (
    SELECT
        'equifax' AS source_table,
        CAST(efx_id AS STRING) as rec_id,
        clip,
        address_id,
        clip_fullAddress AS address,
        efx_name AS company,
        CASE 
            WHEN LENGTH(REGEXP_REPLACE(efx_phone, '[^0-9]', '')) = 11 
                AND LEFT(REGEXP_REPLACE(efx_phone, '[^0-9]', ''), 1) = '1' 
                THEN RIGHT(REGEXP_REPLACE(efx_phone, '[^0-9]', ''), 10)
            ELSE REGEXP_REPLACE(efx_phone, '[^0-9]', '')
        END AS phone_number,
        efx_mrkt_totalind AS business_status,
        efx_primnaicscode AS NAICS,
        CAST(efx_primsic AS INT) AS SIC,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(T1.efx_name), r'\b(llc|inc|corp|shoppingcntr|center|plaza|limited|ltd|company|co|shop|westbrook|jrs)\b', ''),r'[^\w]', '')) AS clean_company,
        CASE 
            -- NAICS matches (Medical, Legal, Accounting, Engineering, Therapy, etc.)
            WHEN efx_primnaicscode IN (
            '621111', '621112', '621210', '621310', '621320', '621330', '621340', '621391', '621399', '621420', '621498', '621999', 
            '541110', '541120', '541199',  
            '541211', '541219',  
            '541310', '541320', '541330',  
            '812199', '812990'  
            ) THEN 'Y'
            WHEN efx_primsic IN (
            '8011', '8021', '8031', '8041', '8042', '8043', '8049', -- Medical and Dental Offices
            '8111', '8200', '8721', -- Legal, Education, and Accounting Services
            '8711', '8712', '8713', '8748', -- Architecture & Engineering
            '8999' -- Other Services (e.g., Consultants, Wellness)
            ) THEN 'Y'
    -- Keywords in Business Name (Medical, Legal, Accounting, Wellness, etc.)
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(doctor|physician|clinic|hospital|dentist|dental|chiropractor|optometrist|psychologist|psychiatrist|therapy|therapist|podiatrist|dermatology|neurology|oncology|pediatrics|gynecology|orthopedics|rehabilitation|nutrition|kinesiology|osteopathy|urology)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(lawyer|attorney|law firm|notary|legal|litigation|juridical|law office|barrister|solicitor)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(accountant|cpa|certified public accountant|auditor|bookkeeper|tax consultant)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(architect|engineering|civil engineer|structural engineer|mechanical engineer|electrical engineer|consulting engineer|design firm)\b') 
            THEN 'Y'
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(massage|acupuncture|alternative therapy|reiki|homeopathy|aesthetics|wellness|spa)\b') 
            THEN 'Y'
     -- Abbreviations but avoiding common misinterpretations
            WHEN REGEXP_CONTAINS(LOWER(efx_name), r'\b(dr|md|dds|do|dpm|phd|dc|od|np|pa|rn|lcsw|lmft|esq|cpa|jd|llm)\b') 
                AND NOT REGEXP_CONTAINS(LOWER(efx_name), r'\b(drive|dr\.|md\s|dc\s|maryland|washington dc)\b')
            THEN 'Y'
    -- If it doesn’t match any criteria, mark as 'N'
    ELSE 'N'
  END AS is_professional_office

    FROM 
        `clgx-idap-bigquery-int-278e.land_property_commercial_prefill.cp_firmographics_equifax_clip_v2_1` T1 
    WHERE T1.efx_mrkt_totalind != 'OB' --AND clip = '9939683648'
),

consolidated_sources AS (
    SELECT * FROM db_usa
    UNION ALL
    SELECT * FROM echo_analytics 
    UNION ALL
    SELECT * FROM equifax
),

bigrams_table AS (
    SELECT 
        source_table,
        clip,
        address_id,
        address,
        NAICS,
        SIC,
        phone_number,
        company,
        clean_company,
        is_professional_office,
        CONCAT(company,source_table,phone_number) AS uid,
        ARRAY(
            SELECT SUBSTR(clean_company, pos, 2)
            FROM UNNEST(GENERATE_ARRAY(1, GREATEST(LENGTH(clean_company) - 1, 1))) AS pos
        ) AS bigrams
    FROM 
        consolidated_sources 
    
    --WHERE clip = test_clip
),

pairwise_comparisons AS (
    SELECT 
        a.clip,
        a.source_table AS source_table_1,
        b.source_table AS source_table_2,
        a.address_id AS address_id_1,
        b.address_id AS address_id_2,
        a.address AS address_1,
        b.address AS address_2,
        a.NAICS AS NAICS_1,
        b.NAICS AS NAICS_2,
        a.SIC AS SIC_1,
        b.SIC AS SIC_2,
        a.phone_number AS phone_1,
        b.phone_number AS phone_2,
        a.company AS company_1,
        b.company AS company_2,
        a.is_professional_office AS ipo_1,
        b.is_professional_office AS ipo_2,
        a.bigrams AS bigrams_1,
        b.bigrams AS bigrams_2
    FROM 
        bigrams_table a
    JOIN 
        bigrams_table b
    ON 
        a.clip = b.clip
        AND a.uid <> b.uid -- Avoid self-join duplicates
),

intersections AS (
    SELECT 
        pc.clip,
        pc.source_table_1,
        pc.source_table_2,
        pc.company_1,
        pc.company_2,
        pc.address_id_1,
        pc.address_id_2,
        pc.address_1,
        pc.address_2,
        pc.NAICS_1,
        pc.NAICS_2,
        pc.SIC_1,
        pc.SIC_2,
        pc.phone_1,
        pc.phone_2,
        pc.ipo_1,
        pc.ipo_2,
        ARRAY_LENGTH(
            ARRAY(
                SELECT DISTINCT bigram_1 
                FROM UNNEST(pc.bigrams_1) AS bigram_1
                WHERE bigram_1 IN (
                    SELECT bigram_2 FROM UNNEST(pc.bigrams_2) AS bigram_2
                )
            )
        ) AS intersection_size
    FROM 
        pairwise_comparisons pc
),
unions AS (
    SELECT 
        pc.clip,
        pc.source_table_1,
        pc.source_table_2,
        pc.company_1,
        pc.company_2,
        pc.address_id_1,
        pc.address_id_2,
        pc.address_1,
        pc.address_2,
        pc.NAICS_1,
        pc.NAICS_2,
        pc.SIC_1,
        pc.SIC_2,
        pc.phone_1,
        pc.phone_2,
        pc.ipo_1,
        pc.ipo_2,
        ARRAY_LENGTH(
            ARRAY(
                SELECT DISTINCT bigram
                FROM UNNEST(pc.bigrams_1) AS bigram
                UNION DISTINCT 
                SELECT bigram
                FROM UNNEST(pc.bigrams_2) AS bigram
            )
        ) AS union_size
    FROM 
        pairwise_comparisons pc
),
jaccard_similarity AS (
    SELECT 
        i.clip,
        i.source_table_1,
        i.source_table_2,
        i.company_1,
        i.company_2,
        i.address_id_1,
        i.address_id_2,
        i.address_1,
        i.address_2,
        i.NAICS_1,
        i.NAICS_2,
        i.SIC_1,
        I.SIC_2,
        i.phone_1,
        i.phone_2,
        i.ipo_1,
        i.ipo_2,
        SAFE_DIVIDE(i.intersection_size, u.union_size) AS jaccard_index
    FROM 
        intersections i
    JOIN 
        unions u
    ON 
        i.clip = u.clip
        AND i.company_1 = u.company_1
        AND i.company_2 = u.company_2
)
--- EA Table
    , aggregated_similarity AS (
    SELECT 
        clip, 
        address_id_1 AS address_id,
        source_table_1 AS source_table,
        company_1 AS company,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax' AND address_id_1 <> address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S1_count_equifax,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa'  AND address_id_1 <> address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S1_count_db_usa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics'  AND address_id_1 <> address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S1_count_echo_analytics,

        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax' AND address_id_1 = address_id_2 THEN company_2 END) AS S2_count_equifax,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa' AND address_id_1 = address_id_2 THEN company_2 END) AS S2_count_db_usa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics' AND address_id_1 = address_id_2 THEN company_2 END) AS S2_count_echo_analytics,

        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND (phone_1 = phone_2 OR (phone_1 IS NULL OR phone_2 IS NULL))AND jaccard_index >= 0.5 THEN company_2 END) AS S3_1_count_dbusa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND (phone_1 = phone_2 OR (phone_1 IS NULL OR phone_2 IS NULL)) AND jaccard_index >= 0.5 THEN company_2 END) AS S3_1_count_efx,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND (phone_1 = phone_2 OR (phone_1 IS NULL OR phone_2 IS NULL)) AND jaccard_index >= 0.5 THEN company_2 END) AS S3_1_count_echo,

        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S3_count_equifax,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S3_count_db_usa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics' AND LOWER(company_1) <> LOWER(company_2) AND address_id_1 = address_id_2 AND jaccard_index >= 0.5 THEN company_2 END) AS S3_count_echo_analytics,

        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax' AND address_id_1 = address_id_2 AND ipo_1 = 'Y' AND ipo_2 = 'Y' THEN company_2 END) AS S4_count_equifax,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa' AND address_id_1 = address_id_2 AND ipo_1 = 'Y' AND ipo_2 = 'Y'THEN company_2 END) AS S4_count_db_usa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics' AND address_id_1 = address_id_2 AND ipo_1 = 'Y' AND ipo_2 = 'Y'THEN company_2 END) AS S4_count_echo_analytics,

        COUNT(DISTINCT CASE WHEN source_table_2 = 'echo_analytics' AND jaccard_index >= 0.5 AND phone_1 <> phone_2 THEN company_2 END) AS S5_count_echo_analytics,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'db_usa'         AND jaccard_index >= 0.5 AND phone_1 <> phone_2 THEN company_2 END) AS S5_count_db_usa,
        COUNT(DISTINCT CASE WHEN source_table_2 = 'equifax'        AND jaccard_index >= 0.5 AND phone_1 <> phone_2 THEN company_2 END) AS S5_count_equifax,

    FROM jaccard_similarity

    GROUP BY clip, address_id_1, company_1, source_table_1
),

--SELECT * FROM aggregated_similarity


for_aggregation AS(
SELECT 
    T1.source_table,
    T1.rec_id,
    T1.company,
    T1.address,
    T1.clip,
    T1.address_id,
    T1.phone_number,
    T1.business_status,
    T1.NAICS,
    T1,SIC,
    CASE WHEN A.S1_count_equifax > 0 THEN 'Y' ELSE 'N' END AS S1_equifax,
    CASE WHEN A.S1_count_db_usa > 0 THEN 'Y' ELSE 'N' END AS S1_itself,
    CASE WHEN A.S1_count_echo_analytics > 0 THEN 'Y' ELSE 'N' END AS S1_echo,

    CASE WHEN A.S2_count_equifax > 1 THEN 'Y' ELSE 'N' END AS S2_equifax,
    CASE WHEN A.S2_count_db_usa > 0 THEN 'Y' ELSE 'N' END AS S2_itself,
    CASE WHEN A.S2_count_echo_analytics > 1 THEN 'Y' ELSE 'N' END AS S2_echo,

    CASE WHEN A.S3_1_count_efx > 0 THEN 'Y' ELSE 'N' END AS S3_1_equifax,
    CASE WHEN A.S3_1_count_dbusa > 0 THEN 'Y' ELSE 'N' END AS S3_1_itself,
    CASE WHEN A.S3_1_count_echo > 0 THEN 'Y' ELSE 'N' END AS S3_1_echo,

    CASE WHEN A.S3_count_equifax > 0 THEN 'Y' ELSE 'N' END AS S3_equifax,
    CASE WHEN A.S3_count_db_usa > 0 THEN 'Y' ELSE 'N' END AS S3_itself,
    CASE WHEN A.S3_count_echo_analytics > 0 THEN 'Y' ELSE 'N' END AS S3_echo,

    CASE WHEN A.S4_count_equifax > 0 THEN 'Y' ELSE 'N' END AS S4_equifax,
    CASE WHEN A.S4_count_db_usa > 0 THEN 'Y' ELSE 'N' END AS S4_itself,
    CASE WHEN A.S4_count_echo_analytics > 0 THEN 'Y' ELSE 'N' END AS S4_echo,

    CASE WHEN A.S5_count_equifax > 1 THEN 'Y' ELSE 'N' END AS S5_equifax,
    CASE WHEN A.S5_count_db_usa > 0 THEN 'Y' ELSE 'N' END AS S5_itself,
    CASE WHEN A.S5_count_echo_analytics > 1 THEN 'Y' ELSE 'N' END AS S5_echo,

FROM db_usa T1
LEFT JOIN aggregated_similarity A 
    ON T1.clip = A.clip 
    AND T1.address_id = A.address_id 
    AND T1.company = A.company
    AND T1.source_table = A.source_table

--WHERE 
    --T1.source_table = 'db_usa' 
    --AND T1.clip = test_clip
    --ORDER BY T1.company ASC, T1.source_table ASC
)
SELECT * FROM for_aggregation
