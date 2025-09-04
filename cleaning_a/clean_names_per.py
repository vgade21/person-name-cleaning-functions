import pandas as pd
import pyodbc
import re
import warnings

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=SyntaxWarning)

## Takes around 16 min to run

# Script Purpose:
# Cleans name fields in person table, splits them out, extracts relevant information (alias) into a separate dataframe, and creates new person records where multiple people were recorded on the same record.
# Feature DQI:
# 32428
# 32377
# 32376
# 32314
# 31661
# 31657
# 31651
# 31650
# 31647
# 31646
# 31643
# Input:
# Output: cleaned given_names and surname with given_names split out into three columns along with a dataframe of aliases

server = "ATLASSBXSQL02.ems.tas.gov.au"
database = "cms_clean"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;"
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

query_4 = """
-- 611,583 records
-- 9,830 excluded (1,925 businesses + 7,905 empoyees). There will be overlap of records. 621,408 records in cms_clean.dbo.person
-- script for person.py
WITH business_CTE AS (
	-- 1,925 records
	SELECT p.id
	FROM cms.dbo.person p
	WHERE
	    -- Ensure only records found in cms_clean.dbo.person are included
	    EXISTS (
	        SELECT 1
	        FROM cms_clean.dbo.person cp
	        WHERE cp.id = p.id
	    )
	    -- Include records linked to victim table only.
	    AND NOT EXISTS (
	        SELECT 1 FROM cms_clean.dbo.suspect s WHERE s.person_id = p.id
	    )
	    AND NOT EXISTS (
	        SELECT 1 FROM cms_clean.dbo.witness w WHERE w.person_id = p.id
	    )
	    AND NOT EXISTS (
	        SELECT 1 FROM cms_clean.dbo.reporting_person rp WHERE rp.person_id = p.id
	    )
	    AND NOT EXISTS (
	        SELECT 1 FROM cms_clean.dbo.offender o WHERE o.person_id = p.id
	    )
	    AND NOT EXISTS (
	        SELECT 1 FROM cms_clean.dbo.stolen_motor_vehicle sv WHERE sv.recovered_reported_by_person_id = p.id
	    )
	    AND (
			p.given_names LIKE '%acn%[0-9]%' OR p.surname LIKE '%acn%[0-9]%'
		    -- Check for 'PTY LTD' or 'P/L' anywhere in the text
		    OR p.given_names LIKE '%PTY LTD%' OR p.surname LIKE '%PTY LTD%'
		    OR p.given_names LIKE '%P/L%' OR p.surname LIKE '%P/L%'
		    OR p.given_names LIKE '%pty limited%' OR p.surname LIKE '%pty limited%'
		    -- Check for 'T/A' anywhere in the text
		    OR p.given_names LIKE '%T/A%' OR p.surname LIKE '%T/A%'
			-- Check for 'LTD' or 'LIMITED'
			OR p.given_names LIKE '% LTD%' OR p.surname LIKE '% LTD%'
			OR p.given_names LIKE '%LIMITED%'	OR p.surname LIKE '%LIMITED%'
			-- Check for 'PTY'
			OR p.given_names LIKE '% PTY%' AND p.surname LIKE '% PTY%'
			-- Check for 'INC' OR 'INCORPORATED'
			OR p.given_names LIKE '% INC%' OR p.surname LIKE '% INC%'
			OR p.given_names LIKE '%INCORPORATED%' OR p.surname LIKE '%INCORPORATED%'
			-- Check for 'TASMANIA' OR 'AUSTRALIA'
			OR p.given_names LIKE '%TASMANIA%' OR p.surname LIKE '%TASMANIA%'
			OR p.given_names LIKE '%AUSTRALIA%' OR p.surname LIKE '%AUSTRALIA%'
			-- Check for other keywords
			OR p.given_names LIKE '%DEPARTMENT%' OR p.surname LIKE '%DEPARTMENT%'
			OR p.given_names LIKE '% CLUB%' OR p.surname LIKE '% CLUB%'
			OR p.given_names LIKE '% SERVICE%'	OR p.surname LIKE '% SERVICE%'
			OR p.given_names LIKE '% SPORTS%' OR p.surname LIKE '% SPORTS%'
			OR p.given_names LIKE '%HOTEL%' OR p.surname LIKE '%HOTEL%'
			OR p.given_names LIKE '%HOBART %'	OR p.surname LIKE '%HOBART %'
			OR p.given_names LIKE '% CHURCH %' OR p.surname LIKE '% CHURCH %'
			OR p.given_names LIKE '% SCHOOL%'	OR p.surname LIKE '% SCHOOL%'
			OR p.given_names LIKE '%COMMUNITY%' OR p.surname LIKE '%COMMUNITY%'
			OR p.given_names LIKE '%DEPT%' OR p.surname LIKE '%DEPT%'
            OR p.given_names LIKE '%HOUSING %' OR p.surname LIKE '%HOUSING %'
            OR p.given_names LIKE '%LAUNCESTON%' OR p.surname LIKE '%LAUNCESTON%'
			OR p.given_names LIKE '% COUNCIL%' OR p.surname LIKE '% COUNCIL%'
			OR p.given_names LIKE '%COUNCIL %' OR p.surname LIKE '%COUNCIL %'
			OR p.given_names LIKE '%PRIMARY%'	OR p.surname LIKE '%PRIMARY%'
			OR p.given_names LIKE '%GROUP%' OR p.surname LIKE '%GROUP%'
			OR p.given_names LIKE '% STATION%' OR p.surname LIKE '% STATION%'
			OR p.given_names LIKE '%SUPERMARKET%'	OR p.surname LIKE '%SUPERMARKET%'
			OR p.given_names LIKE '% DEVONPORT%' OR p.surname LIKE '% DEVONPORT%'
            OR p.given_names LIKE '%-DEVONPORT%' OR p.surname LIKE '%-DEVONPORT%'
			OR p.given_names LIKE '%PHARMACY%' OR p.surname LIKE '%PHARMACY%'
			OR p.given_names LIKE '%RESTAURANT%' OR p.surname LIKE '%RESTAURANT%'
			OR p.given_names LIKE '%ASSOCIATION%'	OR p.surname LIKE '%ASSOCIATION%'
			OR p.given_names LIKE '%ELECTRICAL %'	OR p.surname LIKE '%ELECTRICAL %'
			OR p.given_names LIKE '%COMPANY%'	OR p.surname LIKE '%COMPANY%'
			OR p.given_names LIKE '%CONSTRUCTION%' OR p.surname LIKE '%CONSTRUCTION%'
			OR p.given_names LIKE '%MANAGEMENT%' OR p.surname LIKE '%MANAGEMENT%'
			OR p.given_names LIKE '% BUILDING%' OR p.surname LIKE '% BUILDING%'
			OR p.given_names LIKE '%TRANSPORT%' OR p.surname LIKE '%TRANSPORT%'
			OR p.given_names LIKE '%WOOLWORTHS%' OR p.surname LIKE '%WOOLWORTHS%'
			OR p.given_names LIKE '%GLENORCHY%' OR p.surname LIKE '%GLENORCHY%'
			OR p.given_names LIKE '%SOUTHERN %' OR p.surname LIKE '%SOUTHERN %'
			OR p.given_names LIKE '%BAKERY%' OR p.surname LIKE '%BAKERY%'
			OR p.given_names LIKE '%SOCIETY%'	OR p.surname LIKE '%SOCIETY%'
			OR p.given_names LIKE '%SOLUTIONS%' OR p.surname LIKE '%SOLUTIONS%'
			OR p.given_names LIKE '%CONTRACTING%' OR p.surname LIKE '%CONTRACTING%'
			OR p.given_names LIKE '%DIVISON%'	OR p.surname LIKE '%DIVISON%'
			OR p.given_names LIKE '%FOUNDATION%' OR p.surname LIKE '%FOUNDATION%'
			OR p.given_names LIKE '%CENTRE%' OR p.surname LIKE '%CENTRE%'
			OR p.given_names LIKE '%EDUCATION%' OR p.surname LIKE '%EDUCATION%'
		    OR p.given_names LIKE '%CORPORATION%' OR p.surname LIKE '%CORPORATION%'
		    OR p.given_names LIKE '%CHEMIST%' OR p.surname LIKE '%CHEMIST%'
		    OR p.given_names LIKE '%UNION%' OR p.surname LIKE '%UNION%'
		    OR p.given_names LIKE '% COLLEGE%' OR p.surname LIKE '% COLLEGE%'
		    OR p.given_names LIKE '%ASSOCIATES%' OR p.surname LIKE '%ASSOCIATES%'
		    OR p.given_names LIKE '%JEWELLERS%' OR p.surname LIKE '%JEWELLERS%'
		    OR p.given_names LIKE '%NURSERY%' OR p.surname LIKE '%NURSERY%'
		    OR p.given_names LIKE '%BUSINESS%' OR p.surname LIKE '%BUSINESS%'
		    OR p.given_names LIKE '%GOVERNMENT%' OR p.surname LIKE '%GOVERNMENT%'
		    OR p.given_names LIKE '%INDUSTRIES%' OR p.surname LIKE '%INDUSTRIES%'
		    OR p.given_names LIKE '%SALVATION%' OR p.surname LIKE '%SALVATION%'
		    OR p.given_names LIKE '%HOLDINGS%' OR p.surname LIKE '%HOLDINGS%'
		    OR p.given_names LIKE '%HOMES %' OR p.surname LIKE '%HOMES %'
		    OR p.given_names LIKE '% BODY%' OR p.surname LIKE '% BODY%'
		    OR p.given_names LIKE '%CLOTHING%' OR p.surname LIKE '%CLOTHING%'
		    OR p.given_names LIKE '%DESIGN%' OR p.surname LIKE '%DESIGN%'
		    OR p.given_names LIKE '%COUNTRY%' OR p.surname LIKE '%COUNTRY%'
		    OR p.given_names LIKE '%GARDEN %' OR p.surname LIKE '%GARDEN %'
		    OR p.given_names LIKE '%WAREHOUSE%' OR p.surname LIKE '%WAREHOUSE%'
		    OR p.given_names LIKE '%NORTHERN%' OR p.surname LIKE '%NORTHERN%'
		    OR p.given_names LIKE '%AUTOMOTIVE%' OR p.surname LIKE '%AUTOMOTIVE%'
		    OR p.given_names LIKE '%PROPERTY%' OR p.surname LIKE '%PROPERTY%'
		    OR p.given_names LIKE '% WORLD%' OR p.surname LIKE '% WORLD%'
		    OR p.given_names LIKE '%RENTALS%' OR p.surname LIKE '%RENTALS%'
		    OR p.given_names LIKE '%PLUMBING%' OR p.surname LIKE '%PLUMBING%'
		    OR p.given_names LIKE '%HOSPITAL%' OR p.surname LIKE '%HOSPITAL%'
		    OR p.given_names LIKE '%LIMITED%' OR p.surname LIKE '%LIMITED%'
		    OR p.given_names LIKE '%LIQUOR%' OR p.surname LIKE '%LIQUOR%'
		    OR p.given_names LIKE '%UNIVERSITY%' OR p.surname LIKE '%UNIVERSITY%'
		    OR p.given_names LIKE '%SHOPPING%' OR p.surname LIKE '%SHOPPING%'
		    OR p.given_names LIKE '%EXPRESS%' OR p.surname LIKE '%EXPRESS%'
		    OR p.given_names LIKE '% FARM%' OR p.surname LIKE '% FARM%'
		    OR p.given_names LIKE '%ENTERPRISES%' OR p.surname LIKE '%ENTERPRISES%'
		    OR p.given_names LIKE '%CARS %' OR p.surname LIKE '%CARS %'
		    OR p.given_names LIKE '%GALLERY%' OR p.surname LIKE '%GALLERY%'
		    OR p.given_names LIKE '%STUDIO%' OR p.surname LIKE '%STUDIO%'
		    OR p.given_names LIKE '%MOTEL%' OR p.surname LIKE '%MOTEL%'
		    OR p.given_names LIKE '%DENTAL%' OR p.surname LIKE '%DENTAL%'
		    OR p.given_names LIKE '%corporate%' OR p.surname LIKE '%corporate%'
			OR p.given_names LIKE '%HEALTH%' OR p.surname LIKE '%HEALTH%'
			)
		AND p.id != 594107
),
employee_CTE AS (
-- 7,905 records
SELECT p.id
FROM cms.dbo.person p
WHERE
    -- Ensure only records found in cms_clean.dbo.person are included
    EXISTS (
        SELECT 1
        FROM cms_clean.dbo.person cp
        WHERE cp.id = p.id
    )
    -- Exclude records found in the offender or suspect tables
    AND NOT EXISTS (
        SELECT 1 FROM cms_clean.dbo.offender o WHERE o.person_id = p.id
    )
    AND NOT EXISTS (
        SELECT 1 FROM cms_clean.dbo.suspect s WHERE s.person_id = p.id
    )
    AND (
        -- Records with specific keywords in  p.given_names or surname
        (
	        p.given_names LIKE '%A/SGT%' OR p.given_names LIKE '%A/SERGT%' OR p.given_names LIKE '%A/Sgtt%'
	        OR p.given_names LIKE '%A/C SGT%' OR p.given_names LIKE '%ACT/SGT%' OR p.given_names LIKE '%A/G SGT%'
	        OR p.given_names LIKE '%A/ SGT%' OR p.given_names LIKE '%A/S%' OR p.given_names LIKE '%A/Sergeant%'
	        OR p.given_names LIKE '%S/Sgt%' OR p.given_names LIKE '%DET SGT%' OR p.given_names LIKE '%A/G INSPECTOR%' OR p.given_names LIKE '%A/INSP%' OR p.given_names LIKE '%S/Const%'
	        OR p.given_names LIKE '%S/CONSTABLE%' OR p.given_names LIKE '%SEN/CONSTABLE%' OR p.given_names LIKE '%Senior/Constable%'
	        OR p.given_names LIKE '%S/Cst%' OR p.given_names LIKE '%SNR/CONST%' OR p.given_names LIKE '%S/C%'
	        OR p.given_names LIKE '%1/C Constable%' OR p.given_names LIKE '%1/C CONST%' OR p.given_names LIKE '%1/CONST%' OR p.given_names LIKE '%1/C%'
	        OR p.given_names LIKE '%Det 1/C Constable%' OR p.given_names LIKE '%DET/CONST%' OR p.given_names LIKE '%D/CONSTABLE%'
	        OR p.given_names LIKE '%DETECTIVE S/C%' OR p.given_names LIKE '%D/SGT%' OR p.given_names LIKE '%T/CONSTABLE%'
	        OR p.given_names LIKE '%T/CONST%'
	        OR p.given_names LIKE '%cons %'
	        OR p.given_names LIKE '%cons. %'
	        OR p.given_names LIKE '%const %'
	        OR p.given_names LIKE '%const. %'
	        OR p.given_names LIKE '%insp %'
	        OR p.given_names LIKE '%insp. %'
	        OR p.given_names LIKE '%sgt %'
	        OR p.given_names LIKE '%sgt. %'
	        OR p.given_names LIKE 'det %'
	        OR p.given_names LIKE '%det. %'
	        OR p.given_names LIKE '%C0NS%'
	        OR p.given_names LIKE '%snr/%'
			OR p.given_names LIKE '%snr %'
			OR p.given_names like ('%CONSTABLE%')
			OR p.given_names = 'CONST'
			OR p.given_names = 'cons'
			OR p.given_names like ('%const.%')
			OR p.given_names like ('%(cons)%')
			OR p.given_names like ('%(const)%')
			OR p.given_names like ('%sergeant%')
			OR p.given_names like ('%SGT')
			OR p.given_names = 'sgt'
			OR p.given_names like ('%ACTING SGT%')
			OR p.given_names like ('%ACT SGT%')
			OR p.given_names like ('%insp.%')
			OR p.given_names like ('%INSPECTOR%')
			OR p.given_names like ('%(insp)%')
			OR p.given_names = 'insp'
			OR p.given_names like ('%DETECTIVE%')
			OR p.given_names like ('%det.%')
			OR p.given_names like ('%DET INSP%')
			OR p.given_names like ('%SEN CONST%')
			OR p.given_names like ('%SEN SGT%')
			OR p.given_names like ('%POLICE OFFICER%')
			OR p.given_names like ('%Trainee%')
            OR p.given_names like ('%(SGT)%')
            OR p.given_names like ('% SGT.')
            OR p.given_names = 'SGT.'
            OR p.given_names = 'SGT,'
            OR p.given_names like ('SGT, %')
            OR p.given_names like ('% CONST')
            OR p.given_names like ('%(DET CONST)%')
            OR p.given_names like ('% .CONST')
            OR p.given_names like ('CONST, %')
            OR p.given_names like ('%CONSTBALE %')
            OR p.given_names like ('%CONSTAVLE %')
            OR p.given_names like ('% CONS.')
            OR p.given_names like ('%CONSABLE %')
            OR p.given_names like ('%CONSTABALE %')
            OR p.given_names like ('%CONSTBLE %')
            OR p.given_names like ('%SENIOR CONSTALE %')
            OR p.given_names like ('%CONSTABEL %')
            OR p.given_names = 'CONS.'
            OR p.given_names = 'CONSTABLE'
            OR p.given_names like ('%TASMANIA POLICE%')
            OR p.given_names = 'POLICE'
            OR p.given_names like ('% POLICE')
            OR p.given_names like ('% INSP')
            OR p.given_names like ('INSP, %')
            OR p.given_names like ('%(SERGT)%')
            OR p.given_names like ('%DET CONST%')
            OR p.given_names like ('%SGT(%')
            OR p.given_names = 'CONST,'
            OR p.given_names LIKE ('%CONSTALBE %')
            OR p.given_names LIKE ('%Constale %')
            OR p.given_names LIKE ('%Constazble %')
            OR p.given_names LIKE ('%CONSR %')
            OR p.given_names LIKE ('%(CONS.)%')
            OR p.given_names LIKE ('%CONSTANLE %')
            OR p.surname like ('%(SGT)%')
            OR p.surname like ('% SGT.')
            OR p.surname = 'SGT.'
            OR p.surname = 'SGT,'
            OR p.surname like ('SGT, %')
            OR p.surname like ('% CONST')
            OR p.surname like ('%(DET CONST)%')
            OR p.surname like ('% .CONST')
            OR p.surname like ('CONST, %')
            OR p.surname like ('%CONSTBALE %')
            OR p.surname like ('%CONSTAVLE %')
            OR p.surname like ('% CONS.')
            OR p.surname like ('%CONSABLE %')
            OR p.surname like ('%CONSTABALE %')
            OR p.surname like ('%CONSTBLE %')
            OR p.surname like ('%SENIOR CONSTALE %')
            OR p.surname like ('%CONSTABEL %')
            OR p.surname = 'CONS.'
            OR p.surname = 'CONSTABLE'
            OR p.surname like ('%TASMANIA POLICE%')
            OR p.surname = 'POLICE'
            OR p.surname like ('% POLICE')
            OR p.surname like ('% INSP')
            OR p.surname like ('INSP, %')
            OR p.surname like ('%(SERGT)%')
            OR p.surname like ('%DET CONST%')
            OR p.surname like ('%SGT(%')
            OR p.surname = 'CONST,'
            OR p.surname LIKE ('%CONSTALBE %')
            OR p.surname LIKE ('%Constale %')
            OR p.surname LIKE ('%Constazble %')
            OR p.surname LIKE ('%CONSR %')
            OR p.surname LIKE ('%(CONS.)%')
            OR p.surname LIKE ('%CONSTANLE %')
			OR p.surname like ('%CONSTABLE%')
			OR p.surname = 'CONST'
			OR p.surname = 'cons'
			OR p.surname like ('%const.%')
			OR p.surname like ('%(cons)%')
			OR p.surname like ('%(const)%')
			OR p.surname like ('%sergeant%')
			OR p.surname like ('%SGT')
			OR p.surname = 'sgt'
			OR p.surname like ('%ACTING SGT%')
			OR p.surname like ('%ACT SGT%')
			OR p.surname like ('%insp.%')
			OR p.surname like ('%INSPECTOR%')
			OR p.surname like ('%(insp)%')
			OR p.surname = 'insp'
			OR p.surname like ('%DETECTIVE%')
			OR p.surname like ('%det.%')
			OR p.surname like ('%DET INSP%')
			OR p.surname like ('%SEN CONST%')
			OR p.surname like ('%SEN SGT%')
			OR p.surname like ('%POLICE OFFICER%')
			OR p.surname like ('%Trainee%')
	        OR p.surname LIKE '%snr/%'
	        OR p.surname LIKE '%snr %'
	        OR p.surname LIKE '%C0NS%'
	        OR p.surname LIKE '%cons %'
	        OR p.surname LIKE '%cons. %'
	        OR p.surname LIKE '%const %'
	        OR p.surname LIKE '%const. %'
	        OR p.surname LIKE '%insp %'
	        OR p.surname LIKE '%insp. %'
	        OR p.surname LIKE '%sgt %'
	        OR p.surname LIKE '%sgt. %'
	        OR p.surname LIKE 'det %'
	        OR p.surname LIKE '%det. %'
	        OR p.surname LIKE '%A/SGT%' OR p.surname LIKE '%A/SERGT%' OR p.surname LIKE '%A/Sgtt%'
	        OR p.surname LIKE '%A/C SGT%' OR p.surname LIKE '%ACT/SGT%' OR p.surname LIKE '%A/G SGT%'
	        OR p.surname LIKE '%A/ SGT%' OR p.surname LIKE '%A/S%' OR p.surname LIKE '%A/Sergeant%'
	        OR p.surname LIKE '%S/Sgt%' OR p.surname LIKE '%DET SGT%' OR p.surname LIKE '%A/G INSPECTOR%' OR p.surname LIKE '%A/INSP%' OR p.surname LIKE '%S/Const%'
	        OR p.surname LIKE '%S/CONSTABLE%' OR p.surname LIKE '%SEN/CONSTABLE%' OR p.surname LIKE '%Senior/Constable%'
	        OR p.surname LIKE '%S/Cst%' OR p.surname LIKE '%SNR/CONST%' OR p.surname LIKE '%S/C%'
	        OR p.surname LIKE '%1/C Constable%' OR p.surname LIKE '%1/C CONST%' OR p.surname LIKE '%1/CONST%' OR p.surname LIKE '%1/C%'
	        OR p.surname LIKE '%Det 1/C Constable%' OR p.surname LIKE '%DET/CONST%' OR p.surname LIKE '%D/CONSTABLE%'
	        OR p.surname LIKE '%DETECTIVE S/C%' OR p.surname LIKE '%D/SGT%' OR p.surname LIKE '%T/CONSTABLE%'
	        OR p.surname LIKE '%T/CONST%'
        )
        -- 'u' followed by 3-5 digit number or 's0' followed by 5 digit number
        OR
        (
	        p.given_names LIKE '%u[0-9][0-9][0-9]%' OR p.given_names LIKE '%u[0-9][0-9][0-9][0-9]%' OR p.given_names LIKE '%u[0-9][0-9][0-9][0-9][0-9]%'
            OR p.given_names LIKE '%u [0-9][0-9][0-9]%' OR p.given_names LIKE '%u [0-9][0-9][0-9][0-9]%' OR p.given_names LIKE '%u [0-9][0-9][0-9][0-9][0-9]%'
            OR p.given_names LIKE '%s0[0-9][0-9][0-9][0-9][0-9]%'
	        OR p.surname LIKE '%u[0-9][0-9][0-9]%' OR p.surname LIKE '%u[0-9][0-9][0-9][0-9]%' OR p.surname LIKE '%u[0-9][0-9][0-9][0-9][0-9]%'
            OR p.surname LIKE '%u [0-9][0-9][0-9]%' OR p.surname LIKE '%u [0-9][0-9][0-9][0-9]%' OR p.surname LIKE '%u [0-9][0-9][0-9][0-9][0-9]%'
            OR p.surname LIKE '%s0[0-9][0-9][0-9][0-9][0-9]%'
        )
    )
)
SELECT pp.*
FROM cms.dbo.person pp
WHERE pp.id NOT IN (
    SELECT id FROM business_CTE
    UNION
    SELECT id FROM employee_CTE
) AND EXISTS ( -- Ensure only records found in cms_clean.dbo.person are included
        SELECT 1
        FROM cms_clean.dbo.person cp
        WHERE cp.id = pp.id
    )
"""

df_person_names = pd.read_sql(query_4, connection)

query_2 = """
WITH Business_CTE AS (
    SELECT
        p.id,
        1 AS is_business,
        NULL AS is_employee
    FROM cms.dbo.person p
    WHERE
		(
		p.given_names LIKE '%acn%[0-9]%' OR p.surname LIKE '%acn%[0-9]%'
		-- Check for 'PTY LTD' or 'P/L' anywhere in the text
		OR p.given_names LIKE '%PTY LTD%' OR p.surname LIKE '%PTY LTD%'
		OR p.given_names LIKE '%P/L%' OR p.surname LIKE '%P/L%'
		OR p.given_names LIKE '%pty limited%' OR p.surname LIKE '%pty limited%'
		-- Check for 'T/A' anywhere in the text
		OR p.given_names LIKE '%T/A%' OR p.surname LIKE '%T/A%'
		-- Check for 'LTD' or 'LIMITED'
		OR p.given_names LIKE '% LTD%' OR p.surname LIKE '% LTD%'
		OR p.given_names LIKE '%LIMITED%'	OR p.surname LIKE '%LIMITED%'
		-- Check for 'PTY'
		OR p.given_names LIKE '% PTY%' AND p.surname LIKE '% PTY%'
		-- Check for 'INC' OR 'INCORPORATED'
		OR p.given_names LIKE '% INC%' OR p.surname LIKE '% INC%'
		OR p.given_names LIKE '%INCORPORATED%' OR p.surname LIKE '%INCORPORATED%'
		-- Check for 'TASMANIA' OR 'AUSTRALIA'
		OR p.given_names LIKE '%TASMANIA%' OR p.surname LIKE '%TASMANIA%'
		OR p.given_names LIKE '%AUSTRALIA%' OR p.surname LIKE '%AUSTRALIA%'
		-- Check for other keywords
		OR p.given_names LIKE '%DEPARTMENT%' OR p.surname LIKE '%DEPARTMENT%'
		OR p.given_names LIKE '% CLUB%' OR p.surname LIKE '% CLUB%'
		OR p.given_names LIKE '% SERVICE%'	OR p.surname LIKE '% SERVICE%'
		OR p.given_names LIKE '% SPORTS%' OR p.surname LIKE '% SPORTS%'
		OR p.given_names LIKE '%HOTEL%' OR p.surname LIKE '%HOTEL%'
		OR p.given_names LIKE '%HOBART %'	OR p.surname LIKE '%HOBART %'
		OR p.given_names LIKE '% CHURCH %' OR p.surname LIKE '% CHURCH %'
		OR p.given_names LIKE '% SCHOOL%'	OR p.surname LIKE '% SCHOOL%'
		OR p.given_names LIKE '%COMMUNITY%' OR p.surname LIKE '%COMMUNITY%'
		OR p.given_names LIKE '%DEPT%' OR p.surname LIKE '%DEPT%'
		OR p.given_names LIKE '%HOUSING %' OR p.surname LIKE '%HOUSING %'
		OR p.given_names LIKE '%LAUNCESTON%' OR p.surname LIKE '%LAUNCESTON%'
		OR p.given_names LIKE '% COUNCIL%' OR p.surname LIKE '% COUNCIL%'
		OR p.given_names LIKE '%COUNCIL %' OR p.surname LIKE '%COUNCIL %'
		OR p.given_names LIKE '%PRIMARY%'	OR p.surname LIKE '%PRIMARY%'
		OR p.given_names LIKE '%GROUP%' OR p.surname LIKE '%GROUP%'
		OR p.given_names LIKE '% STATION%' OR p.surname LIKE '% STATION%'
		OR p.given_names LIKE '%SUPERMARKET%'	OR p.surname LIKE '%SUPERMARKET%'
		OR p.given_names LIKE '% DEVONPORT%' OR p.surname LIKE '% DEVONPORT%'
        OR p.given_names LIKE '%-DEVONPORT%' OR p.surname LIKE '%-DEVONPORT%'
		OR p.given_names LIKE '%PHARMACY%' OR p.surname LIKE '%PHARMACY%'
		OR p.given_names LIKE '%RESTAURANT%' OR p.surname LIKE '%RESTAURANT%'
		OR p.given_names LIKE '%ASSOCIATION%'	OR p.surname LIKE '%ASSOCIATION%'
		OR p.given_names LIKE '%ELECTRICAL %'	OR p.surname LIKE '%ELECTRICAL %'
		OR p.given_names LIKE '%COMPANY%'	OR p.surname LIKE '%COMPANY%'
		OR p.given_names LIKE '%CONSTRUCTION%' OR p.surname LIKE '%CONSTRUCTION%'
		OR p.given_names LIKE '%MANAGEMENT%' OR p.surname LIKE '%MANAGEMENT%'
		OR p.given_names LIKE '% BUILDING%' OR p.surname LIKE '% BUILDING%'
		OR p.given_names LIKE '%TRANSPORT%' OR p.surname LIKE '%TRANSPORT%'
		OR p.given_names LIKE '%WOOLWORTHS%' OR p.surname LIKE '%WOOLWORTHS%'
		OR p.given_names LIKE '%GLENORCHY%' OR p.surname LIKE '%GLENORCHY%'
		OR p.given_names LIKE '%SOUTHERN %' OR p.surname LIKE '%SOUTHERN %'
		OR p.given_names LIKE '%BAKERY%' OR p.surname LIKE '%BAKERY%'
		OR p.given_names LIKE '%SOCIETY%'	OR p.surname LIKE '%SOCIETY%'
		OR p.given_names LIKE '%SOLUTIONS%' OR p.surname LIKE '%SOLUTIONS%'
		OR p.given_names LIKE '%CONTRACTING%' OR p.surname LIKE '%CONTRACTING%'
		OR p.given_names LIKE '%DIVISON%'	OR p.surname LIKE '%DIVISON%'
		OR p.given_names LIKE '%FOUNDATION%' OR p.surname LIKE '%FOUNDATION%'
		OR p.given_names LIKE '%CENTRE%' OR p.surname LIKE '%CENTRE%'
		OR p.given_names LIKE '%EDUCATION%' OR p.surname LIKE '%EDUCATION%'
		OR p.given_names LIKE '%CORPORATION%' OR p.surname LIKE '%CORPORATION%'
		OR p.given_names LIKE '%CHEMIST%' OR p.surname LIKE '%CHEMIST%'
		OR p.given_names LIKE '%UNION%' OR p.surname LIKE '%UNION%'
		OR p.given_names LIKE '% COLLEGE%' OR p.surname LIKE '% COLLEGE%'
		OR p.given_names LIKE '%ASSOCIATES%' OR p.surname LIKE '%ASSOCIATES%'
		OR p.given_names LIKE '%JEWELLERS%' OR p.surname LIKE '%JEWELLERS%'
		OR p.given_names LIKE '%NURSERY%' OR p.surname LIKE '%NURSERY%'
		OR p.given_names LIKE '%BUSINESS%' OR p.surname LIKE '%BUSINESS%'
		OR p.given_names LIKE '%GOVERNMENT%' OR p.surname LIKE '%GOVERNMENT%'
		OR p.given_names LIKE '%INDUSTRIES%' OR p.surname LIKE '%INDUSTRIES%'
		OR p.given_names LIKE '%SALVATION%' OR p.surname LIKE '%SALVATION%'
		OR p.given_names LIKE '%HOLDINGS%' OR p.surname LIKE '%HOLDINGS%'
		OR p.given_names LIKE '%HOMES %' OR p.surname LIKE '%HOMES %'
		OR p.given_names LIKE '% BODY%' OR p.surname LIKE '% BODY%'
		OR p.given_names LIKE '%CLOTHING%' OR p.surname LIKE '%CLOTHING%'
		OR p.given_names LIKE '%DESIGN%' OR p.surname LIKE '%DESIGN%'
		OR p.given_names LIKE '%COUNTRY%' OR p.surname LIKE '%COUNTRY%'
		OR p.given_names LIKE '%GARDEN %' OR p.surname LIKE '%GARDEN %'
		OR p.given_names LIKE '%WAREHOUSE%' OR p.surname LIKE '%WAREHOUSE%'
		OR p.given_names LIKE '%NORTHERN%' OR p.surname LIKE '%NORTHERN%'
		OR p.given_names LIKE '%AUTOMOTIVE%' OR p.surname LIKE '%AUTOMOTIVE%'
		OR p.given_names LIKE '%PROPERTY%' OR p.surname LIKE '%PROPERTY%'
		OR p.given_names LIKE '% WORLD%' OR p.surname LIKE '% WORLD%'
		OR p.given_names LIKE '%RENTALS%' OR p.surname LIKE '%RENTALS%'
		OR p.given_names LIKE '%PLUMBING%' OR p.surname LIKE '%PLUMBING%'
		OR p.given_names LIKE '%HOSPITAL%' OR p.surname LIKE '%HOSPITAL%'
		OR p.given_names LIKE '%LIMITED%' OR p.surname LIKE '%LIMITED%'
		OR p.given_names LIKE '%LIQUOR%' OR p.surname LIKE '%LIQUOR%'
		OR p.given_names LIKE '%UNIVERSITY%' OR p.surname LIKE '%UNIVERSITY%'
		OR p.given_names LIKE '%SHOPPING%' OR p.surname LIKE '%SHOPPING%'
		OR p.given_names LIKE '%EXPRESS%' OR p.surname LIKE '%EXPRESS%'
		OR p.given_names LIKE '% FARM%' OR p.surname LIKE '% FARM%'
		OR p.given_names LIKE '%ENTERPRISES%' OR p.surname LIKE '%ENTERPRISES%'
		OR p.given_names LIKE '%CARS %' OR p.surname LIKE '%CARS %'
		OR p.given_names LIKE '%GALLERY%' OR p.surname LIKE '%GALLERY%'
		OR p.given_names LIKE '%STUDIO%' OR p.surname LIKE '%STUDIO%'
		OR p.given_names LIKE '%MOTEL%' OR p.surname LIKE '%MOTEL%'
		OR p.given_names LIKE '%DENTAL%' OR p.surname LIKE '%DENTAL%'
		OR p.given_names LIKE '%corporate%' OR p.surname LIKE '%corporate%'
		OR p.given_names LIKE '%HEALTH%' OR p.surname LIKE '%HEALTH%'
		)
		AND p.id != 594107
),
Employee_CTE AS (
    SELECT
        p.id,
        NULL AS is_business,
        1 AS is_employee
    FROM cms.dbo.person p
    WHERE
        (
		p.given_names LIKE '%A/SGT%' OR p.given_names LIKE '%A/SERGT%' OR p.given_names LIKE '%A/Sgtt%'
		OR p.given_names LIKE '%A/C SGT%' OR p.given_names LIKE '%ACT/SGT%' OR p.given_names LIKE '%A/G SGT%'
		OR p.given_names LIKE '%A/ SGT%' OR p.given_names LIKE '%A/S%' OR p.given_names LIKE '%A/Sergeant%'
		OR p.given_names LIKE '%S/Sgt%' OR p.given_names LIKE '%DET SGT%' OR p.given_names LIKE '%A/G INSPECTOR%' OR p.given_names LIKE '%A/INSP%' OR p.given_names LIKE '%S/Const%'
		OR p.given_names LIKE '%S/CONSTABLE%' OR p.given_names LIKE '%SEN/CONSTABLE%' OR p.given_names LIKE '%Senior/Constable%'
		OR p.given_names LIKE '%S/Cst%' OR p.given_names LIKE '%SNR/CONST%' OR p.given_names LIKE '%S/C%'
		OR p.given_names LIKE '%1/C Constable%' OR p.given_names LIKE '%1/C CONST%' OR p.given_names LIKE '%1/CONST%' OR p.given_names LIKE '%1/C%'
		OR p.given_names LIKE '%Det 1/C Constable%' OR p.given_names LIKE '%DET/CONST%' OR p.given_names LIKE '%D/CONSTABLE%'
		OR p.given_names LIKE '%DETECTIVE S/C%' OR p.given_names LIKE '%D/SGT%' OR p.given_names LIKE '%T/CONSTABLE%'
        OR p.given_names LIKE '%T/CONST%'
        OR p.given_names LIKE '%cons %'
        OR p.given_names LIKE '%cons. %'
        OR p.given_names LIKE '%const %'
        OR p.given_names LIKE '%const. %'
        OR p.given_names LIKE '%insp %'
        OR p.given_names LIKE '%insp. %'
        OR p.given_names LIKE '%sgt %'
        OR p.given_names LIKE '%sgt. %'
        OR p.given_names LIKE 'det %'
        OR p.given_names LIKE '%det. %'
        OR p.given_names LIKE '%C0NS%'
        OR p.given_names LIKE '%snr/%'
        OR p.given_names LIKE '%snr %'
        OR p.given_names like ('%CONSTABLE%')
        OR p.given_names = 'CONST'
        OR p.given_names = 'cons'
        OR p.given_names like ('%const.%')
        OR p.given_names like ('%(cons)%')
        OR p.given_names like ('%(const)%')
        OR p.given_names like ('%sergeant%')
        OR p.given_names like ('%SGT')
        OR p.given_names = 'sgt'
        OR p.given_names like ('%ACTING SGT%')
        OR p.given_names like ('%ACT SGT%')
        OR p.given_names like ('%insp.%')
        OR p.given_names like ('%INSPECTOR%')
        OR p.given_names like ('%(insp)%')
        OR p.given_names = 'insp'
        OR p.given_names like ('%DETECTIVE%')
        OR p.given_names like ('%det.%')
        OR p.given_names like ('%DET INSP%')
        OR p.given_names like ('%SEN CONST%')
        OR p.given_names like ('%SEN SGT%')
        OR p.given_names like ('%POLICE OFFICER%')
        OR p.given_names like ('%Trainee%')
        OR p.given_names like ('%(SGT)%')
        OR p.given_names like ('% SGT.')
        OR p.given_names = 'SGT.'
        OR p.given_names = 'SGT,'
        OR p.given_names like ('SGT, %')
        OR p.given_names like ('% CONST')
        OR p.given_names like ('%(DET CONST)%')
        OR p.given_names like ('% .CONST')
        OR p.given_names like ('CONST, %')
        OR p.given_names like ('%CONSTBALE %')
        OR p.given_names like ('%CONSTAVLE %')
        OR p.given_names like ('% CONS.')
        OR p.given_names like ('%CONSABLE %')
        OR p.given_names like ('%CONSTABALE %')
        OR p.given_names like ('%CONSTBLE %')
        OR p.given_names like ('%SENIOR CONSTALE %')
        OR p.given_names like ('%CONSTABEL %')
        OR p.given_names = 'CONS.'
        OR p.given_names = 'CONSTABLE'
        OR p.given_names like ('%TASMANIA POLICE%')
        OR p.given_names = 'POLICE'
        OR p.given_names like ('% POLICE')
        OR p.given_names like ('% INSP')
        OR p.given_names like ('INSP, %')
        OR p.given_names like ('%(SERGT)%')
        OR p.given_names like ('%DET CONST%')
        OR p.given_names like ('%SGT(%')
        OR p.given_names = 'CONST,'
        OR p.given_names LIKE ('%CONSTALBE %')
        OR p.given_names LIKE ('%Constale %')
        OR p.given_names LIKE ('%Constazble %')
        OR p.given_names LIKE ('%CONSR %')
        OR p.given_names LIKE ('%(CONS.)%')
        OR p.given_names LIKE ('%CONSTANLE %')
        OR p.surname like ('%(SGT)%')
        OR p.surname like ('% SGT.')
        OR p.surname = 'SGT.'
        OR p.surname = 'SGT,'
        OR p.surname like ('SGT, %')
        OR p.surname like ('% CONST')
        OR p.surname like ('%(DET CONST)%')
        OR p.surname like ('% .CONST')
        OR p.surname like ('CONST, %')
        OR p.surname like ('%CONSTBALE %')
        OR p.surname like ('%CONSTAVLE %')
        OR p.surname like ('% CONS.')
        OR p.surname like ('%CONSABLE %')
        OR p.surname like ('%CONSTABALE %')
        OR p.surname like ('%CONSTBLE %')
        OR p.surname like ('%SENIOR CONSTALE %')
        OR p.surname like ('%CONSTABEL %')
        OR p.surname = 'CONS.'
        OR p.surname = 'CONSTABLE'
        OR p.surname like ('%TASMANIA POLICE%')
        OR p.surname = 'POLICE'
        OR p.surname like ('% POLICE')
        OR p.surname like ('% INSP')
        OR p.surname like ('INSP, %')
        OR p.surname like ('%(SERGT)%')
        OR p.surname like ('%DET CONST%')
        OR p.surname like ('%SGT(%')
        OR p.surname = 'CONST,'
        OR p.surname LIKE ('%CONSTALBE %')
        OR p.surname LIKE ('%Constale %')
        OR p.surname LIKE ('%Constazble %')
        OR p.surname LIKE ('%CONSR %')
        OR p.surname LIKE ('%(CONS.)%')
        OR p.surname LIKE ('%CONSTANLE %')
        OR p.surname like ('%CONSTABLE%')
        OR p.surname = 'CONST'
        OR p.surname = 'cons'
        OR p.surname like ('%const.%')
        OR p.surname like ('%(cons)%')
        OR p.surname like ('%(const)%')
        OR p.surname like ('%sergeant%')
        OR p.surname like ('%SGT')
        OR p.surname = 'sgt'
        OR p.surname like ('%ACTING SGT%')
        OR p.surname like ('%ACT SGT%')
        OR p.surname like ('%insp.%')
        OR p.surname like ('%INSPECTOR%')
        OR p.surname like ('%(insp)%')
        OR p.surname = 'insp'
        OR p.surname like ('%DETECTIVE%')
        OR p.surname like ('%det.%')
        OR p.surname like ('%DET INSP%')
        OR p.surname like ('%SEN CONST%')
        OR p.surname like ('%SEN SGT%')
        OR p.surname like ('%POLICE OFFICER%')
        OR p.surname like ('%Trainee%')
        OR p.surname LIKE '%snr/%'
        OR p.surname LIKE '%snr %'
        OR p.surname LIKE '%C0NS%'
        OR p.surname LIKE '%cons %'
        OR p.surname LIKE '%cons. %'
        OR p.surname LIKE '%const %'
        OR p.surname LIKE '%const. %'
        OR p.surname LIKE '%insp %'
        OR p.surname LIKE '%insp. %'
        OR p.surname LIKE '%sgt %'
        OR p.surname LIKE '%sgt. %'
        OR p.surname LIKE 'det %'
        OR p.surname LIKE '%det. %'
		OR p.surname LIKE '%A/SGT%' OR p.surname LIKE '%A/SERGT%' OR p.surname LIKE '%A/Sgtt%'
		OR p.surname LIKE '%A/C SGT%' OR p.surname LIKE '%ACT/SGT%' OR p.surname LIKE '%A/G SGT%'
		OR p.surname LIKE '%A/ SGT%' OR p.surname LIKE '%A/S%' OR p.surname LIKE '%A/Sergeant%'
		OR p.surname LIKE '%S/Sgt%' OR p.surname LIKE '%DET SGT%' OR p.surname LIKE '%A/G INSPECTOR%' OR p.surname LIKE '%A/INSP%' OR p.surname LIKE '%S/Const%'
		OR p.surname LIKE '%S/CONSTABLE%' OR p.surname LIKE '%SEN/CONSTABLE%' OR p.surname LIKE '%Senior/Constable%'
		OR p.surname LIKE '%S/Cst%' OR p.surname LIKE '%SNR/CONST%' OR p.surname LIKE '%S/C%'
		OR p.surname LIKE '%1/C Constable%' OR p.surname LIKE '%1/C CONST%' OR p.surname LIKE '%1/CONST%' OR p.surname LIKE '%1/C%'
		OR p.surname LIKE '%Det 1/C Constable%' OR p.surname LIKE '%DET/CONST%' OR p.surname LIKE '%D/CONSTABLE%'
		OR p.surname LIKE '%DETECTIVE S/C%' OR p.surname LIKE '%D/SGT%' OR p.surname LIKE '%T/CONSTABLE%'
		OR p.surname LIKE '%T/CONST%'
	)
	-- 'u' followed by 3-5 digit number or 's0' followed by 5 digit number
	OR
	(
        p.given_names LIKE '%u[0-9][0-9][0-9]%' OR p.given_names LIKE '%u[0-9][0-9][0-9][0-9]%' OR p.given_names LIKE '%u[0-9][0-9][0-9][0-9][0-9]%'
        OR p.given_names LIKE '%u [0-9][0-9][0-9]%' OR p.given_names LIKE '%u [0-9][0-9][0-9][0-9]%' OR p.given_names LIKE '%u [0-9][0-9][0-9][0-9][0-9]%'
        OR p.given_names LIKE '%s0[0-9][0-9][0-9][0-9][0-9]%'
        OR p.surname LIKE '%u[0-9][0-9][0-9]%' OR p.surname LIKE '%u[0-9][0-9][0-9][0-9]%' OR p.surname LIKE '%u[0-9][0-9][0-9][0-9][0-9]%'
        OR p.surname LIKE '%u [0-9][0-9][0-9]%' OR p.surname LIKE '%u [0-9][0-9][0-9][0-9]%' OR p.surname LIKE '%u [0-9][0-9][0-9][0-9][0-9]%'
        OR p.surname LIKE '%s0[0-9][0-9][0-9][0-9][0-9]%'
	)
)
-- Combine both CTEs
SELECT
    p.id,
    COALESCE(b.is_business, e.is_business) AS is_business,
    COALESCE(e.is_employee, b.is_employee) AS is_employee
FROM (
    SELECT id FROM Business_CTE
    UNION
    SELECT id FROM Employee_CTE
) p
LEFT JOIN Business_CTE b ON p.id = b.id
LEFT JOIN Employee_CTE e ON p.id = e.id;
"""

# 11,184 employees and businesses in cms.dbo.person
df_person_emp_bus = pd.read_sql(query_2, connection)

# Close the connection
cursor.close()
connection.close()

# Script Purpose:
# Cleans name fields in person table, splits them out, extracts relevant information (alias) into a separate dataframe, and creates new person records where multiple people were recorded on the same record.
# Feature DQI:
# 32428, 32377, 32376, 32314, 31661, 31657, 31651, 31650, 31647, 31646, 31643
# Input: original person table
# Output: cleaned person table (cleaned given_names and surname with given_names split out into three columns along with a dataframe of aliases)

# 611,583 records
df_person_names_2 = df_person_names.copy()


# Function to update the is_business and is_employee columns in df_person_emp_bus based on the below id mappings.
def update_business_employee_flags(df):
    df_copy = df.copy()
    updates = {
        7453: (1, None),
        18580: (None, 1),
        29326: (None, 1),
        31751: (None, 1),
        34749: (None, 1),
        40443: (1, None),
        46836: (1, None),
        85192: (1, None),
        87412: (None, 1),
        117482: (1, None),
        120072: (1, None),
        163729: (1, None),
        216668: (1, None),
        296824: (1, None),
        406716: (1, None),
        407055: (1, None),
        407106: (1, None),
        407584: (1, None),
        408740: (1, None),
        408915: (1, None),
        417958: (1, None),
        432075: (1, None),
        477896: (1, None),
        482495: (1, None),
        554541: (1, None),
        564320: (1, None),
        608172: (1, None),
        666622: (1, None),
    }

    # Apply updates to the dataframe
    for person_id, (is_business, is_employee) in updates.items():
        df_copy.loc[df_copy["id"] == person_id, "is_business"] = is_business
        df_copy.loc[df_copy["id"] == person_id, "is_employee"] = is_employee
    return df_copy


df_person_emp_bus = update_business_employee_flags(df_person_emp_bus)


# Adding the is_business and is_employee columns to df_person_names_2
def merge_business_employee_flags(df, df_person_emp_bus):
    df_copy = df.copy()
    df_merged = df_copy.merge(df_person_emp_bus[["id", "is_business", "is_employee"]], on="id", how="left")
    return df_merged


# 611,583 records and 19 columns
# 9 with is_employee = 1 (one extra due to police being included in search) and 331 with is_business = 1
df_person_names_2 = merge_business_employee_flags(df_person_names_2, df_person_emp_bus)

"""
function that sets is_business or is_employee = 1 for the specified IDs and replaces / with a space (' ') in the surname column for specific IDs.
"""


def update_business_flags(df):
    df_copy = df.copy()
    # IDs that require is_business = 1 and '/' replacement in surname
    ids_replace_surname = {13526, 45574, 145623, 296566, 305662, 393450, 450821, 581798}

    # IDs that require only is_business = 1
    ids_set_business = {
        4950,
        3327,
        80995,
        95380,
        117077,
        163539,
        246135,
        269481,
        270157,
        287930,
        454590,
        481649,
        515316,
        515416,
        588158,
        641409,
        10614,
        15011,
        43736,
        53578,
        58475,
        61468,
        61478,
        121009,
        168163,
        197771,
        199583,
        187293,
        224875,
        238310,
        258619,
        276271,
        288137,
        384621,
        387680,
        507622,
        519769,
        114672,
    }

    # Subset of ids_set_business that require '&' -> 'and' replacement in surname
    ids_replace_ampersand = {
        4950,
        3327,
        80995,
        95380,
        117077,
        163539,
        246135,
        269481,
        270157,
        287930,
        454590,
        481649,
        515316,
        515416,
        588158,
        641409,
    }

    # IDs that require only is_employee = 1
    ids_employee = {255336}

    # Update is_employee to 1 for all relevant IDs
    df_copy.loc[df_copy["id"].isin(ids_employee), "is_employee"] = 1

    # Update is_business to 1 for all relevant IDs
    df_copy.loc[df_copy["id"].isin(ids_replace_surname | ids_set_business), "is_business"] = 1

    # Replace '/' with ' ' in surname for specific IDs
    df_copy.loc[df_copy["id"].isin(ids_replace_surname), "surname"] = df_copy.loc[
        df_copy["id"].isin(ids_replace_surname), "surname"
    ].str.replace("/", " ", regex=False)

    # Replace '&' with 'and' in surname for specific IDs
    df_copy.loc[df_copy["id"].isin(ids_replace_ampersand), "surname"] = df_copy.loc[
        df_copy["id"].isin(ids_replace_ampersand), "surname"
    ].str.replace("&", "and", regex=False)

    return df_copy


df_person_names_2 = update_business_flags(df_person_names_2)

"""
function that searches for and removes 'ESTATE OF' phrases in the given order from both the given_names and surname columns.
"""


def clean_estate_terms(df):
    df_copy = df.copy()
    # Define the ordered phrases to be removed (in correct sequence)
    estate_phrases = [
        "THE ESTATE OF THE LATE",
        "ESTATE OF THE LATE",
        "ESTATE OF LATE",
        "ESTATE OF THE",
        "DECEASED ESTATE OF",
        "ESTATE OF",
        "The Late",
    ]

    # Create a regex pattern that matches whole phrases
    pattern = r"\b(?:" + "|".join(re.escape(phrase) for phrase in estate_phrases) + r")\b"

    def remove_estate_terms(value):
        if not isinstance(value, str):  # Skip non-string values
            return value

        # Remove the phrases in the given order
        value = re.sub(pattern, "", value, flags=re.IGNORECASE)

        # Clean up multiple spaces that may have been left
        value = re.sub(r"\s+", " ", value).strip()

        return value

    # Apply the function to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(remove_estate_terms)
    df_copy["surname"] = df_copy["surname"].apply(remove_estate_terms)

    return df_copy


df_person_names_2 = clean_estate_terms(df_person_names_2)

# Function to identify and change records that start with "REG " and contain numbers in either the surname or given_names columns to NULL.
"""
Finds records with Rego numbers and makes them NULL.
Examples:
140244	REG CD6394
140245	REG CH5144
140246	REG DC7507
140247	REG LA0101
140249	REG XZA4578
140251	REG CJ9801
"""


def clean_reg_entries(df):
    df_copy = df.copy()
    # Regex pattern to match records starting with "REG " and containing numbers
    pattern = r"^REG .*?\d"

    # Replace matching records in the 'surname' column with NULL
    df_copy["surname"] = df_copy["surname"].apply(
        lambda x: None if isinstance(x, str) and pd.notna(x) and bool(re.match(pattern, x)) else x
    )

    # Replace matching records in the 'given_names' column with NULL
    df_copy["given_names"] = df_copy["given_names"].apply(
        lambda x: None if isinstance(x, str) and pd.notna(x) and bool(re.match(pattern, x)) else x
    )
    return df_copy


# Apply the function to the dataframe
df_person_names_2 = clean_reg_entries(df_person_names_2)

"""
Identifies records where given_names or surname start and end with either single (') or double (") quotes with
no other text (spaces and non-alphanumeric are okay) after the closing quote. If the condition is met, it removes the quotes.
Examples:
66710 - 'CLARY'
66711 - 'POM'[
52241 - "OWEN"

The following will be left as they are:
276285 - '
535341 - 'OTO'OTA
399845 - "RITA KAY" VESSEL
"""


def clean_quoted_names(df):
    df_copy = df.copy()

    def remove_surrounding_quotes(name):
        if isinstance(name, str):
            # Match names that start and end with quotes, allowing spaces and non-alphanumeric characters after the closing quote
            match = re.fullmatch(r'["\']([^"\']+)["\'][\s\W]*', name.strip())
            if match:
                return match.group(1)  # Return text inside quotes
        return name  # Return original if no change needed

    # Apply function to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(remove_surrounding_quotes)
    df_copy["surname"] = df_copy["surname"].apply(remove_surrounding_quotes)

    return df_copy


# Apply the function to the dataframe
df_person_names_2 = clean_quoted_names(df_person_names_2)

"""
function to process below replacing:
"`" (backtick) with a blank.
"–" (en dash) with "-" (hyphen).
"""


def clean_other_stuff(df):
    df_copy = df.copy()

    # function to clean individual string values
    def replace_chars(value):
        if isinstance(value, str):  # Process only string values
            return value.replace("`", "").replace("–", "-")
        return value  # Return as is for non-strings

    # Apply the function to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(replace_chars)
    df_copy["surname"] = df_copy["surname"].apply(replace_chars)
    return df_copy


# Apply the function to the dataframe
df_person_names_2 = clean_other_stuff(df_person_names_2)

"""
Function that:
Removes "REV.", "REV", "DR.", "DR", "SIR", "LORD", "MRS", "SISTER", "FATHER" only if they appear at the start of the name.
Removes "DOCTOR" and "REVEREND" anywhere in the column, regardless of position.
"""


def clean_other_stuff_2(df):
    df_copy = df.copy()

    def process_name(name):
        if not isinstance(name, str):  # Skip non-strings
            return name
        # Remove keywords anywhere in the string
        name = re.sub(r"\b(REVEREND|DOCTOR|Uses Name)\b", "", name, flags=re.IGNORECASE)
        # Remove prefixes if they appear at the start
        name = re.sub(r"^(REV\.?|DR\.?|SIR\.?|LORD\.?|FATHER\.?|SISTER\.?|MRS\.?)\s*", "", name, flags=re.IGNORECASE)
        return name.strip()

    # Apply processing to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(process_name)
    df_copy["surname"] = df_copy["surname"].apply(process_name)

    return df_copy


df_person_names_2 = clean_other_stuff_2(df_person_names_2)

"""
Function that:
Removes ACN that is followed by any numbers.
Replaces care of abbreviations with blank.
Replace police rank abbreviations with full form values.
"""


def process_person_names(df):
    # Make a copy of the dataframe
    df_copy = df.copy()

    # Make copies of given_names and surname columns
    df_copy["given_names_old"] = df_copy["given_names"]
    df_copy["surname_old"] = df_copy["surname"]

    # Remove 'ACN ' followed by numeric text
    def remove_acn(value):
        if not isinstance(value, str):  # Skip non-strings
            return value
        # Regex to match 'ACN ' followed by numeric text only
        return re.sub(r"\bACN (?=\d+\b)", "", value, flags=re.IGNORECASE)

    df_copy["given_names"] = df_copy["given_names"].apply(remove_acn)
    df_copy["surname"] = df_copy["surname"].apply(remove_acn)

    # Replace 'C/-', 'C/0', 'C/O', 'C/E', 'C/', 'C/ -' with blank
    def replace_c_slash(value):
        if not isinstance(value, str):  # Skip non-strings
            return value
        # Replace 'C/-', 'C/ -', 'C/0', 'C/O', 'C/E' with blank
        value = re.sub(r"C/ ?-|C/0|C/O|C/E|C-O", "", value, flags=re.IGNORECASE)
        # Replace any remaining 'C/' with blank
        value = re.sub(r"C/", "", value, flags=re.IGNORECASE)
        return value

    df_copy["given_names"] = df_copy["given_names"].apply(replace_c_slash)
    df_copy["surname"] = df_copy["surname"].apply(replace_c_slash)

    # Replace keywords with corresponding values
    replacements = {
        "P/L": "Pty Ltd",
        "T/A": "Trading As",
        "C0NS": "Constable",
        "SGT": "Sergeant",
        "A/SGT": "Acting Sergeant",
        "A/SERGT": "Acting Sergeant",
        "A/Sgtt": "Acting Sergeant",
        "A/C SGT": "Acting Sergeant",
        "ACT/SGT": "Acting Sergeant",
        "A/G SGT": "Acting Sergeant",
        "A/ SGT": "Acting Sergeant",
        "A/Sergeant": "Acting Sergeant",
        "S/Sgt": "Senior Sergeant",
        "DET SGT": "Detective Sergeant",
        "A/G INSPECTOR": "Acting Inspector",
        "A/INSP": "Acting Inspector",
        "S/Const": "Senior Constable",
        "S/CONSTABLE": "Senior Constable",
        "SEN/CONSTABLE": "Senior Constable",
        "Senior/Constable": "Senior Constable",
        "SNR/ CONST": "Senior Constable",
        "S/Cst": "Senior Constable",
        "SNR/CONST": "Senior Constable",
        "Det 1/C Constable": "Detective First class Constable",
        "1/C Constable": "First class Constable",
        "1/C CONST": "First class Constable",
        "1/CONST": "First class Constable",
        "DET/CONST": "Detective Constable",
        "DET CONST": "Detective Constable",
        "D/CONSTABLE": "Detective Constable",
        "DETECTIVE S/C": "Detective Senior Constable",
        "DET SEN CONST": "Detective Senior Constable",
        "D/SGT": "Detective Sergeant",
        "T/CONSTABLE": "Trainee Constable",
        "T/CONST": "Trainee Constable",
        "1/C": "First class",
        "S/C": "Senior Constable",
        "A/S": "Acting Sergeant",
    }

    def replace_keywords(value):
        if not isinstance(value, str):  # Skip non-strings
            return value
        for old, new in replacements.items():
            value = re.sub(rf"\b{re.escape(old)}\b", new, value, flags=re.IGNORECASE)
        return value

    df_copy["given_names"] = df_copy["given_names"].apply(replace_keywords)
    df_copy["surname"] = df_copy["surname"].apply(replace_keywords)

    return df_copy


df_person_names_processed = process_person_names(df_person_names_2)

"""
Function to move all employee ranks to the end of the given_names record.
For example:
given_names - David
surname - Smith Detective Sergeant

becomes:
given_names - David Detective Sergeant
surname - Smith
"""


def move_titles_to_given_names(df):
    df_copy = df.copy()
    # Define the keywords in the correct order to prevent splitting composite titles
    keywords = [
        "Detective Senior Constable",
        "Detective First class Constable",
        "First class Constable",
        "Detective Constable",
        "Detective Sergeant",
        "Senior Sergeant",
        "Acting Sergeant",
        "Acting Inspector",
        "Senior Constable",
        "Constable",
        "Sergeant",
        "Trainee Constable",
    ]

    # Create a regex pattern to match any of the keywords
    pattern = r"\b(?:" + "|".join(re.escape(word) for word in keywords) + r")\b"

    def process_row(row):
        given_names = row["given_names"]
        surname = row["surname"]

        if not isinstance(given_names, str):
            given_names = ""
        if not isinstance(surname, str):
            surname = ""

        # Find all keywords in the combined text
        combined_text = f"{given_names} {surname}"
        matches = re.findall(pattern, combined_text, flags=re.IGNORECASE)

        if matches:
            # Remove keywords from both columns
            cleaned_given_names = re.sub(pattern, "", given_names, flags=re.IGNORECASE).strip()
            cleaned_surname = re.sub(pattern, "", surname, flags=re.IGNORECASE).strip()

            # Append the keywords (in original order) to the given_names
            new_given_names = f"{cleaned_given_names} {' '.join(matches)}".strip()

            return pd.Series([new_given_names, cleaned_surname])
        else:
            return pd.Series([given_names, surname])

    # Apply the function to the dataframe
    df_copy[["given_names", "surname"]] = df_copy.apply(process_row, axis=1)

    return df_copy


df_person_names_processed_2 = move_titles_to_given_names(df_person_names_processed)

"""
function to make records NULL if the given_names or surname column contains whole-word matches of any of the specified keywords.
Skips processing any record if given_names or surname contains an opening ( or closing ).
Manually updates given_names or surname for certain records.
"""


def nullify_if_contains_keywords(df):
    df_copy = df.copy()
    # keywords list
    keywords = [
        "unknown",
        "OFFENDER",
        "OFFENDERS",
        "OFFENDERS.GIRLFRIEND",
        "VICTIM",
        "VICTIMS",
        "VICTIMLESS",
        "ANONYMOUS",
        "UNKNOWN",
        "NUMEROUS",
        "ANONYMOUSE",
        "NOT KNOWN",
        "PROVIDED",
        "DETAILS",
        "Manager",
        "AT THIS",
        "TO BE ADVISED",
        "TO ADVISE",
        "WILL ADVISE",
        "ADVISED",
        "CONFIRMED",
        "UNCONFIRMED",
        "tba",
        "FROM CNR",
        "FEMALE",
        "PARENTS",
        "WIFE",
        "EMPLOYEE",
        "unable",
        "WITNESS",
        "WITNESSES",
        "VARIOUS",
        "Unnamed",
        "SURNAME",
        "TBC",
        "(Tbc)",
        "Unk",
        "persons",
        "U/K",
        "u/lk",
        "N/K",
        "U/KNOWN",
        "N/A",
        "K/N",
        "N/A.",
        "M/O",
        "N/K/",
        "M/V",
    ]

    # Create a regex pattern to match whole words (case-insensitive)
    pattern = r"\b(?:" + "|".join(re.escape(word) for word in keywords) + r")\b"

    def process_column(value):
        if not isinstance(value, str):
            return value  # Skip non-strings

        # Skip processing if the column contains '(' or ')'
        if "(" in value or ")" in value:
            return value

        # If keyword is found as a whole word, replace the value with NULL
        if re.search(pattern, value, flags=re.IGNORECASE):
            return None

        return value  # Keep original value if no match

    # Apply the function to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(process_column)
    df_copy["surname"] = df_copy["surname"].apply(process_column)

    # Manually update the given_names or surname for certain records
    df_copy.loc[df_copy["id"] == 49546, "given_names"] = "YVONNE"
    df_copy.loc[df_copy["id"] == 4796, "surname"] = None
    df_copy.loc[df_copy["id"] == 100421, "surname"] = None
    df_copy.loc[df_copy["id"] == 176562, "surname"] = None
    df_copy.loc[df_copy["id"] == 221451, "surname"] = None
    df_copy.loc[df_copy["id"] == 317088, "surname"] = None
    df_copy.loc[df_copy["id"] == 570249, "surname"] = None
    df_copy.loc[df_copy["id"] == 581296, "surname"] = None
    df_copy.loc[df_copy["id"] == 664243, "surname"] = None
    df_copy.loc[df_copy["id"] == 676898, "surname"] = None
    df_copy.loc[df_copy["id"] == 50887, "surname"] = None
    df_copy.loc[df_copy["id"] == 324811, "surname"] = None
    df_copy.loc[df_copy["id"] == 451577, "surname"] = None
    df_copy.loc[df_copy["id"] == 69204, "surname"] = None
    df_copy.loc[df_copy["id"] == 324934, "surname"] = None
    df_copy.loc[df_copy["id"] == 384602, "surname"] = None
    df_copy.loc[df_copy["id"] == 176562, "surname"] = None
    df_copy.loc[df_copy["id"] == 676898, "surname"] = None
    df_copy.loc[df_copy["id"] == 303319, "surname"] = None
    df_copy.loc[df_copy["id"] == 324934, "surname"] = None
    df_copy.loc[df_copy["id"] == 332109, "surname"] = None
    df_copy.loc[df_copy["id"] == 328805, "surname"] = None
    df_copy.loc[df_copy["id"] == 384602, "surname"] = None
    df_copy.loc[df_copy["id"] == 384499, "surname"] = None
    df_copy.loc[df_copy["id"] == 332109, "surname"] = None
    df_copy.loc[df_copy["id"] == 87032, "surname"] = None
    df_copy.loc[df_copy["id"] == 622997, "surname"] = None
    df_copy.loc[df_copy["id"] == 581296, "surname"] = None
    df_copy.loc[df_copy["id"] == 581296, "given_names"] = None
    df_copy.loc[df_copy["id"] == 195650, "given_names"] = None
    df_copy.loc[df_copy["id"] == 271206, "given_names"] = None
    df_copy.loc[df_copy["id"] == 266328, "given_names"] = "MARK"
    df_copy.loc[df_copy["id"] == 150914, "surname"] = "SHORTLAND AKA HODGETTS"
    df_copy.loc[df_copy["id"] == 159144, "given_names"] = "MR & MRS"
    df_copy.loc[df_copy["id"] == 208998, "surname"] = "sullivan aka jones"
    df_copy.loc[df_copy["id"] == 144475, "surname"] = "O SHANNESSEY"
    df_copy.loc[df_copy["id"] == 548874, "surname"] = "O'DONOGHUE"
    df_copy.loc[df_copy["id"] == 158774, "surname"] = "RILEY MCDONALD"
    df_copy.loc[df_copy["id"] == 364577, "surname"] = "AIMERS MCGUINNESS"
    df_copy.loc[df_copy["id"] == 167946, "given_names"] = "DAVID RUSSELL MCCALL"
    df_copy.loc[df_copy["id"] == 301034, "given_names"] = "IAN WILLIAM MCDONALD"

    return df_copy


df_person_names_processed_3 = nullify_if_contains_keywords(df_person_names_processed_2)

"""
function that removes an ampersand (&) at the end of values in the given_names and surname columns.
"""


def remove_trailing_ampersand(df):
    df_copy = df.copy()

    def clean_value(value):
        if isinstance(value, str):
            return re.sub(r"&\s*$", "", value).strip()  # Remove '&' at the end
        return value

    # Apply to both columns
    df_copy["given_names"] = df_copy["given_names"].apply(clean_value)
    df_copy["surname"] = df_copy["surname"].apply(clean_value)

    return df_copy


df_person_names_processed_3 = remove_trailing_ampersand(df_person_names_processed_3)

##########################################
##########################################
"""
The clean_names() function processes and cleans the given_names and surname columns.

1) Handling Special Cases
Extracts and removes A.K.A. (A.K.A, Also Known As) references
Detects A.K.A. occurrences and moves them to a separate cleaned text column.
Removes "also known as ...", "known as ...", "UNKNOWN AS THIS TIME ...", etc.
Handles Date of Birth (DOB) references
Removes text starting with "DOB:..." and "DOB <number>".
Removes non-name markers
Detects age references ("25yrs", "30 years", etc.) and removes them.
Matches and removes unwanted placeholders ("U/K", "N/A", "Not", "N/K", etc.).

2) Formatting and Standardizing Text
Fixes special characters
Removes unwanted brackets ( ... ), quotes "...", and apostrophes between words.
Converts "O " to "O'" (e.g., "O Connor" → "O'Connor").
Replaces spaces around hyphens (e.g., "Smith - Brown" → "Smith-Brown").
Standardizes "Junior" and "Jr" → "Jnr".

3) Extracting Valid Names
Keeps only alphabetic and hyphenated names
Extracts valid names using regex:
Example:
Input: "Dr. John (aka Jack) Smith - 25yrs"
Output: "John Smith"
Removes non-name characters
Strips out anything that isn't a name (e.g., numbers, special symbols).

4) Handling Remaining Text
Stores "removed text" in a separate output column
Collects all removed parts into a separate column (removed_text).
Extracts extra information separately (..._Extra columns)
Stores text like "aka", "DOB", "years", etc., in a separate column.
"""


def clean_names(column_name):
    # Patterns for extraction and special handling
    pattern_brackets_and_quotes = re.compile(r"\(.*?\)|\".*?\"")
    pattern_valid = re.compile(r"([A-Za-z]+(?:[-' ][A-Za-z]+)*)")
    pattern_remove_directly = re.compile(
        r"^(U/K|N/A|N/A\.|N/K|N/K/|K/N|u/lk|N\.K|N\?K|persons u/k|Not|/k/|N\.A\.)$|^.{2,3}/$", re.IGNORECASE
    )
    pattern_years = re.compile(r"\b\d+yrs\b", re.IGNORECASE)
    pattern_also_known_as = re.compile(r"also known as .*?(?=$|[^a-zA-Z\'\- ])", re.IGNORECASE)
    pattern_unknown_as = re.compile(r"UNKNOWN AS THIS TIME .*?(?=$|[^a-zA-Z\'\- ])", re.IGNORECASE)
    pattern_known_as = re.compile(r"known as .*?(?=$|[^a-zA-Z\'\- ])", re.IGNORECASE)
    pattern_dob = re.compile(r"DOB:.*$", re.IGNORECASE)
    pattern_dob_number = re.compile(r"DOB\s+\d", re.IGNORECASE)
    # pattern ensures 'aka' is either at the start of the string or preceded by a non-alphabetical character
    pattern_aka = re.compile(r"(?:\b|\W)aka\s+(.*)$", re.IGNORECASE)
    pattern_numeral_years = re.compile(r"\b\d+\s*years\b", re.IGNORECASE)
    pattern_numeralyears = re.compile(r"\b\d+years\b", re.IGNORECASE)
    pattern_quotes_in_words = re.compile(r'(?<=\w)["’,‘”`“](?=\w)')
    pattern_space_around_hyphen = re.compile(r"\s*-\s*")
    pattern_o_space = re.compile(r"\bO\s(?=\w)")
    pattern_apostrophe_word_space = re.compile(r"(?<=\w)'\s(?=\w)")
    pattern_between_apostrophes = re.compile(r"'([^']*)'")
    pattern_junior_jr = re.compile(r"\bjunior\b|\bjr\b", re.IGNORECASE)
    pattern_senior_jr = re.compile(r"\bsenior\b", re.IGNORECASE)
    pattern_aka_dot_explicit = re.compile(r"A\.K\.A\.\s*(.*)", re.IGNORECASE)
    pattern_aka_explicit = re.compile(r"A\.K\.A\s*(.*)", re.IGNORECASE)

    def clean_name(name):
        if pd.isna(name):
            return "", "", name

        # Extract and handle A.K.A.
        aka_dot_match = pattern_aka_dot_explicit.search(name)
        if aka_dot_match:
            extra_aka_dot = aka_dot_match.group(1)  # Capture everything after A.K.A.
            # Remove the A.K.A. part from the name
            name = pattern_aka_dot_explicit.sub("", name)
            removed_aka_dot = "A.K.A."
        else:
            extra_aka_dot = ""
            removed_aka_dot = ""

        # Extract and handle A.K.A
        aka_match = pattern_aka_explicit.search(name)
        if aka_match:
            extra_aka = aka_match.group(1)  # Capture everything after A.K.A.
            # Remove the A.K.A part from the name
            name = pattern_aka_explicit.sub("", name)
            removed_aka = "A.K.A"
        else:
            extra_aka = ""
            removed_aka = ""

        # Apply all regex removals
        dob_text = pattern_dob.findall(name)
        name = pattern_dob.sub("", name)
        dob_text_num = pattern_dob_number.findall(name)
        name = pattern_dob_number.sub("", name)
        aka_text = pattern_aka.findall(name)
        name = pattern_aka.sub("", name)
        numeral_years_text = pattern_numeral_years.findall(name)
        name = pattern_numeral_years.sub("", name)
        numeralyears_text = pattern_numeralyears.findall(name)
        name = pattern_numeralyears.sub("", name)
        name = pattern_space_around_hyphen.sub("-", name)

        # Track and replace special quotes or commas in words
        special_chars_removed = pattern_quotes_in_words.findall(name)
        name = pattern_quotes_in_words.sub("'", name)  # Replace them with apostrophes
        also_known_as_text = pattern_also_known_as.findall(name)
        name = pattern_also_known_as.sub("", name)
        unknown_as_text = pattern_unknown_as.findall(name)
        name = pattern_unknown_as.sub("", name)
        known_as_text = pattern_known_as.findall(name)
        name = pattern_known_as.sub("", name)
        years_text = pattern_years.findall(name)
        name = pattern_years.sub("", name)

        # Replace "O " with "O'"
        name = pattern_o_space.sub("O'", name)
        # Remove apostrophe between word and space
        apostrophe_removed = pattern_apostrophe_word_space.findall(name)
        name = pattern_apostrophe_word_space.sub(" ", name)
        # Remove text between apostrophes
        text_between_apostrophes = pattern_between_apostrophes.findall(name)
        name = pattern_between_apostrophes.sub("", name)
        # Replace "junior" or "jr" with "Jnr"
        junior_to_jnr = pattern_junior_jr.findall(name)
        name = pattern_junior_jr.sub("Jnr", name)
        # Replace "senior" with "Snr"
        senior_to_snr = pattern_senior_jr.findall(name)
        name = pattern_senior_jr.sub("Snr", name)

        if pattern_remove_directly.match(name):
            return "", "", name  # Handle full removals directly

        extra = pattern_brackets_and_quotes.findall(name)
        name = pattern_brackets_and_quotes.sub("", name)

        valid_parts = pattern_valid.findall(name)
        cleaned_name = " ".join(valid_parts)

        remaining_text = pattern_valid.sub("", name)
        removed_text = (
            " ".join(
                [removed_aka_dot]
                + [removed_aka]
                + extra
                + dob_text
                + dob_text_num
                + aka_text
                + numeral_years_text
                + numeralyears_text
                + years_text
                + unknown_as_text
                + also_known_as_text
                + known_as_text
                + special_chars_removed
                + apostrophe_removed
                + text_between_apostrophes
                + junior_to_jnr
                + senior_to_snr
            )
            + " "
            + remaining_text.strip()
        )
        extra_cleaned = " ".join(
            [extra_aka_dot]
            + [extra_aka]
            + extra
            + aka_text
            + unknown_as_text
            + also_known_as_text
            + known_as_text
            + text_between_apostrophes
        ).strip()

        return cleaned_name, extra_cleaned, removed_text.strip()

    # Apply the cleaning function
    cleaned_names, extra_texts, removed_texts = zip(*column_name.apply(clean_name))
    return pd.Series(cleaned_names), pd.Series(extra_texts), pd.Series(removed_texts)


# 611,583 records and 27 columns
df_final_2 = df_person_names_processed_3.copy()

# Apply cleaning to columns
(
    df_final_2["Cleaned_Given_Names"],
    df_final_2["Cleaned_Given_Names_Extra"],
    df_final_2["Removed_Given_Names"],
) = clean_names(df_final_2["given_names"])
(
    df_final_2["Cleaned_Surname"],
    df_final_2["Cleaned_Surname_Extra"],
    df_final_2["Removed_Surname"],
) = clean_names(df_final_2["surname"])

"""
This function cleans and processes _Extra columns with following:

Removes age patterns:
If the text contains age-related patterns like "2yo", "2 YO", "2 y.o", "2 y/o", the entire entry is cleared ("").

Removes unwanted special characters:
Deletes punctuation marks: ; " . : , / [ ] ? and back slashes too while keeping text intact.

Removes numerals and certain keywords:
Eliminates numbers and specific phrases like:
    "a k a", "also known as", "known as", "english name".
    Allows for multiple spaces between words.

Cleans hyphens and apostrophes:
Removes hyphens/apostrophes:
    If surrounded by spaces (- or ').
    If they appear at the start or end of the text.

Removes entries starting with "U" followed by Numbers:
If the text begins with "U12345", "u567", etc., the entry is cleared.

Removes Entries Containing Specific Keywords:
If the text contains certain keywords, the entire entry is cleared.
"""


def process_extra_columns(text):
    # Check if the input is valid
    if not isinstance(text, str):
        return text

    # Remove entire entries containing age patterns like '2yo', '2 YO', etc.
    if re.search(r"\d+\s*(yo|y\.o|y/o)", text, flags=re.IGNORECASE):
        return ""

    # Remove unwanted characters, but keep text within valid context
    text = re.sub(r'([();".:,/\[\]\\\?\]])', "", text)

    # Remove numerals and certain keywords, allowing for multiple spaces between words
    text = re.sub(
        r"\b\d+|also\s*known\s*as|known\s*as|english\s*name\b",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # Remove a single hyphen or apostrophe surrounded by spaces on one or both sides and hyphens or apostrophes at the start or end of a string
    text = re.sub(r"(?<=\s)[-'\s]|[\'-](?=\s)|^[-']+|[-']+$", "", text)

    # Remove entries starting with 'U' followed by numbers (case-insensitive)
    text = re.sub(r"\bU\d+", "", text, flags=re.IGNORECASE)

    # Keywords to search and remove entire entries if matched
    keywords = [
        "provided",
        "medically",
        "CANADA",
        "Gateway",
        "TIME",
        "manager",
        "Prefers",
        "SECURITY",
        "STAFF",
        "LTD",
        "CIB",
        "ADVISE",
        "ADVISED",
        "MRS",
        "DON'T",
        "PROFILE",
        "confirmed",
        "aged",
        "police",
        "tba",
        "trainee",
        "boy",
        "LADY",
        "ADMINISTRATOR",
        "OFFICE",
        "NOT",
        "DR",
        "JUSTICE",
        "MEDICAL",
        "Trading",
        "VOLUNTEERS",
        "UNIT",
        "CONTROLLER",
        "female",
        "male",
        "previously",
        "deceased",
        "biological",
        "victim",
        "WIFE",
        "TEACHER",
        "SISTER",
        "SON",
        "EMPLOYEE",
        "SERVICES",
        "STARTUP",
        "FLEET",
        "NEIGHBOUR",
        "INC",
        "Towel",
        "unknown",
        "old",
        "unable",
        "confirm",
        "dob",
        "pronounced",
        "REV",
        "NO",
        "Fire",
        "DOC",
        "SSO",
        "Similar",
        "Address",
        "AUS",
        "TAS",
        "AUST",
        "HOBART",
        "WITNESS",
        "Cfs",
        "years",
        "incident",
        "relationship",
        "father",
        "the",
        "road",
        "officer",
        "CAPTAIN",
        "DEC",
        "TASMANIA",
        "EURO",
        "GENERAL",
        "VARIOUS",
        "OPERATIONS",
        "REFUSED",
        "step",
        "mother",
        "notes",
        "select",
        "please",
        "possibly",
        "OFFENDER",
        "PARTY",
        "DIVISON",  # codespell:ignore
        "KOREA",
        "GERMANY",
        "SEARCH",
        "SUPERINTENDENT",
        "CORPORATE",
        "CONTRACTORS",
        "unkown",  # codespell:ignore
        "Unnamed",
        "SERVICE",
        "TASMANIA",
        "UNKNOQN",
        "MAYBE",
        "GIVEN",
        "NAMES",
        "PRESIDENT",
        "REVEREND",
        "PRISON",
        "AUSTRALIA",
        "PROPERTY",
        "DEMOLITION",
        "RENTAL",
        "DUTY",
        "two",
        "TEST",
        "One",
        "OFFENDERONE",
        "VICTIMONE",
        "testone",
        "OFFENDERTWO",
        "TFS",
        "GIRLFRIEND",
        "LADY",
        "nil",
        "SURNAME",
        "ROL_COMPNY",
        "BIRTH",
        "WORKER",
        "TBC",
        "AVIS",
        "testtwo",
        "VICTIMTWO",
        "Testa",
        "Testb",
        "Mobile",
        "Data",
        "Tester",
        "three",
        "Named",
        "HOWEVER",
        "OWNER-VEH",
        "BUILDERS",
    ]
    # Ensure whole-word matches for keywords
    if any(re.search(rf"\b{keyword}\b", text, flags=re.IGNORECASE) for keyword in keywords):
        return ""

    # Keywords to remove individually within text (without removing the entire entry)
    keywords_to_remove_in_text = ["AKA", "SIR", "SNR", "SENIOR", "JNR", "Junior", "Use", "ALIAS", "Uses", "NEE"]
    text = re.sub(
        r"\b(?:" + "|".join(re.escape(word) for word in keywords_to_remove_in_text) + r")\b",
        "",
        text,
        flags=re.IGNORECASE,
    )

    return text.strip()


df_final_3 = df_final_2.copy()
# Apply additional processing to "_Extra" columns
df_final_3["Cleaned_Given_Names_Extra"] = df_final_3["Cleaned_Given_Names_Extra"].apply(process_extra_columns)
df_final_3["Cleaned_Surname_Extra"] = df_final_3["Cleaned_Surname_Extra"].apply(process_extra_columns)

"""
Handles records containing slashes (/) appropriately.
Perform the appropriate splitting based on the first /, and update the relevant columns accordingly.
"""


def process_slash_entries(df):
    df_copy = df.copy()
    # Define the regex pattern for finding slashes surrounded by alpha characters
    pattern_slash = re.compile(r"(?<![\W\d])\s*/\s*(?![\W\d])")

    # Define the entries to exclude and substrings that, if present, should exclude the record
    exclusions_exact = {
        "N/A",
        "N/A.",
        " N/A",
        "N/K",
        "U/K",
        "n/k",
        "N/K/",
        "u/k",
        "n/a",
        "K/N",
        "u/lk",
        "REGISTRAR OF M/V",
        "U/KNOWN",
        "SEE M/O",
        "persons u/k",
        "Parent/Child",
    }
    exclusions_substring = [
        "y/o",
        "(",
        "- C/E",
        "VICTIM/S",
        "PERSON/S",
        " U/K",
        "n/k",
        "A/INSP",
        "SNR/",
        "A/SGT",
        " O/N",
        "trading",
        "SERVICES",
        "UNKNOWN",
        "VICTIM",
        "HEALTH",
        " u/k",
        " M/O",
        " M/V",
        "europcar",
    ]

    # Convert exclusions_exact to lowercase
    exclusions_exact_lower = {item.lower() for item in exclusions_exact}
    exclusions_substring_lower = [item.lower() for item in exclusions_substring]

    # Function to process individual rows
    def process_row(row):
        # Process the given_names column
        if pd.notna(row["given_names"]):
            given_names_lower = row["given_names"].lower()
            if given_names_lower not in exclusions_exact_lower and not any(
                sub in given_names_lower for sub in exclusions_substring_lower
            ):
                # Find the first occurrence of a slash pattern
                match = pattern_slash.search(row["given_names"])
                if match:
                    # Everything before the first '/'
                    row["Cleaned_Given_Names"] = row["given_names"][: match.start()].strip()
                    # Everything after the first '/', including '/'
                    row["Removed_Given_Names"] = row["given_names"][match.start() :].strip()
                    # Everything after the first '/'
                    row["Cleaned_Given_Names_Extra"] = row["given_names"][match.start() + 1 :].strip()

        # Process the surname column
        if pd.notna(row["surname"]):
            surname_lower = row["surname"].lower()
            if surname_lower not in exclusions_exact_lower and not any(
                sub in surname_lower for sub in exclusions_substring_lower
            ):
                # Find the first occurrence of a slash pattern
                match = pattern_slash.search(row["surname"])
                if match:
                    # Everything before the first '/'
                    row["Cleaned_Surname"] = row["surname"][: match.start()].strip()
                    # Everything after the first '/', including '/'
                    row["Removed_Surname"] = row["surname"][match.start() :].strip()
                    # Everything after the first '/'
                    row["Cleaned_Surname_Extra"] = row["surname"][match.start() + 1 :].strip()

        return row

    # Apply the function to each row of the DataFrame
    df_copy = df_copy.apply(process_row, axis=1)
    return df_copy


df_final_4 = df_final_3.copy()
# Process the DataFrame
df_final_4 = process_slash_entries(df_final_4)


# Cleans up the '/' in the '..._Extra' columns
def clean_slash(df):
    df_copy = df.copy()

    # function that processes the Cleaned_Surname_Extra column
    def process_extra(column):
        if pd.isna(column):
            return column  # Return NaN as is

        # Remove any '/ ' (forward slash followed by a space)
        column = column.replace("/ ", "")

        # Replace any '/' with ', '
        column = column.replace("/", ", ")

        # Remove leading/trailing spaces and replace multiple spaces with a single space
        column = re.sub(r"\s+", " ", column).strip()

        return column

    # Apply the function to the columns
    df_copy["Cleaned_Given_Names_Extra"] = df_copy["Cleaned_Given_Names_Extra"].apply(process_extra)
    df_copy["Cleaned_Surname_Extra"] = df_copy["Cleaned_Surname_Extra"].apply(process_extra)
    return df_copy


df_final_4 = clean_slash(df_final_4)

"""
Handles records containing colon (:) appropriately.
Setting the appropriate splitting, and update the relevant columns accordingly.
"""


def update_person_names(df):
    df_copy = df.copy()
    updates = {
        199757: ("JODIE", "SHANE", "MACDONALD", "LEWIS"),
        200011: ("VALERY", "DON", "KULLRICH", "PITCHER"),
        200740: ("RACHAEL VANESSA", "JOHN WILLIAM", "RICHARDSON", "BRADFORD"),
        201582: ("SHARON ANN MARIE", "MICHAEL JOHN", "WILSON", "SHEPPARD"),
        193700: ("KIM MAREE", "SHANE LEE", "DALY", "SMITH"),
        212246: ("SUSAN", "COLIN", "ROGERS", "SHERRINGTON"),
        213127: ("SUZANNE MAREE", "STEPHEN ANTHONY", "WILLIS", "MALLICK"),
        214695: ("MERRYN LOUISE", "", "DUHIG", "DUHIJ"),
        223346: ("MARIO LUIGI", "", "STEGEMAN", ""),
        224103: ("ELIZABETH DARDIS", "BRUCE ANTHONY", "WESTBROOK", "TURNER"),
        226288: ("KYM", "JASON", "WOLTER-SALER", "HALL"),
        230396: ("JENIFER GAYE", "RHONDA DEANNE", "CHILCOTT", "TALBOT"),
        232943: ("CASANDRA ANNE", "STUART", "MCKAY", "MCPHIE"),
        233328: ("LEEANDA RAE", "", "BRUMBY", ""),
        234127: ("CLARE", "SIMON", "DOUGLAS", "CASH"),
        237098: ("NICOLE", "MARTIN", "LEACH", "STIRLING"),
        237950: ("TRACEY JANE", "TREVOR EARLE", "JARVIS", "KAINE"),
        241905: ("HELEN MARGARET", "GLEN MAXWELL", "SANDERS", "TOSH"),
        249471: ("CAROLINE ANN", "GEOFFREY CHARLES", "ASH", "WICKHAM"),
        250963: ("LINDA ANNE", "MICHAEL WAYNE", "BOWEN", "SMITH"),
        252403: ("LAURA ELIZABETH", "IAN JAMES", "SMITH", "LEWIS"),
        245118: ("LYNETTE", "CHRISTOPHER MARK", "BLOOMFIELD", "DWYER"),
        246386: ("MARIA JOSEPHINE", "ANDREW WILLIAM RONALD", "FLETCHER", "JONES"),
        246622: ("EMELENE", "ANDREW", "WALKER", "LARNER"),
        254776: ("PATRICIA JOY", "SHAUN PATRICK", "HILL", "RICKARDS"),
        256521: ("TINA", "KELVIN", "MAWER", "MARKHAM"),
        256615: ("AMANDA JANE", "SIMON ANDREW", "PREDDY", "HOWARD"),
        258633: ("GEOFF COOK CARS", "", "", ""),
        259315: ("NATALIE", "PAMELA GAYE", "LYONS", "STANCOMBE"),
        259474: ("TRACEY ANNE", "CRAIG", "EAVES", "DAWES"),
        259731: ("DEANNA MAREE", "MATHEW REECE", "UTTING", "BENJAMIN"),
        261051: ("DANA JOY", "DOUGLAS WAYNE", "NORTON", "SAWLEY"),
        265924: ("HELEN", "IAN FRANK NORMAN", "SEMLER", "TERRY"),
        267449: ("TIMOTHY NEIL", "KEVIN NEIL", "DENEHEY", "HILL"),
        271272: ("ROBERT JOHN", "GLEN BRIAN", "PALECEK", "KADEN"),
        271439: ("SANDY PAULINE", "", "BENJAMIN", ""),
        277258: ("JULIE-ANNE", "KENNETH ALWYN", "STRAUSS", "HERBERT"),
        277525: ("TAMARA ELIZABETH", "ANDREW GORDON FRANCIS", "WATTS", "BALDOCK"),
        278009: ("JAALA", "DANIEL", "STILL", "IRVINE"),
        287144: ("METTE MARIA", "RORY BRENDAN", "EK", "KEARNEY"),
        281866: ("BERADETTE ANNE", "DAVID GRAHAM", "JONES", "MINEALL"),
        287520: ("JASON PAUL", "WESLEY JAMES", "BRAY", "PARK"),
        287508: ("SOFIA", "SHANE", "LESLIE", "LESLIE"),
        285852: ("JASON JAMES", "MICHAEL JAMES", "GRUBB", "SMITH"),
        292841: ("PAUL RAYMOND", "BENJAMIN MAXWELL", "DONNELLY", "BUCKNEY"),
        291149: ("DELIA FAY", "MICHAEL BERNARD", "THOMPSON", "BENDOR"),
        297190: ("KAREN ELIZABETH", "RAYMOND ANDREW", "ENRIGHT", "FORSTER"),
        296852: ("NICKETA MAREE", "ANN FLORENCE", "MASATORA", "BLEATHMAN"),
        298072: ("KERRY CHARLOTTE", "", "JARVIS", ""),
        298836: ("ANN", "NICKEETA", "BLEATHAMN", "MASATORA"),
        299598: ("DAVID AND MELISSA ANN", "", "SUSHAMES GLOVER", ""),
        307884: ("LISA KATHLEEN", "DANIEL JAMES", "ARMSTRONG", "TOWNSEND"),
        308862: ("KARYL FRANCES", "MICHAEL JOHN", "MICHAELS", "RUSSELL"),
        309965: ("WENDY JACQUELINE", "", "WHEATLEY", ""),
        310351: ("NICOLE", "", "GRIMMOND", ""),
        311155: ("CRYSTAL JEAN MARGARET", "RIC RODERICK", "STREET", "MURRAY"),
        311708: ("LILLIAN DORIS", "RICHARD", "HAINES", "TAYLOR"),
        311369: ("COLLEEN MARGARET", "COLIN RAYMOND", "WATSON", "GRACE"),
        312543: ("MIA JANETTE HUSTED", "ANDREW JAMES", "HART", "DAND"),
        313511: ("KENNETH WILLIAM", "", "MORAN", ""),
        313500: ("NATASHA ANN", "AARON WAYNE", "LOVELL", "JONES"),
        314562: ("SHARON LEE", "DAVID NICHOLAS", "CARTER", "ASSMANN"),
        319403: ("MAREE", "ARTHUR RICHARD", "ROBERTS", "STOPS"),
        315854: ("SAMANTHA", "HAYDEN", "MARSH", "COAD"),
        316689: ("ELYSE DALE", "AARON DAVID", "MCKENZIE", "DARKE"),
        319922: ("NAOMI ROSE", "ADRIAN MURRAY", "CHAMBERS", "CONNELL"),
        317962: ("ANTHEA ROSEMARY", "JAMES FERRERS", "HILLS", "FERGUSSON"),
        321471: ("PETA LOUISE", "KARL NORMAN", "WELLS", "LOGAN"),
        367604: ("", "", "", ""),
        394153: ("MARK", "", "KRAMER", ""),
        398763: ("Peter William", "", "MCCLURE", ""),
        626546: ("Issac", "", "EXANDRU", ""),
        668280: ("Danielle Louise", "", "FARRELLY", ""),
    }

    # Apply updates to the dataframe
    for person_id, values in updates.items():
        df_copy.loc[
            df_copy["id"] == person_id,
            ["Cleaned_Given_Names", "Cleaned_Given_Names_Extra", "Cleaned_Surname", "Cleaned_Surname_Extra"],
        ] = values

    return df_copy


# Apply the cleaning function to df_final_3
df_final_4A = update_person_names(df_final_4)

"""
function that finds records where:
Both ( and ) exist in the same column.
There is an & somewhere inside the parentheses.
It removes the & but keeps the rest of the text intact.
"""


def remove_ampersand_in_parentheses(df):
    df_copy = df.copy()

    def process_text(value):
        if not isinstance(value, str):
            return value

        # Find text inside parentheses that contains '&'
        matches = re.findall(r"\([^)]*&[^)]*\)", value)

        for match in matches:
            # Remove '&' inside the parentheses
            cleaned_match = match.replace("&", "")
            value = value.replace(match, cleaned_match)

        return value.strip()

    # Apply to both given_names and surname columns
    df_copy["given_names"] = df_copy["given_names"].apply(process_text)
    df_copy["surname"] = df_copy["surname"].apply(process_text)

    return df_copy


df_final_4A = remove_ampersand_in_parentheses(df_final_4A)

"""
Handles records containing ampersands (&) appropriately.
Perform the appropriate splitting based on the first &, and update the relevant columns accordingly.
"""


def process_ampersand_entries(df):
    # Define the regex pattern for finding ampersands surrounded by alpha characters
    pattern_ampersand = re.compile(r"(?<![\W\d])\s*&\s*(?![\W\d])")

    # Define the entries to exclude and substrings that, if present, should exclude the record
    # exclusions_exact = set()  # No exact exclusions for this case
    exclusions_substring = [
        "health",
        " human ",
        " services",
        "department",
        "tasmania",
        "management",
        "plumbing",
        "transport",
        "construction",
        "maintenance",
        "engineering",
        "automotive",
        " glass",
        " windows",
        "education",
        "training",
        "resources",
        "corporation",
        " water",
        "sewerage",
        " motors",
        "finance",
        "property",
        "building",
        " p/l",
        " pty",
        " ltd",
        "security",
        " services",
        "construction",
        "architecture",
        "rentals",
        "education",
        "insurance",
        "counselling",
        " blinds",
        "concreting",
        "excavation",
        "holdings",
        "organisation",
        "PHOTOGRAPHIC",
        "MERCHANTS",
        "fire",
        "truck",
    ]

    # Convert exclusions_substring to lowercase
    exclusions_substring_lower = [item.lower() for item in exclusions_substring]

    # Function to process individual rows
    def process_row(row):
        # Skip processing if is_business == 1
        if row.get("is_business") == 1:
            return row  # return the row unchanged

        # Initialize flags to track regex matches
        match_given_names = pattern_ampersand.search(row["given_names"]) if pd.notna(row["given_names"]) else None
        match_surname = pattern_ampersand.search(row["surname"]) if pd.notna(row["surname"]) else None

        # Process if the pattern is found in both columns
        if match_given_names and match_surname:
            # Process given_names
            if not any(sub in row["given_names"].lower() for sub in exclusions_substring_lower):
                row["Cleaned_Given_Names"] = row["given_names"][: match_given_names.start()].strip()
                row["Removed_Given_Names"] = row["given_names"][match_given_names.start() :].strip()
                row["Cleaned_Given_Names_Extra"] = row["given_names"][match_given_names.start() + 1 :].strip()

            # Process surname
            if not any(sub in row["surname"].lower() for sub in exclusions_substring_lower):
                row["Cleaned_Surname"] = row["surname"][: match_surname.start()].strip()
                row["Removed_Surname"] = row["surname"][match_surname.start() :].strip()
                row["Cleaned_Surname_Extra"] = row["surname"][match_surname.start() + 1 :].strip()

        # Process if the pattern is found in given_names but not in surname
        elif match_given_names and not match_surname:
            # Process given_names
            if not any(sub in row["given_names"].lower() for sub in exclusions_substring_lower):
                row["Cleaned_Given_Names"] = row["given_names"][: match_given_names.start()].strip()
                row["Removed_Given_Names"] = row["given_names"][match_given_names.start() :].strip()
                row["Cleaned_Given_Names_Extra"] = row["given_names"][match_given_names.start() + 1 :].strip()

        return row

    # Apply the function to each row of the DataFrame
    df = df.apply(process_row, axis=1)
    return df


# Example Usage
df_final_5 = df_final_4A.copy()
df_final_5 = process_ampersand_entries(df_final_5)


# Cleans up the '&' in the '..._Extra' columns
def clean_ampersand(df):
    # Define a function that processes the Cleaned_Surname_Extra column
    def process_extra_2(column):
        if pd.isna(column):
            return column  # Return NaN as is

        # If the value is just '&', replace it with NULL
        if column.strip() == "&":
            return None

        # Remove any '& ' (forward slash followed by a space)
        column = column.replace("& ", "")

        # If the value starts with '&', remove the leading '&'
        if column.startswith("&"):
            column = column[1:].strip()

        # Replace any '&' with ', '
        column = column.replace("&", ", ")

        # Remove any text between an open ( and close ) bracket, including the brackets
        column = re.sub(r"\([^)]*\)", "", column)

        # Remove leading/trailing spaces and replace multiple spaces with a single space
        column = re.sub(r"\s+", " ", column).strip()

        return column

    # Apply the function to the columns
    df["Cleaned_Given_Names_Extra"] = df["Cleaned_Given_Names_Extra"].apply(process_extra_2)
    df["Cleaned_Surname_Extra"] = df["Cleaned_Surname_Extra"].apply(process_extra_2)
    return df


# Apply the cleaning function to df_final_5
df_final_5 = clean_ampersand(df_final_5)

"""
Handles records with 'nee' and ensuring data is split and stored in the appropriate columns.
"""


def process_nee_entries(df):
    # Define the regex patterns
    pattern_nee_paren = re.compile(r"\(nee\s([^\)]+)\)", re.IGNORECASE)
    pattern_nee = re.compile(r"\bnee\s([^\)]+)", re.IGNORECASE)

    # Function to process each row
    def process_row(row):
        # Process the given_names column
        if pd.notna(row["given_names"]):
            # Look for '(nee ' first, case-insensitive
            match_paren = pattern_nee_paren.search(row["given_names"])
            if match_paren:
                nee_text = match_paren.group(1).strip()
                row["Cleaned_Given_Names"] = row["given_names"][: match_paren.start()].strip()
                row["Cleaned_Given_Names_Extra"] = nee_text
                if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                    row["Removed_Given_Names"] = "NEE " + match_paren.group(1).strip() + ")"
            else:
                # If no '(nee ' is found, look for ' nee '
                match = pattern_nee.search(row["given_names"])
                if match:
                    nee_text = match.group(1).strip()
                    row["Cleaned_Given_Names"] = row["given_names"][: match.start()].strip()
                    row["Cleaned_Given_Names_Extra"] = nee_text
                    if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                        row["Removed_Given_Names"] = "NEE " + match.group(1).strip()

        # Process the surname column
        if pd.notna(row["surname"]):
            # Look for '(nee ' first, case-insensitive
            match_paren = pattern_nee_paren.search(row["surname"])
            if match_paren:
                nee_text = match_paren.group(1).strip()
                row["Cleaned_Surname"] = row["surname"][: match_paren.start()].strip()
                row["Cleaned_Surname_Extra"] = nee_text
                if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                    row["Removed_Surname"] = "NEE " + match_paren.group(1).strip() + ")"
            else:
                # If no '(nee ' is found, look for ' nee '
                match = pattern_nee.search(row["surname"])
                if match:
                    nee_text = match.group(1).strip()
                    row["Cleaned_Surname"] = row["surname"][: match.start()].strip()
                    row["Cleaned_Surname_Extra"] = nee_text
                    if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                        row["Removed_Surname"] = "NEE " + match.group(1).strip()

        return row

    # Apply the function to each row of the DataFrame
    df = df.apply(process_row, axis=1)
    return df


df_final_6 = df_final_5.copy()
df_final_6 = process_nee_entries(df_final_6)

# # Cleans/removes up the 'NEE' in the '..._Extra' columns
# Currently no 'NEE' found in these columns.
# def remove_nee_from_surname_extra(df):
#     # Remove 'NEE ' (case-sensitive) from the 'Cleaned_Surname_Extra' column
#     df['Cleaned_Surname_Extra'] = df['Cleaned_Surname_Extra'].str.replace('NEE ', '', regex=False).str.strip()
#     return df

# # Apply the function to remove 'NEE ' from Cleaned_Surname_Extra
# df_final_6 = remove_nee_from_surname_extra(df_final_6)

"""
Handles records with 'use' and ensuring data is split and stored in the appropriate columns.
"""


def process_use_entries(df):
    # Define the regex patterns
    pattern_use_paren = re.compile(r"\(use\s([^\)]+)\)", re.IGNORECASE)
    pattern_star_use_paren = re.compile(r"\(\*use\s([^\)]+)\)", re.IGNORECASE)
    pattern_use = re.compile(r"\buse\s([^\)]+)", re.IGNORECASE)

    # Function to process each row
    def process_row(row):
        # Process the given_names column
        if pd.notna(row["given_names"]):
            # Look for '(*Use ' first, case-insensitive
            match_star_paren = pattern_star_use_paren.search(row["given_names"])
            if match_star_paren:
                use_text = match_star_paren.group(1).strip()
                row["Cleaned_Given_Names"] = row["given_names"][: match_star_paren.start()].strip()
                row["Cleaned_Given_Names_Extra"] = use_text
                if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                    row["Removed_Given_Names"] = "*use " + match_star_paren.group(1).strip() + ")"
            else:
                # If no '(*Use ' is found, look for '(use '
                match_paren = pattern_use_paren.search(row["given_names"])
                if match_paren:
                    use_text = match_paren.group(1).strip()
                    row["Cleaned_Given_Names"] = row["given_names"][: match_paren.start()].strip()
                    row["Cleaned_Given_Names_Extra"] = use_text
                    if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                        row["Removed_Given_Names"] = "use " + match_paren.group(1).strip() + ")"
                else:
                    # If no '(use ' is found, look for ' use '
                    match = pattern_use.search(row["given_names"])
                    if match:
                        use_text = match.group(1).strip()
                        row["Cleaned_Given_Names"] = row["given_names"][: match.start()].strip()
                        row["Cleaned_Given_Names_Extra"] = use_text
                        if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                            row["Removed_Given_Names"] = "use " + match.group(1).strip()

        # Process the surname column
        if pd.notna(row["surname"]):
            # Look for '(*Use ' first, case-insensitive
            match_star_paren = pattern_star_use_paren.search(row["surname"])
            if match_star_paren:
                use_text = match_star_paren.group(1).strip()
                row["Cleaned_Surname"] = row["surname"][: match_star_paren.start()].strip()
                row["Cleaned_Surname_Extra"] = use_text
                if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                    row["Removed_Surname"] = "*use " + match_star_paren.group(1).strip() + ")"
            else:
                # If no '(*Use ' is found, look for '(use '
                match_paren = pattern_use_paren.search(row["surname"])
                if match_paren:
                    use_text = match_paren.group(1).strip()
                    row["Cleaned_Surname"] = row["surname"][: match_paren.start()].strip()
                    row["Cleaned_Surname_Extra"] = use_text
                    if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                        row["Removed_Surname"] = "use " + match_paren.group(1).strip() + ")"
                else:
                    # If no '(use ' is found, look for ' use '
                    match = pattern_use.search(row["surname"])
                    if match:
                        use_text = match.group(1).strip()
                        row["Cleaned_Surname"] = row["surname"][: match.start()].strip()
                        row["Cleaned_Surname_Extra"] = use_text
                        if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                            row["Removed_Surname"] = "use " + match.group(1).strip()

        return row

    # Apply the function to each row of the DataFrame
    df = df.apply(process_row, axis=1)
    return df


df_final_7 = df_final_6.copy()
df_final_7 = process_use_entries(df_final_7)

"""
Handles records with 'and' and ensuring data is split and stored in the appropriate columns.
"""


def process_and_entries(df):
    # Define the regex patterns
    pattern_nee = re.compile(r"\band\s([^\)]+)", re.IGNORECASE)

    # Function to process each row
    def process_row(row):
        # Skip processing if is_business == 1
        if row.get("is_business") == 1:
            return row  # return the row unchanged

        # Process the given_names column
        if pd.notna(row["given_names"]):
            # look for ' and '
            match = pattern_nee.search(row["given_names"])
            if match:
                nee_text = match.group(1).strip()
                row["Cleaned_Given_Names"] = row["given_names"][: match.start()].strip()
                row["Cleaned_Given_Names_Extra"] = nee_text
                if pd.isna(row["Removed_Given_Names"]) or row["Removed_Given_Names"] == "":
                    row["Removed_Given_Names"] = "AND " + match.group(1).strip()

        # Process the surname column
        if pd.notna(row["surname"]):
            # look for ' and '
            match = pattern_nee.search(row["surname"])
            if match:
                nee_text = match.group(1).strip()
                row["Cleaned_Surname"] = row["surname"][: match.start()].strip()
                row["Cleaned_Surname_Extra"] = nee_text
                if pd.isna(row["Removed_Surname"]) or row["Removed_Surname"] == "":
                    row["Removed_Surname"] = "AND " + match.group(1).strip()

        return row

    # Apply the function to each row of the DataFrame
    df = df.apply(process_row, axis=1)
    return df


df_final_7A = df_final_7.copy()
df_final_7B = process_and_entries(df_final_7A)

# Update the rows where 'surname' matches these edge case patterns
# Currently no edge cases identified.
df_final_7B.loc[df_final_7B["surname"] == "SM ITH", "Cleaned_Surname"] = "SMITH"

# # Updated function to process police rank keywords and move them to the start of the cleaned_columns
# def process_extra_columns_and_move(df):
#     # Define the keywords to look for (exact matches)
#     keywords = [
#         "Acting Sergeant",
#         "Senior Sergeant",
#         "Detective Sergeant",
#         "Acting Inspector",
#         "Senior Constable",
#         "Detective First class Constable",
#         "First class Constable",
#         "Detective Constable",
#         "Detective Senior Constable",
#         "Detective Sergeant",
#         "Trainee Constable",
#         "First class",
#         "Senior Constable",
#         "Acting Sergeant",
#         "CONSTABLE",
#         "CONST",
#         "SEN CONSTABLE",
#         "SNR CONSTABLE",
#         "DET CONST",
#         "DET CONSTABLE",
#         "SNR CONST",
#         "SEN CONST",
#         "SPECIAL CONSTABLE",
#         "INSP",
#         "INSPECTOR",
#         "Sergeant",
#         "SERGT",
#         "SGT",
#         "ACTING SGT",
#         "SERGT",
#         "SENIOR",
#         "SNR",
#         "SENATOR",
#     ]

#     # Convert keywords to lowercase for case-insensitive matching
#     keywords_lower = [k.lower() for k in keywords]

#     # Helper function to process individual columns
#     def move_keywords(extra_column, main_column):
#         for index, row in df.iterrows():
#             extra_value = row[extra_column]
#             if isinstance(extra_value, str) and extra_value.strip().lower() in keywords_lower:
#                 # Remove the keyword from the extra column
#                 df.at[index, extra_column] = ""

#                 # Add the keyword to the start of the main column
#                 main_value = row[main_column] if pd.notna(row[main_column]) else ""
#                 updated_main = f"{extra_value.strip()} {main_value}".strip()
#                 df.at[index, main_column] = updated_main

#     # Process both columns
#     move_keywords("Cleaned_Surname_Extra", "Cleaned_Surname")
#     move_keywords("Cleaned_Given_Names_Extra", "Cleaned_Given_Names")

#     return df

# # Apply the updated function to df_final_7
# df_final_7 = process_extra_columns_and_move(df_final_7)

"""
List of keywords to identify as non-name entries. Can be updated with new keywords.
Any Cleaned_Given_Names or Cleaned_Surname with any of the below will be moved to the relevant "Not_A_Name_..." column
"""
# "WKS", "UNBORN", "Unbourn", "PREGNANT", "MONTHS", "BABY", person
# "Father", "DOB", "SERVICE", "SERVICES", "HEALTH", "TASMANIA", "police", "CLEANING", "COMMUNITY", "One",
keywords = set(
    [
        "ABOUT",
        "INDUCED",
        "RESIDENTS",
        "Unkown",  # codespell:ignore
        "UKNOWN",  # codespell:ignore
        "UNKNOW",  # codespell:ignore
        "UNKNOWN",  # codespell:ignore
        "UNKNOWMN",  # codespell:ignore
        "un known",  # codespell:ignore
        "Unnamed",
        "UNKNOQN",
        "Unknwon",
        "Unknon",
        "DETECTED",
        "persons",
        "person",
        "PERSONNEL",
        "PERSONEL",
        "STOREPERSON",
        "TEST",
        "OFFENDERONE",
        "VICTIMONE",
        "testone",
        "OFFENDERTWO",
        "testtwo",
        "VICTIMTWO",
        "VICTIM",
        "NOT KNOWN",
        "NOT KNOW",
        "DECLINED",
        "female",
        "HOWEVER",
        "Parent",
        "deceased",
        "unconfirmed",
        "LEGITIMATE",
        "SOLD",
        "REPOSSESSION",
        "ANONYMOUS",
        "INVESTIGATION",
        "select",
        "please",
        "possibly",
        "OFFENDER",
        "PARTY",
        "RESIDENT",
        "BEING",
        "BOULEVARD",
        "tba",
        "Unidentified",
        "stated",
        "disclose",
        "Recorded",
        "biological",
        "provided",
        "medically",
        "confirmed",
        "aged",
        "pronounced",
        "relationship",
        "CALLED",
        "Testa",
        "Testb",
        "Mobile",
        "three",
        "Named",
        "GRANDFIELDS",
        "ADVISE",
        "OVERWRITE",
        "Null",
        "NIL",
        "None",
        "GIVEN BY",
    ]
)


def classify_names(data, column_name):
    # Initialize new columns for names and non-name entries
    name_column = column_name + "_2"
    non_name_column = "Not_A_Name_" + column_name
    data[name_column] = ""
    data[non_name_column] = ""

    # Prepare for matching keywords as whole words and case insensitive
    keyword_pattern = r"\b(?:" + "|".join(re.escape(kw) for kw in keywords) + r")\b"
    keyword_regex = re.compile(keyword_pattern, re.IGNORECASE)

    for index, row in data.iterrows():
        text = row[column_name]
        if pd.isna(text):
            continue

        # Check for keywords using regex for whole word match
        if keyword_regex.search(text):
            # Place entire record in non-name column if keyword found
            data.at[index, non_name_column] = text
        else:
            data.at[index, name_column] = text  # Otherwise place it in the name column

    return data


df_final_8 = df_final_7B.copy()
# Apply the function to both the given names and surnames
df_final_8 = classify_names(df_final_8, "Cleaned_Given_Names")
df_final_8 = classify_names(df_final_8, "Cleaned_Surname")

"""
Moving the 'JNR' and 'SNR' suffix in given_names or surname to the end of the surname column.
Task 36352
"""


def move_jnr_to_surname(df):
    df_copy = df.copy()
    # keywords in the correct order to prevent splitting
    keywords = ["Jnr"]

    # regex pattern to match any of the keywords
    pattern = r"\b(?:" + "|".join(re.escape(word) for word in keywords) + r")\b"

    def process_row(row):
        given_names = row["Cleaned_Given_Names_2"]
        surname = row["Cleaned_Surname_2"]

        if not isinstance(given_names, str):
            given_names = ""
        if not isinstance(surname, str):
            surname = ""

        # Find all keywords in the combined text
        combined_text = f"{given_names} {surname}"
        matches = re.findall(pattern, combined_text, flags=re.IGNORECASE)

        if matches:
            # Remove keywords from both columns
            cleaned_given_names = re.sub(pattern, "", given_names, flags=re.IGNORECASE).strip()
            cleaned_surname = re.sub(pattern, "", surname, flags=re.IGNORECASE).strip()

            # Append the keywords to the given_names
            new_surname = f"{cleaned_surname} {' '.join(matches)}".strip()

            return pd.Series([cleaned_given_names, new_surname])
        else:
            return pd.Series([given_names, surname])

    # Apply the function to the columns
    df_copy[["Cleaned_Given_Names_2", "Cleaned_Surname_2"]] = df_copy.apply(process_row, axis=1)

    return df_copy


df_final_8 = move_jnr_to_surname(df_final_8)


def move_snr_to_surname(df):
    df_copy = df.copy()
    # keywords in the correct order to prevent splitting
    keywords = ["Snr"]

    # regex pattern to match any of the keywords
    pattern = r"\b(?:" + "|".join(re.escape(word) for word in keywords) + r")\b"

    def process_row(row):
        given_names = row["Cleaned_Given_Names_2"]
        surname = row["Cleaned_Surname_2"]

        if not isinstance(given_names, str):
            given_names = ""
        if not isinstance(surname, str):
            surname = ""

        # Find all keywords in the combined text
        combined_text = f"{given_names} {surname}"
        matches = re.findall(pattern, combined_text, flags=re.IGNORECASE)

        if matches:
            # Remove keywords from both columns
            cleaned_given_names = re.sub(pattern, "", given_names, flags=re.IGNORECASE).strip()
            cleaned_surname = re.sub(pattern, "", surname, flags=re.IGNORECASE).strip()

            # Append the keywords to the given_names
            new_surname = f"{cleaned_surname} {' '.join(matches)}".strip()

            return pd.Series([cleaned_given_names, new_surname])
        else:
            return pd.Series([given_names, surname])

    # Apply the function to the columns
    df_copy[["Cleaned_Given_Names_2", "Cleaned_Surname_2"]] = df_copy.apply(process_row, axis=1)

    return df_copy


df_final_8 = move_snr_to_surname(df_final_8)

"""
Standardising the single apostrophe in Irish surnames.
"""


def setup_name_mapping():
    # Define the mapping dictionary
    return {
        "OBORNE": "O'BORNE",
        "OBRIAN": "O'BRIAN",
        "OBRIEN": "O'BRIEN",
        "OBYRNE": "O'BYRNE",
        "OCALLAGHAN": "O'CALLAGHAN",
        "OCHAYA": "O'CHAYA",
        "OCONNOR": "O'CONNOR",
        "ODEGAARD": "O'DEGAARD",
        "ODOHERTY": "O'DOHERTY",
        "ODONALD": "O'DONALD",
        "ODONELL": "O'DONELL",
        "ODONNELL": "O'DONNELL",
        "ODWYER": "O'DWYER",
        "OGAREY": "O'GAREY",
        "OGRADY": "O'GRADY",
        "OHALLORAN": "O'HALLORAN",
        "OHIA": "O'HIA",
        "OKEEFE": "O'KEEFE",
        "OKENY": "O'KENY",
        "OLOUGHLIN": "O'LOUGHLIN",
        "OMAN": "O'MAN",
        "ONEAL": "O'NEAL",
        "ONEIL": "O'NEIL",
        "ONEILL": "O'NEILL",
        "OREILLY": "O'REILLY",
        "OREILLY-MONKS": "O'REILLY-MONKS",
        "OROURKE": "O'ROURKE",
        "OSANMOH": "O'SANMOH",
        "OSHEA": "O'SHEA",
        "OSIGN": "O'SIGN",
        "OSULLIVAN": "O'SULLIVAN",
        "OTOOLE": "O'TOOLE",
        "O' BRIEN": "O'BRIEN",
        "O BRIEN": "O'BRIEN",
        "O CONNOR": "O'CONNOR",
        "O DONNELL": "O'DONNELL",
        "O DONOGHUE": "O'DONOGHUE",
        "O KEEFE": "O'KEEFE",
        "O LOUGHLIN": "O'LOUGHLIN",
        "O NEARA": "O'NEARA",
        "O REILLY": "O'REILLY",
        "O ROURKE": "O'ROURKE",
        "O SULLIVAN": "O'SULLIVAN",
        "O TOOLE": "O'TOOLE",
        "O SHANNESSEY": "O'SHANNESSEY",
        "O HALLORAN": "O'HALLORAN",
        "O-MARA": "O'MARA",
        "O-DEA": "O'DEA",
        "O-HEHIR": "O'HEHIR",
        "O-RICHARD MARE": "O'RICHARD MARE",
        "O-RICHARD-MARE": "O'RICHARD MARE",
        "O-KAYNE": "O'KAYNE",
    }


# Apply the mapping with case-insensitivity
name_map = setup_name_mapping()

# Convert the name_map keys to lowercase for case-insensitive matching
name_map = {key.lower(): value for key, value in name_map.items()}

# Apply the case-insensitive lookup by converting the given names and surnames to lowercase
df_final_8["Cleaned_Given_Names_2"] = df_final_8.apply(
    lambda x: (
        name_map.get(x["given_names"].strip().lower(), x["Cleaned_Given_Names_2"])
        if isinstance(x["given_names"], str) and x["given_names"].strip().lower() in name_map
        else x["Cleaned_Given_Names_2"]
    ),
    axis=1,
)

df_final_8["Cleaned_Surname_2"] = df_final_8.apply(
    lambda x: name_map[x["surname"].strip().lower()]
    if x["surname"] and x["surname"].strip().lower() in name_map
    else x["Cleaned_Surname_2"],
    axis=1,
)

# rearranging the column order (29 columns)
rearrange_columns = [
    "id",
    "version",
    "country_of_origin_id",
    "date_created",
    "date_of_birth",
    "given_names",
    "Cleaned_Given_Names",
    "Cleaned_Given_Names_Extra",
    "Removed_Given_Names",
    "Cleaned_Given_Names_2",
    "Not_A_Name_Cleaned_Given_Names",
    "indigenous_status_id",
    "last_updated",
    "ne_reason",
    "not_editable",
    "primary_address_id",
    "primary_contact_id",
    "racial_appearance_id",
    "sex_id",
    "surname",
    "Cleaned_Surname",
    "Cleaned_Surname_Extra",
    "Removed_Surname",
    "Cleaned_Surname_2",
    "Not_A_Name_Cleaned_Surname",
    "care_of_address",
    "spi",
    "is_business",
    "is_employee",
]
# 611,583 records and 29 columns
df_final_8 = df_final_8[rearrange_columns]

# Replaces any blank (empty) values in the dataframe with None
df_final_8.replace("", None, inplace=True)

"""
Function to do some general cleaning of the '..._Extra' and cleaned columns:
Removing leading and trailing commas.
Making NULL equivalents NULL.
"""


# Process the cleaned_data_2 dataframe
def process_cleaned_data(df):
    df_copy = df.copy()
    # Make records equal to ',' or starting with ',' in Cleaned_Given_Names_Extra and Cleaned_Surname_Extra blank
    df_copy["Cleaned_Given_Names_Extra"] = df_copy["Cleaned_Given_Names_Extra"].apply(
        lambda x: "" if isinstance(x, str) and (x.strip() == "," or x.strip().startswith(",")) else x
    )
    df_copy["Cleaned_Surname_Extra"] = df_copy["Cleaned_Surname_Extra"].apply(
        lambda x: "" if isinstance(x, str) and (x.strip() == "," or x.strip().startswith(",")) else x
    )

    # Remove trailing ',' in Cleaned_Given_Names_Extra and Cleaned_Surname_Extra
    df_copy["Cleaned_Given_Names_Extra"] = df_copy["Cleaned_Given_Names_Extra"].apply(
        lambda x: x[:-1].strip() if isinstance(x, str) and x.strip().endswith(",") else x
    )
    df_copy["Cleaned_Surname_Extra"] = df_copy["Cleaned_Surname_Extra"].apply(
        lambda x: x[:-1].strip() if isinstance(x, str) and x.strip().endswith(",") else x
    )

    # Make specific records in Cleaned_Given_Names_2 and Cleaned_Surname_2 blank if they match the specified values (case-insensitive)
    invalid_values = {
        "U K",
        "u k",
        "N A",
        "n a",
        "N K",
        "n k",
        "K N",
        "k n",
        "u lk",
        "UK",
        "uk",
        "NA",
        "na",
        "NK",
        "Nk",
        "nk",
        "KN",
        "kn",
        "ulk",
        "X",
        "XX",
        "XXXX",
    }
    # Convert to lowercase for case-insensitive matching
    invalid_values = {value.lower() for value in invalid_values}

    df_copy["Cleaned_Given_Names_2"] = df_copy["Cleaned_Given_Names_2"].apply(
        lambda x: "" if isinstance(x, str) and x.strip().lower() in invalid_values else x
    )
    df_copy["Cleaned_Surname_2"] = df_copy["Cleaned_Surname_2"].apply(
        lambda x: "" if isinstance(x, str) and x.strip().lower() in invalid_values else x
    )
    return df_copy


df_final_8A = process_cleaned_data(df_final_8)

"""
Strip leading/trailing spaces and replace multiple spaces with a single space.
If the result is an empty string after cleaning, set it to NULL.
"""


# Clean up spaces (applied to all string columns)
def clean_spaces(text):
    if isinstance(text, str):  # Ensure we only apply to string values
        cleaned_text = " ".join(text.split())
        return cleaned_text if cleaned_text else None
    return text


columns_to_clean = [
    "Cleaned_Given_Names_2",
    "Cleaned_Surname_2",
    "Cleaned_Given_Names_Extra",
    "Cleaned_Surname_Extra",
]
for col in columns_to_clean:
    df_final_8A[col] = df_final_8A[col].apply(clean_spaces)

"""
Function to update care_of_address.
It searches for "Care Of" in the Cleaned_Surname_2 or Cleaned_Given_Names_2 columns and sets the care_of_address column to 1 for matching records
"""


def update_care_of_address(df):
    df_copy = df.copy()
    # Check for "Care Of" (case-insensitive) in either Cleaned_Surname_2 or Cleaned_Given_Names_2
    df_copy["care_of_address"] = df_copy.apply(
        lambda row: 1
        if (
            isinstance(row["Cleaned_Surname_2"], str)
            and "care of" in row["Cleaned_Surname_2"].lower()
            or isinstance(row["Cleaned_Given_Names_2"], str)
            and "care of" in row["Cleaned_Given_Names_2"].lower()
        )
        else row["care_of_address"],
        axis=1,
    )
    return df_copy


df_final_8A = update_care_of_address(df_final_8A)

"""
function that creates a new column 'can_exclude' and sets it to 1 for records where:
Both Cleaned_Given_Names_2 and Cleaned_Surname_2 are NULL
OR both date_of_birth and Cleaned_Surname_2 are NULL
"""


def set_can_exclude(df):
    df_copy = df.copy()
    # Create 'can_exclude' column and initialize with 0
    df_copy["can_exclude"] = 0

    # Set 'can_exclude' to 1 where either condition is met
    df_copy.loc[
        (df_copy["Cleaned_Given_Names_2"].isna() & df_copy["Cleaned_Surname_2"].isna())
        | (df_copy["date_of_birth"].isna() & df_copy["Cleaned_Surname_2"].isna()),
        "can_exclude",
    ] = 1

    return df_copy


# 611,583 records and 30 columns
df_final_8B = set_can_exclude(df_final_8A)

# Capitalize columns
columns_to_sentence_case = ["given_names"]
column_to_upper_case = "Cleaned_Surname_2"
# Convert the given name columns to sentence case
for col in columns_to_sentence_case:
    if col in df_final_8B.columns:
        df_final_8B.loc[:, col] = df_final_8B[col].str.capitalize()
# Convert the 'Cleaned_Surname_2' column to uppercase
if column_to_upper_case in df_final_8B.columns:
    df_final_8B.loc[:, column_to_upper_case] = df_final_8B[column_to_upper_case].str.upper()

"""
Find records where 'Cleaned_Surname_2' starts with 'MAC ' or 'MC ' (case insensitive) treating NaN as False.
Remove the space for these records.
"""
mask = df_final_8B["Cleaned_Surname_2"].str.contains(r"^(MAC |MC )", case=False, regex=True, na=False)
df_final_8B.loc[mask, "Cleaned_Surname_2"] = df_final_8B.loc[mask, "Cleaned_Surname_2"].str.replace(
    r"^(MAC |MC )",
    lambda match: match.group(0).replace(" ", ""),
    case=False,
    regex=True,
)

# Specify the final column order (22 columns)
final_columns = [
    "id",
    "version",
    "country_of_origin_id",
    "date_created",
    "date_of_birth",
    "Cleaned_Given_Names_2",
    "indigenous_status_id",
    "last_updated",
    "ne_reason",
    "not_editable",
    "primary_address_id",
    "primary_contact_id",
    "racial_appearance_id",
    "sex_id",
    "Cleaned_Surname_2",
    "care_of_address",
    "spi",
    "Cleaned_Given_Names_Extra",
    "Cleaned_Surname_Extra",
    "is_business",
    "is_employee",
    "can_exclude",
]
# 611,583 records and 22 columns
cleaned_data = df_final_8B[final_columns]

"""
Function to clean and split the given_names into the 3 specified columns.
Removes leading, trailing, and extra spaces.
"""


def split_given_names(df):
    df_copy = df.copy()
    df_copy["Cleaned_Given_Names_2"] = df_copy["Cleaned_Given_Names_2"].str.strip().replace(r"\s+", " ", regex=True)
    name_splits = df_copy["Cleaned_Given_Names_2"].str.split(" ", n=2, expand=True)

    # Assign each part to the appropriate new column
    df_copy["given_name_1"] = name_splits[0]
    df_copy["given_name_2"] = name_splits[1]
    df_copy["given_name_3"] = name_splits[2]
    return df_copy


cleaned_data_2 = split_given_names(cleaned_data)

# Capitalize columns
columns_to_sentence_case = [
    "given_name_1",
    "given_name_1",
    "given_name_1",
]
column_to_upper_case = "Cleaned_Surname_2"
# Convert the given name columns to sentence case
for col in columns_to_sentence_case:
    if col in cleaned_data_2.columns:
        cleaned_data_2.loc[:, col] = cleaned_data_2[col].str.capitalize()
# Convert the 'Cleaned_Surname_2' column to uppercase
if column_to_upper_case in cleaned_data_2.columns:
    cleaned_data_2.loc[:, column_to_upper_case] = cleaned_data_2[column_to_upper_case].str.upper()

# 611,583 records and 22 columns
df_final_9 = cleaned_data_2[
    [
        "id",
        "version",
        "country_of_origin_id",
        "date_created",
        "date_of_birth",
        "given_name_1",
        "given_name_2",
        "given_name_3",
        "indigenous_status_id",
        "last_updated",
        "ne_reason",
        "not_editable",
        "primary_address_id",
        "primary_contact_id",
        "racial_appearance_id",
        "sex_id",
        "Cleaned_Surname_2",
        "care_of_address",
        "spi",
        "is_business",
        "is_employee",
        "can_exclude",
    ]
]

# Rename column
df_final_9.rename(columns={"Cleaned_Surname_2": "Cleaned_Surname"}, inplace=True)

##########################################
##########################################

# Processing and uploading the aliases we extracted into the '_Extra' columns
# Creating a separate dataframe for the alias names
# 611,583  records and 22 columns
df_final_10 = cleaned_data_2[
    [
        "id",
        "version",
        "country_of_origin_id",
        "date_created",
        "date_of_birth",
        "Cleaned_Given_Names_2",
        "indigenous_status_id",
        "last_updated",
        "ne_reason",
        "not_editable",
        "primary_address_id",
        "primary_contact_id",
        "racial_appearance_id",
        "sex_id",
        "Cleaned_Surname_2",
        "care_of_address",
        "spi",
        "Cleaned_Given_Names_Extra",
        "Cleaned_Surname_Extra",
        "is_business",
        "is_employee",
        "can_exclude",
    ]
]

# Rename column
df_final_10.rename(columns={"Cleaned_Surname_2": "Cleaned_Surname"}, inplace=True)
df_final_10.rename(columns={"Cleaned_Given_Names_2": "Cleaned_Given_Names"}, inplace=True)

# Filter for non-NULL entries in 'Cleaned_Given_Names_Extra' or 'Cleaned_Surname_Extra'
# 2,070 records
filtered_df = df_final_10[
    df_final_10["Cleaned_Given_Names_Extra"].notna() | df_final_10["Cleaned_Surname_Extra"].notna()
]

# Create a new DataFrame for each combination
# List to hold all the new rows
new_rows = []

for _, row in filtered_df.iterrows():
    # Create combinations if both fields have valid entries
    if pd.notna(row["Cleaned_Surname_Extra"]):
        # 1. Cleaned_Given_Names and Cleaned_Surname_Extra
        new_rows.append(
            {
                "id": row["id"],
                "Alias_Given_Names": row["Cleaned_Given_Names"],
                "Alias_Surname": row["Cleaned_Surname_Extra"],
            }
        )

    if pd.notna(row["Cleaned_Given_Names_Extra"]):
        # 2. Cleaned_Given_Names_Extra and Cleaned_Surname
        new_rows.append(
            {
                "id": row["id"],
                "Alias_Given_Names": row["Cleaned_Given_Names_Extra"],
                "Alias_Surname": row["Cleaned_Surname"],
            }
        )

    if pd.notna(row["Cleaned_Given_Names_Extra"]) and pd.notna(row["Cleaned_Surname_Extra"]):
        # 3. Cleaned_Given_Names_Extra and Cleaned_Surname_Extra
        new_rows.append(
            {
                "id": row["id"],
                "Alias_Given_Names": row["Cleaned_Given_Names_Extra"],
                "Alias_Surname": row["Cleaned_Surname_Extra"],
            }
        )

# Create the new DataFrame
# 2,542 records
alias_df = pd.DataFrame(new_rows)


# Split Alias_Surname if it contains a comma and create new rows for each surname
# Define a function to handle splitting
def split_surnames(row):
    surnames = [surname.strip() for surname in row["Alias_Surname"].split(",")]  # Split by comma and strip spaces
    return pd.DataFrame(
        {
            "id": [row["id"]] * len(surnames),  # Keep the same id
            # Keep the same Alias_Given_Names
            "Alias_Given_Names": [row["Alias_Given_Names"]] * len(surnames),
            "Alias_Surname": surnames,  # Assign each surname to a new row
        }
    )


# Apply the function to rows that contain a comma in Alias_Surname
# 4 records with comma in Alias_Surname
expanded_df = alias_df[alias_df["Alias_Surname"].str.contains(",", na=False)].apply(split_surnames, axis=1)
# Convert expanded_df from a series of DataFrames to a single DataFrame
# 8 records
expanded_df = pd.concat(expanded_df.values)

# Get the rows that don't contain a comma and append them to the expanded DataFrame
# 2,538 records (2,542 - 4)
no_comma_df = alias_df[~alias_df["Alias_Surname"].str.contains(",", na=False)]

# Combine both DataFrames
# 2,546 records (2,542 + 4)
"""
1,830 (group of 1 - single id) - 253, 2394, 3808, 4542
4 (group of 2 ids) - 578126, 578694, 580870, 657035 (GILLIES/OSULLIVAN/STOLTENBERG)
236 (group of 3 ids) - 199757, 200011, 200740, 201582


1,830 + 4*2 + 236*3 = 2,546

test = final_alias_df['id'].value_counts().map(lambda x: '1' if x == 1 else '2' if x == 2 else '3' if x == 3 else '4+').value_counts().sort_index().rename_axis('Group').reset_index(name='Count')
"""
# 2,546 records (8 + 2,538)
final_alias_df = pd.concat([no_comma_df, expanded_df], ignore_index=True)


# Function to clean and split the given_names into the specified columns
def split_given_names_alias(df):
    df_copy = df.copy()
    # Remove leading, trailing, and extra spaces
    df_copy["Alias_Given_Names"] = df_copy["Alias_Given_Names"].str.strip().replace(r"\s+", " ", regex=True)

    # Split the given_names into up to 3 components
    name_splits = df_copy["Alias_Given_Names"].str.split(" ", n=2, expand=True)

    # Assign each part to the appropriate new column
    df_copy["alias_given_name_1"] = name_splits[0]
    df_copy["alias_given_name_2"] = name_splits[1]
    df_copy["alias_given_name_3"] = name_splits[2]
    return df_copy


# 2,546 records and 6 columns
final_alias_df_2 = split_given_names_alias(final_alias_df)

# Capitalize columns
columns_to_sentence_case = [
    "alias_given_name_1",
    "alias_given_name_2",
    "alias_given_name_3",
]
column_to_upper_case = "Alias_Surname"
# Convert the given name columns to sentence case
for col in columns_to_sentence_case:
    if col in final_alias_df_2.columns:
        final_alias_df_2.loc[:, col] = final_alias_df_2[col].str.capitalize()
# Convert the 'Alias_Surname' column to uppercase
if column_to_upper_case in final_alias_df_2.columns:
    final_alias_df_2.loc[:, column_to_upper_case] = final_alias_df_2[column_to_upper_case].str.upper()

##########################################
##########################################


# Getting lists of aliases that aren't actually aliases and should be added as new person records
def is_not_blank(value):
    return pd.notna(value) and value.strip() != ""


"""
True new person records.

lists for each condition:
given_names does contain a separator.
Cleaned_Given_Names_Extra is not blank.
surname does not contain a separator.
"""
# '&' only in given_names
# 893 records (253, 2394, 4542, 5067, 5680, 9503)
seperator_in_gn_not_sn_1 = df_final_8B[
    (df_final_8B["given_names"].str.contains("&", na=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (~df_final_8B["surname"].str.contains("&|/", na=False))
]["id"].tolist()

# '/' only in given_names
# 6 records (121475, 132003, 132074, 189564, 232040, 486578)
seperator_in_gn_not_sn_2 = df_final_8B[
    (df_final_8B["given_names"].str.contains("/", na=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (~df_final_8B["surname"].str.contains("&|/", na=False))
]["id"].tolist()

# ' and ' only in given_names
# 116 records (5418, 10788, 10789, 11376, 11734)
seperator_in_gn_not_sn_3 = df_final_8B[
    (df_final_8B["given_names"].str.contains(" and ", na=False, case=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (~df_final_8B["surname"].str.contains(" and ", na=False, case=False))
]["id"].tolist()

"""
True new person records but also aliases.

lists for each condition:
given_names does contain a separator.
Cleaned_Given_Names_Extra is not blank.
surname does contain a separator.
Cleaned_Surname_Extra is not blank.
"""
# '&' in both given_names and surname
# 166 records (51424, 279296, 298345, 327473, 328093)
seperator_in_gn_and_sn_1 = df_final_8B[
    (df_final_8B["given_names"].str.contains("&", na=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (df_final_8B["surname"].str.contains("&", na=False))
    & (df_final_8B["Cleaned_Surname_Extra"].apply(is_not_blank))
]["id"].tolist()

# '/' in both given_names and surname
# 3 records (275409, 374112, 406447)
seperator_in_gn_and_sn_2 = df_final_8B[
    (df_final_8B["given_names"].str.contains("/", na=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (df_final_8B["surname"].str.contains("/", na=False))
    & (df_final_8B["Cleaned_Surname_Extra"].apply(is_not_blank))
]["id"].tolist()

# '&' and '/' in given_names and surname
# 2 records (352661, 362909)
seperator_in_gn_and_sn_3 = df_final_8B[
    (df_final_8B["given_names"].str.contains("&", na=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (df_final_8B["surname"].str.contains("/", na=False))
    & (df_final_8B["Cleaned_Surname_Extra"].apply(is_not_blank))
]["id"].tolist()

# ' and ' in both given_names and surname
# 2 records (364259, 396415)
seperator_in_gn_and_sn_4 = df_final_8B[
    (df_final_8B["given_names"].str.contains(" and ", na=False, case=False))
    & (df_final_8B["Cleaned_Given_Names_Extra"].apply(is_not_blank))
    & (df_final_8B["surname"].str.contains(" and ", na=False, case=False))
    & (df_final_8B["Cleaned_Surname_Extra"].apply(is_not_blank))
    & (df_final_8B["is_business"] != 1)
]["id"].tolist()

# ':' in both given_names and surname
# 58 records
seperator_in_gn_and_sn_5 = [
    199757,
    200011,
    200740,
    201582,
    193700,
    212246,
    213127,
    224103,
    226288,
    230396,
    232943,
    234127,
    237098,
    237950,
    241905,
    249471,
    250963,
    252403,
    245118,
    246386,
    246622,
    254776,
    256521,
    256615,
    259315,
    259474,
    259731,
    261051,
    265924,
    267449,
    271272,
    277258,
    277525,
    278009,
    287144,
    281866,
    287520,
    287508,
    285852,
    292841,
    291149,
    297190,
    296852,
    298836,
    307884,
    308862,
    311155,
    311708,
    311369,
    312543,
    313500,
    314562,
    319403,
    315854,
    316689,
    319922,
    317962,
    321471,
]

# Check to see there is no over lap of id in these lists
# list(set(seperator_in_gn_not_sn_1) & set(seperator_in_gn_not_sn_2) & set(seperator_in_gn_not_sn_3) & set(seperator_in_gn_and_sn_1) & set(seperator_in_gn_and_sn_2) & set(seperator_in_gn_and_sn_3) & set(seperator_in_gn_and_sn_4) & set(seperator_in_gn_and_sn_5))

"""
We need to limit definition of aliases to those person records with names between brackets, precedded by "NEE" or "Use".
Those found where names are separated by "&" or "/" need to be reviewed to separate aliases and genuine different people.
In which case we can exclude these from the final_alias_df_2 dataframe using the id column.
"""
"""
Processing the new records:
    seperator_in_gn_not_sn_1, seperator_in_gn_not_sn_2, seperator_in_gn_not_sn_3

    Currently below is found in the final table df_final_9
    given_names_1, surname_1

    below found in final alias table final_alias_df_2:
    given_names_2, surname_1

    Should move the below from final alias table final_alias_df_2 to df_final_9:
    given_names_2, surname_1
"""

# Find all records in final_alias_df_2 with an id in the lists
# 1,015 records
ids_to_find = set(seperator_in_gn_not_sn_1 + seperator_in_gn_not_sn_2 + seperator_in_gn_not_sn_3)
# 1,017 records (2 extra records (404652) that will be removed later)
seperator_in_gn_not_sn_df = final_alias_df_2[final_alias_df_2["id"].isin(ids_to_find)]

# Rename columns in seperator_in_gn_not_sn_df to match df_final_9 where necessary
seperator_in_gn_not_sn_df = seperator_in_gn_not_sn_df.rename(
    columns={
        "alias_given_name_1": "given_name_1",
        "alias_given_name_2": "given_name_2",
        "alias_given_name_3": "given_name_3",
        "Alias_Surname": "Cleaned_Surname",
    }
)

# Select only the columns common between the two dataframes
common_columns = [
    "id",
    "given_name_1",
    "given_name_2",
    "given_name_3",
    "Cleaned_Surname",
]
seperator_in_gn_not_sn_df = seperator_in_gn_not_sn_df[common_columns]

# Append the records to df_final_9
# 612,600 records 22 columns (1,017 records added)
df_final_11 = pd.concat([df_final_9, seperator_in_gn_not_sn_df], ignore_index=True)

"""
Processing the new records with aliases:
    seperator_in_gn_and_sn_1, seperator_in_gn_and_sn_2, seperator_in_gn_and_sn_3, seperator_in_gn_and_sn_4, seperator_in_gn_and_sn_5

    Currently below is found in the final table df_final_9
    given_names_1, surname_1

    below found in final alias table final_alias_df_2:
    given_names_1, surname_2
    given_names_2, surname_1
    given_names_2, surname_2

    Should remove the below from final alias table final_alias_df_2:
    given_names_1, surname_2
    given_names_2, surname_1

    Should move the below from final alias table final_alias_df_2 to df_final_9:
    given_names_2, surname_2
"""

# Combine the IDs from the lists into a single set
# 231 records
ids_to_find = set(
    seperator_in_gn_and_sn_1
    + seperator_in_gn_and_sn_2
    + seperator_in_gn_and_sn_3
    + seperator_in_gn_and_sn_4
    + seperator_in_gn_and_sn_5
)
# Filter records in final_alias_df_2 for matching ids
# 693 records (these ids are duplicated in final_alias_df_2)
seperator_in_gn_and_sn_df = final_alias_df_2[final_alias_df_2["id"].isin(ids_to_find)]

# Process each group of records for each id
# Group by id and retain the third record for each group (the one we want to move from table final_alias_df_2 to df_final_9)
# 231 records
seperator_in_gn_and_sn_df = (
    seperator_in_gn_and_sn_df.groupby("id")
    .nth(2)  # Select the third record (index starts from 0, so 2 is the third)
    .reset_index()
)

# Rename columns in seperator_in_gn_and_sn_df to match df_final_9 where necessary
seperator_in_gn_and_sn_df = seperator_in_gn_and_sn_df.rename(
    columns={
        "alias_given_name_1": "given_name_1",
        "alias_given_name_2": "given_name_2",
        "alias_given_name_3": "given_name_3",
        "Alias_Surname": "Cleaned_Surname",
    }
)

# Select only the common columns
common_columns = [
    "id",
    "given_name_1",
    "given_name_2",
    "given_name_3",
    "Cleaned_Surname",
]
seperator_in_gn_and_sn_df = seperator_in_gn_and_sn_df[common_columns]

# Append the third records to df_final_11
# 612,831 records 22 columns (231 records added)
df_final_12 = pd.concat([df_final_11, seperator_in_gn_and_sn_df], ignore_index=True)

# Removing the records that aren't aliases from the final_alias_df_2 dataframe
# Combine all IDs from the lists into a single set
# 1,246 records
ids_to_remove = set(
    seperator_in_gn_not_sn_1
    + seperator_in_gn_not_sn_2
    + seperator_in_gn_not_sn_3
    + seperator_in_gn_and_sn_1
    + seperator_in_gn_and_sn_2
    + seperator_in_gn_and_sn_3
    + seperator_in_gn_and_sn_4
    + seperator_in_gn_and_sn_5
)

# Remove records with ids in the combined list
# 836 records (1620 removed)
final_alias_df_2_filtered = final_alias_df_2[~final_alias_df_2["id"].isin(ids_to_remove)]

"""
Dealing with below person record edge case:
404652
Rowan & Kelly
POPOWSKI (MIDSON)

Rowan MIDSON and Kelly MIDSON need to be removed from df_final_12
"""

# Find records with id = 404652 and drop the 2nd and 4th records from the filtered group
indices_to_remove = df_final_12[df_final_12["id"] == 404652].iloc[[1, 3]].index
# 612,829 records 22 columns (2 records removed)
df_final_12 = df_final_12.drop(indices_to_remove)

"""
# Creating unique id for the new person records we added.
# All new person records in a group have original id with suffixes _1, _2, ...
# These records have the same version, date_created, and last_updated as the first row in their group.
"""
df_final_13 = df_final_12.copy()

# Identify duplicate IDs and create suffixes
df_final_13["suffix"] = (
    df_final_13.groupby("id")
    .cumcount()  # Create count for each duplicate ID
    .apply(lambda x: f"_{x}" if x > 0 else "")  # Add suffix only to duplicates
)

# Append suffix to 'id'
df_final_13["id"] = df_final_13["id"].astype(str) + df_final_13["suffix"]
df_final_13.drop(columns=["suffix"], inplace=True)


def copy_info_for_ids(df, columns_to_copy):
    # Identify records with "_"
    underscore_ids = df[df["id"].str.contains("_", na=False)]

    for idx, row in underscore_ids.iterrows():
        # Extract the part of the id before "_"
        base_id = row["id"].split("_")[0]
        # Find the record with the base_id
        base_record = df[df["id"] == base_id]
        if not base_record.empty:
            # Copy the specified columns' values
            for col in columns_to_copy:
                df.at[idx, col] = base_record.iloc[0][col]

    return df


# Columns to copy
columns_to_copy = ["version", "date_created", "last_updated", "is_business", "is_employee", "can_exclude"]

# 612,829 records 22 columns
df_final_13 = copy_info_for_ids(df_final_13, columns_to_copy)

# Rename surname column
df_final_13.rename(columns={"Cleaned_Surname": "surname"}, inplace=True)

# Perform a join between final_alias_df_2_filtered and df_final_9
final_alias_df_3_filtered = pd.merge(
    final_alias_df_2_filtered,
    df_final_9[
        [
            "id",
            "version",
            "country_of_origin_id",
            "date_created",
            "date_of_birth",
            "indigenous_status_id",
            "last_updated",
            "ne_reason",
            "not_editable",
            "primary_address_id",
            "primary_contact_id",
            "racial_appearance_id",
            "sex_id",
            "care_of_address",
            "spi",
            "is_business",
            "is_employee",
            "can_exclude",
        ]
    ],
    on="id",
    how="inner",  # Ensures only matching records are included
)

# Rename the Alias_Surname column to alias_surname
final_alias_df_3_filtered = final_alias_df_3_filtered.rename(columns={"Alias_Surname": "alias_surname"})

# Reorganize columns to match the desired order
# 836 records and 22 columns
final_alias_df_3_filtered = final_alias_df_3_filtered[
    [
        "id",
        "version",
        "country_of_origin_id",
        "date_created",
        "date_of_birth",
        "alias_given_name_1",
        "alias_given_name_2",
        "alias_given_name_3",
        "indigenous_status_id",
        "last_updated",
        "ne_reason",
        "not_editable",
        "primary_address_id",
        "primary_contact_id",
        "racial_appearance_id",
        "sex_id",
        "alias_surname",
        "care_of_address",
        "spi",
        "is_business",
        "is_employee",
        "can_exclude",
    ]
]

"""
df_final_9 - 611,583 records and 22 columns - person table records with no additional records.
df_final_13 - 612,829 records and 22 columns - person table with new records with SAME id - person

final_alias_df_2 - 2,546 records and 6 columns - persons stored as aliases.
final_alias_df_3_filtered - 836 records and 22 columns - removed additional persons that were found to be new persons - person_alias
"""
