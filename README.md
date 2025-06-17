CREATE DATABASE FINANCIAL_AUDIT;
USE FINANCIAL_AUDIT;

ALTER DATABASE FINANCIAL_AUDIT
MODIFY NAME = CRIME_RECORD;

CREATE TABLE CRIME_DATA (
             FINANCIAL_YEAR VARCHAR(15),
			 FINANCIAL_QUARTER INT,
			 FORCE_NAME VARCHAR(150),
			 OFFENCE_DESCRIPTION VARCHAR(150),
			 OFFENCE_GROUP VARCHAR(150),
			 OFFENCE_SUBGROUP VARCHAR(150),
			 OFFENCE_CODE VARCHAR(15),
			 NUMBER_OF_OFFENCES INT,
			 NUMBER_OF_OFFENCES_2 INT
);
  
  -------------------------------------------------------------------------------

EXEC sp_rename 'CRIME_DATA.NUMBER_OF_OFFENCES_2', 'NUMBER_OF_OFFENCES2', 'COLUMN';
ALTER TABLE  CRIME_DATA
ALTER COLUMN NUMBER_OF_OFFENCES2 VARCHAR(70);

ALTER TABLE CRIME_DATA
ALTER COLUMN OFFENCE_CODE VARCHAR(70);

ALTER TABLE CRIME_DATA
ALTER COLUMN NUMBER_OF_OFFENCES VARCHAR(70);

ALTER TABLE CRIME_DATA
ALTER COLUMN FINANCIAL_QUARTER VARCHAR(150);

BULK INSERT CRIME_DATA
FROM "C:\Program Files\Microsoft SQL Server\CRIME_DATA.csv"
WITH (
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
FIRSTROW = 2
);

------------------------------------------------------------------------------------------------

SELECT * 
FROM CRIME_DATA

SELECT 
    FORCE_NAME,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT OFFENCE_DESCRIPTION) as DISTINCT_OFFENCES,
    COUNT(*) - COUNT(OFFENCE_DESCRIPTION) as MISSING_DESCRIPTIONS,
    AVG(LEN(OFFENCE_DESCRIPTION)) as AVG_DESCRIPTION_LENGTH
FROM CRIME_DATA
GROUP BY FORCE_NAME

UNION ALL

SELECT 
    OFFENCE_GROUP,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT OFFENCE_GROUP) as DISTINCT_GROUPS,
    0 as MISSING_DESCRIPTIONS, -- placeholder to match column count
    AVG(LEN(OFFENCE_GROUP)) as AVG_GROUP_LENGTH
FROM CRIME_DATA
GROUP BY OFFENCE_GROUP;

-----------------------------------------------------------------------

SELECT
    FORCE_NAME,
    COUNT(*) as RECORD_COUNT,
    LEN(FORCE_NAME) as NAME_LENGTH
FROM CRIME_DATA
GROUP BY FORCE_NAME
ORDER BY COUNT(*) DESC;

---------------------------------------------------------------------------

ALTER TABLE CRIME_DATA 
ADD 
    CALENDAR_COLUMN VARCHAR(100);
    Total_Offences INT,
    Financial_Year_Start_Date DATE,
    Financial_Year_End_Date DATE,
    Calendar_Year INT,
    Calendar_Quarter INT,
    Quarter_Start_Date DATE,
    Quarter_End_Date DATE,
    Offence_Severity_Weight DECIMAL(3,2),
    Force_Category NVARCHAR(50),
    Crime_Category_Major NVARCHAR(100),
    Is_Violent_Crime BIT,
    Is_Property_Crime BIT,
    Offences_Per_100k_Population DECIMAL(10,2)
;

-------------------------------------------------------------------------------

ALTER TABLE CRIME_DATA ALTER COLUMN TOTAL_OFFENCES VARCHAR(150);
ALTER TABLE CRIME_DATA ALTER COLUMN FINANCIAL_YEAR_START_DATE DATE;
ALTER TABLE CRIME_DATA ALTER COLUMN FINANCIAL_YEAR_END_DATE DATE;
ALTER TABLE CRIME_DATA ALTER COLUMN CALENDAR_YEAR VARCHAR(100);
ALTER TABLE CRIME_DATA ALTER COLUMN QUARTER_START_DATE DATE;
ALTER TABLE CRIME_DATA ALTER COLUMN OFFENCE_SEVERITY_WEIGHT DECIMAL(3,2);
ALTER TABLE CRIME_DATA ALTER COLUMN FORCE_CATEGORY NVARCHAR(50);
ALTER TABLE CRIME_DATA ALTER COLUMN CRIME_CATEGORY_MAJOR NVARCHAR(100);
ALTER TABLE CRIME_DATA ALTER COLUMN IS_VIOLENT_CRIME BIT;
ALTER TABLE CRIME_DATA ALTER COLUMN IS_PROPERTY_CRIME BIT;
ALTER TABLE CRIME_DATA ALTER COLUMN OFFENCES_PER_100k_POPULATION DECIMAL(10,2);

-----------------------------------------------------------------------------------------

UPDATE CRIME_DATA
SET TOTAL_OFFENCES = 
    COALESCE(TRY_CAST(REPLACE(NUMBER_OF_OFFENCES, ',', '') AS FLOAT), 0) +
    COALESCE(TRY_CAST(REPLACE(NUMBER_OF_OFFENCES2, ',', '') AS FLOAT), 0);

UPDATE CRIME_DATA
SET NUMBER_OF_OFFENCES2 = REPLACE(NUMBER_OF_OFFENCES2, ',', '');

------------------------------

SELECT 
    'Both columns match' as comparison_type,
    COUNT(*) as record_count
FROM CRIME_DATA 
WHERE [NUMBER_OF_OFFENCES] = [NUMBER_OF_OFFENCES2]
UNION ALL
SELECT 
    'Column1 > Column2',
    COUNT(*)
FROM CRIME_DATA
WHERE [NUMBER_OF_OFFENCES] > [NUMBER_OF_OFFENCES2]
UNION ALL
SELECT 
    'Column2 > Column1',
    COUNT(*)
FROM CRIME_DATA 
WHERE [NUMBER_OF_OFFENCES2] > [NUMBER_OF_OFFENCES]
UNION ALL
SELECT 
    'One column is NULL',
    COUNT(*)
FROM CRIME_DATA
WHERE [NUMBER_OF_OFFENCES] IS NULL OR [NUMBER_OF_OFFENCES2] IS NULL;

------------------------------------------------

UPDATE CRIME_DATA
SET 
    Financial_Year_Start_Date = 
CASE 
     WHEN FINANCIAL_YEAR LIKE '%/%' 
     THEN DATEFROMPARTS(CAST(LEFT([FINANCIAL_YEAR], 4) AS INT), 4, 1)
     ELSE DATEFROMPARTS(CAST([FINANCIAL_YEAR] AS INT), 4, 1)
  END,

Financial_Year_End_Date = 
 CASE 
      WHEN [FINANCIAL_YEAR] LIKE '%/%' 
      THEN DATEFROMPARTS(CAST(LEFT([FINANCIAL_YEAR], 4) AS INT) + 1, 3, 31)
      ELSE DATEFROMPARTS(CAST([FINANCIAL_YEAR] AS INT) + 1, 3, 31)
  END,

 Calendar_Year = 
        CASE 
            WHEN [FINANCIAL_YEAR] LIKE '%/%' 
            THEN CAST(LEFT([FINANCIAL_YEAR], 4) AS INT)
            ELSE CAST([FINANCIAL_YEAR] AS INT)
        END;

-- Calculate quarter dates based on financial quarters
UPDATE CRIME_DATA
SET 
    Calendar_Quarter = 
        CASE FINANCIAL_QUARTER
            WHEN 'Q1' THEN 1
            WHEN 'Q2' THEN 2  
            WHEN 'Q3' THEN 3
            WHEN 'Q4' THEN 4
            ELSE NULL
        END,
    
    Quarter_Start_Date = 
        CASE FINANCIAL_QUARTER
            WHEN 'Q1' THEN DATEADD(MONTH, 0, Financial_Year_Start_Date)   
            WHEN 'Q2' THEN DATEADD(MONTH, 3, Financial_Year_Start_Date)    
            WHEN 'Q3' THEN DATEADD(MONTH, 6, Financial_Year_Start_Date)   
            WHEN 'Q4' THEN DATEADD(MONTH, 9, Financial_Year_Start_Date)   
            ELSE NULL
        END,
    
      Quarter_End_Date = 
        CASE FINANCIAL_QUARTER
            WHEN 'Q1' THEN DATEADD(DAY, -1, DATEADD(MONTH, 3, Financial_Year_Start_Date))
            WHEN 'Q2' THEN DATEADD(DAY, -1, DATEADD(MONTH, 6, Financial_Year_Start_Date))
            WHEN 'Q3' THEN DATEADD(DAY, -1, DATEADD(MONTH, 9, Financial_Year_Start_Date))
            WHEN 'Q4' THEN Financial_Year_End_Date
            ELSE NULL
 END; 

 -----------------------------------------------------------------
	
	`
SELECT *
FROM CRIME_DATA



ALTER TABLE CRIME_DATA
DROP COLUMN Offence_Severity_Weight;

------------------------------------------------------------------------

CREATE VIEW V_QUARTERLY_CRIME_REVIEW AS
SELECT 
    Calendar_Year,
    Financial Quarter,
    COUNT(*) as Crime_Types_Count,
    SUM(Total_Offences) as Total_Offences,
    
    SUM(CASE WHEN Is_Violent_Crime = 1 THEN Total_Offences ELSE 0 END) as Violent_Crime_Count,
    SUM(CASE WHEN Is_Property_Crime = 1 THEN Total_Offences ELSE 0 END) as Property_Crime_Count
FROM your_table_name
WHERE Total_Offences > 0
GROUP BY 
    Calendar_Year, Financial Quarter,  
    Force_Category, Crime_Category_Major;
