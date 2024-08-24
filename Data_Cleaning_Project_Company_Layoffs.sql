-- Data Cleaning Project

SELECT *
FROM layoffs;

-- 1.Remove Duplicates 
-- 2.Standardize the Data
-- 3.Null Values or Blank Values
-- 4. Remove Any un needed columns


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


-- removing duplicates
 SELECT *, 
 ROW_NUMBER() OVER(
 PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
 SELECT *, 
 ROW_NUMBER() OVER(
 PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage, 
 country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_cte AS
(
 SELECT *, 
 ROW_NUMBER() OVER(
 PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage, 
 country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
 SELECT *, 
 ROW_NUMBER() OVER(
 PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage, 
 country, funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2;

-- standardizing data

-- Check each column for potential issues

-- Remove unnecessary space from company names
SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);



SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;

-- Need to update following industries as they're essentially the same
# industry
'Crypto'
'Crypto Currency'
'CryptoCurrency'

SELECT *
FROM layoffs_staging2
WHERE industry LIKE '%Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%'
;

SELECT DISTINCT industry
FROM layoffs_staging2
;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1
;
-- Everything seems ok in location column

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;

-- Issue found United States with and without a full stop / period.
# country
'United States'
'United States.'

-- First check to see which one os the correct version
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

-- without full stop is correct version


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) Trimmed_country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;


SELECT *
FROM layoffs_staging2
;

-- Data type for date is currently set as Text need to change it.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- Now we can alter the date data type 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Populate null values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_ofF IS NULL
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL

;



SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Check if other rows are populated 
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

# company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
'Airbnb', 'SF Bay Area', '', '30', NULL, '2023-03-03', 'Post-IPO', 'United States', '6400', '1'
'Airbnb', 'SF Bay Area', 'Travel', '1900', '0.25', '2020-05-05', 'Private Equity', 'United States', '5400', '1'

-- Found company column populated industry as 'Travel' we can now fill the empty space

-- Check what other companies have some rows with empty industries and some populated.

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

-- Transalatethe above to an update statement.

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;
 
-- UPDATE didnt work possibly because its ' ' and not null. next step set empty space to null.


UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- Retry update query 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- works great now


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
'Bally\'s Interactive', 'Providence', NULL, NULL, '0.15', '2023-01-18', 'Post-IPO', 'United States', '946', '1'
-- this one is still null

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- There only one row. Others had other rows (lay offs)

-- Removing un needed column and rows.

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_ofF IS NULL
;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_ofF IS NULL
;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Data is now CLEAN!

