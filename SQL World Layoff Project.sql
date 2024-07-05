-- Data Cleaning


SELECT *
FROM layoffs;

-- 1. Remove Dupicates
-- 2. Standardize Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns or Rows


CREATE TABLE layoffs_staging
(LIKE layoffs);


SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;



SELECT *, ROW_NUMBER()OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, dates) AS row_num
FROM layoffs_staging;



WITH duplicate_cte AS 
(SELECT *, ROW_NUMBER()OVER(
	PARTITION BY company, locations, 
	industry, total_laid_off, percentage_laid_off, dates, stage, 
	country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';



-- Table: public.layoffs_staging

-- DROP TABLE IF EXISTS public.layoffs_staging;

CREATE TABLE IF NOT EXISTS public.layoffs_staging2
(
    company character varying(50) COLLATE pg_catalog."default",
    locations character varying(50) COLLATE pg_catalog."default",
    industry character varying(50) COLLATE pg_catalog."default",
    total_laid_off character varying(50) COLLATE pg_catalog."default",
    percentage_laid_off character varying(50) COLLATE pg_catalog."default",
    dates character varying(50) COLLATE pg_catalog."default",
    stage character varying(50) COLLATE pg_catalog."default",
    country character varying(50) COLLATE pg_catalog."default",
    funds_raised_millions character varying(50) COLLATE pg_catalog."default",
	row_num INT
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.layoffs_staging2
    OWNER to postgres;
	

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER()OVER(
	PARTITION BY company, locations, 
	industry, total_laid_off, percentage_laid_off, dates, stage, 
	country, funds_raised_millions) AS row_num
FROM layoffs_staging; 


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Confirming duplicates are deleted

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);



SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT *
FROM layoffs_staging2
WHERE total_laid_off LIKE 'NULL%'
AND percentage_laid_off LIKE 'NULL%';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
OR industry LIKE 'NULL%';


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT T1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '' OR t1.industry LIKE 'NULL%')
AND t2.industry IS NOT NULL OR t2.industry NOT LIKE 'NULL%';

UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
AND (t1.industry IS NULL OR t1.industry = '' OR t1.industry LIKE 'NULL%')
AND t2.industry IS NOT NULL AND t2.industry NOT LIKE 'NULL%';


SELECT *
FROM layoffs_staging2
WHERE total_laid_off LIKE 'NULL%'
AND percentage_laid_off LIKE 'NULL%';



DELETE 
FROM layoffs_staging2
WHERE total_laid_off LIKE 'NULL%'
AND percentage_laid_off LIKE 'NULL%';


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

