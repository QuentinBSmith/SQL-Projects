-- Exploratory Data Analysis Project

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Deleting Duplicates

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging2;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging2
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'

CREATE TABLE `layoffs_staging3` (
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
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging2;

DELETE
FROM layoffs_staging3
WHERE row_num > 1;

SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb';

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

-- Continuing Exploration

SELECT *
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging3;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY stage
ORDER BY 1 DESC;

SELECT company, industry, SUM(total_laid_off)
OVER(PARTITION BY industry ORDER BY total_laid_off DESC) as rolling_total
FROM layoffs_staging3;

SELECT SUBSTRING(`date`, 1,7) as `month`, SUM(total_laid_off)
FROM layoffs_staging3
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) as `month`, SUM(total_laid_off) as total_off
FROM layoffs_staging3
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, total_off, SUM(total_off) OVER (ORDER BY `month`) as rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) as Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

SELECT DISTINCT company, total_laid_off, percentage_laid_off, ROUND(total_laid_off / percentage_laid_off,0) as total_employees
FROM layoffs_staging3
WHERE total_laid_off IS NOT NULL
AND percentage_laid_off IS NOT NULL;

SELECT SUM(total_laid_off)
FROM layoffs_staging3;

WITH industry_layoffs as
(SELECT industry, SUM(total_laid_off) as total_layoffs
FROM layoffs_staging3
GROUP BY industry
ORDER BY total_layoffs DESC)
SELECT *, round((total_layoffs/383659) * 100,2) as industry_laid_off_percentage
FROM industry_layoffs
;


