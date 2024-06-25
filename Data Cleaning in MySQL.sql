-- Data Cleaning in MySQL
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
from layoffs;

-- This is the plan i intend on following to clean the dataset
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null Values or Blank Values
-- 4. Remove any columns that are not needed

-- Before anything is done, a deuplicated table would be created, this is to ensure that te original table isnt messed up or damage during the process of cleaning in situiations of human error.

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;

-- Next we take the first step of the data cleaning process which is to find and remove duplicates

-- Finding DUPLICATES
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Now we have to identify the duplicate rows in the table
with duplicate_cte AS
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- To get rid of the dup,icates, i considered creating a duplicate table that includes the row_num column. This way i would be able to get rid of the duplicates

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
  `row_num` int
) ;

select *
from layoffs_staging2;

 -- Now i will add the values into the table, including the row_num column
 
INSERT INTO layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Now i identify the duplicate rows

select *
from layoffs_staging2
where row_num > 1;

-- After identifying the, the next step is to get rid of them as they are not needed
delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- Standardizing data
-- This next step has to do with making sure the values in each column align and excess space in each row is removed
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

-- Here i attempt to fix typo errors noticed in the table
select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Cyrpto%';

-- Here im inspecting just to figure out if there are error in typing these values. The distinct oprtion makes sure there arent multiple entries of the same value, which makes it easir to spot error in the input of values

select distinct industry
from layoffs_staging2;

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

-- In the country column, United_States was wronglyy inputted, after identifying this error, i can attempt to rectify it
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;
 
 -- This step makes sure that every value is Updated as United States, which saves the confusion when doing analysis
 update layoffs_staging2
 set country = trim(trailing '.' from country)
 where country like 'United States%';
 
 select distinct country
 from layoffs_staging2
 order by 1;
 
 -- I found out the date column is stored as a string.. This will cause further confusion if ignored. To fix this, we just have to change the date column from string back to date format
 select `date`,
 str_to_date(`date`, '%m/%d/%Y')
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date` = str_to_date(`date`, '%m/%d/%Y');
 
 select `date`
 from layoffs_staging2;
 
 alter table layoffs_staging2
 modify column `date` date;
 
 -- 3. Null Values or Blank Values
 -- Identify Null Values in the table
 select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 -- Now we look at the company column
 
 select *
 from layoffs_staging2
 where company in ('Airbnb', 'Carvana', 'Juul', 'Bally''s interactive');
 
 -- The airbnb is a travel, but this one just isn't populated.
-- it's the same for the others. The idea is to write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it relatively easy so if there were thousands i wouldn't have to manually check them all
 
 -- next step is to set the blanks to nulls since those are typically easier to work with
 select *
 from layoffs_staging2
 where industry is null
 or industry = ' ';
 
  update layoffs_staging2
 set industry = null
 where industry = ' ';

-- now we need to populate those nulls if possible

select *
from layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL AND t2.industry <> '';

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL AND t2.industry <> '';

-- Check to see if it was successful

select *
from layoffs_staging2;

select *
from layoffs_staging2
where company = 'Airbnb';


-- Removing Columns that are not needed
 select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 -- Deleting data that are of no use
 delete 
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select *
 from layoffs_staging2;
 
 -- Delete the row_num column
 alter table layoffs_staging2
 drop column row_num;
 