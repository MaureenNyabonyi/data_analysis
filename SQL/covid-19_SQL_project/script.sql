-- Create table fatalities
CREATE TABLE covid_db.fatalities (
country_code TEXT,
continent TEXT,
location TEXT,
date TEXT,
population BIGINT DEFAULT NULL,
total_cases BIGINT DEFAULT NULL,
new_cases BIGINT DEFAULT NULL,
new_cases_smoothed FLOAT,
total_deaths BIGINT DEFAULT NULL,
new_deaths BIGINT DEFAULT NULL,
new_deaths_smoothed FLOAT,
total_cases_per_million BIGINT DEFAULT NULL,
new_cases_per_million BIGINT DEFAULT NULL,
new_cases_smoothed_per_million BIGINT DEFAULT NULL,
total_deaths_per_million BIGINT DEFAULT NULL,
new_deaths_per_million BIGINT DEFAULT NULL,
new_deaths_smoothed_per_million FLOAT,
reproduction_rate BIGINT DEFAULT NULL,
icu_patients BIGINT DEFAULT NULL,
icu_patients_per_million BIGINT DEFAULT NULL,
hosp_patients BIGINT DEFAULT NULL,
hosp_patients_per_million BIGINT DEFAULT NULL,
weekly_icu_admissions BIGINT DEFAULT NULL,
weekly_icu_admissions_per_million BIGINT DEFAULT NULL,
weekly_hosp_admissions BIGINT DEFAULT NULL,
weekly_hosp_admissions_per_million BIGINT DEFAULT NULL
);

-- Create table covid_vaccinations
CREATE TABLE covid_db.vaccinations (
country_code TEXT,
continent TEXT,
location TEXT,
date TEXT,
new_tests BIGINT,
total_tests BIGINT,
total_tests_per_thousand BIGINT,
new_tests_per_thousand BIGINT,
new_tests_smoothed BIGINT,
new_tests_smoothed_per_thousand FLOAT,
positive_rate FLOAT,
tests_per_case FLOAT,
tests_units BIGINT,
total_vaccinations BIGINT,
people_vaccinated BIGINT,
people_fully_vaccinated BIGINT,
new_vaccinations BIGINT,
new_vaccinations_smoothed BIGINT,
total_vaccinations_per_hundred FLOAT,
people_vaccinated_per_hundred FLOAT,
people_fully_vaccinated_per_hundred FLOAT,
new_vaccinations_smoothed_per_million BIGINT,
stringency_index FLOAT,
population_density FLOAT,
median_age FLOAT,
aged_65_older FLOAT,
aged_70_older FLOAT,
gdp_per_capita FLOAT,
extreme_poverty FLOAT,
cardiovasc_death_rate FLOAT,
diabetes_prevalence FLOAT,
female_smokers FLOAT,
male_smokers FLOAT,
handwashing_facilities FLOAT,
hospital_beds_per_thousand FLOAT,
life_expectancy FLOAT,
human_development_index FLOAT,
excess_mortality FLOAT
);

-- Load the CSV file into the tables (covid_db.fatalities and covid_db.vaccinations) 
-- format date to yyyy-mm-dd
LOAD DATA LOCAL INFILE '/dataset/covid_fatalities.csv' INTO TABLE covid_db.fatalities CHARACTER SET UTF8MB4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES 
(country_code,continent,location,@date,population,total_cases,new_cases,new_cases_smoothed,total_deaths,new_deaths,new_deaths_smoothed,total_cases_per_million,new_cases_per_million,new_cases_smoothed_per_million,total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,reproduction_rate,icu_patients,icu_patients_per_million,hosp_patients,hosp_patients_per_million,weekly_icu_admissions,weekly_icu_admissions_per_million,weekly_hosp_admissions,weekly_hosp_admissions_per_million)
SET date = STR_TO_DATE(@date, '%d/%m/%y');

LOAD DATA LOCAL INFILE '/dataset/covid_vaccination.csv' INTO TABLE covid_db.vaccinations CHARACTER SET UTF8MB4 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 LINES 
(country_code,continent,location,date,new_tests,total_tests,total_tests_per_thousand,new_tests_per_thousand,new_tests_smoothed,new_tests_smoothed_per_thousand,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,new_vaccinations,new_vaccinations_smoothed,total_vaccinations_per_hundred,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,new_vaccinations_smoothed_per_million,stringency_index,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index,excess_mortality)
SET date = STR_TO_DATE(@date, '%d/%m/%y');


-- 7 days moving average of new deaths in EUROPE between 2021-03-01 to 2021-07-04
SELECT date,
AVG(new_deaths) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as 7_day_rolling_average_of_new_infections
FROM covid_db.fatalities 
WHERE date BETWEEN '2021-03-01' AND '2021-07-04'
AND location = 'Europe';

-- total deaths as of 2021-07-04 order in DESC order. Excluding ('World', 'international', 'European Union', 'Oceania')
SELECT location, 
MAX(total_deaths) as max_deaths
FROM covid_db.fatalities 
WHERE LENGTH(continent) <= 0
AND location not in('World', 'international', 'European Union', 'Oceania')
GROUP BY location
ORDER BY max_deaths DESC;


-- Ordered average number of new deaths by location - Continents and Countries
SELECT location, ROUND(AVG(new_deaths)) AS average_fatalities FROM covid_db.fatalities GROUP BY location ORDER BY average_fatalities DESC;

-- Top 20 average no of cases of each country
SELECT continent, location,
ROUND(AVG((total_cases / population) * 100), 2) AS avg_percentage_of_population_infected
FROM covid_db.fatalities
GROUP BY continent, location
ORDER BY avg_percentage_of_population_infected DESC;

--Top 20 countries with the highest rate of infection in relation to the population size
SELECT location,
MAX(total_cases) AS max_cases,
ROUND(MAX(total_cases / population * 100), 2) AS percentage_of_population_infected FROM covid_db.fatalities
GROUP BY location
ORDER BY percentage_of_population_infected DESC
LIMIT 20;

-- countries with the highest no of fatalities ordered in descending order as of 2021-07-04
SELECT location, 
MAX(total_deaths) as max_deaths
FROM covid_db.fatalities 
WHERE LENGTH(continent) > 0
GROUP BY location
ORDER BY max_deaths DESC;

-- no of new vaccinated and moving average of new vaccinated over time in Europe
SELECT continent, location, date, new_vaccinations,
AVG(new_vaccinations) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as 7_days_moving_avg_vaccinations
FROM covid_db.vaccinations 
WHERE continent = 'Europe'
ORDER BY location, date;

-- share of people fully vaccinated as of 2021-07-04
SELECT location, 
ROUND(MAX(people_fully_vaccinated/fatalities.population) * 100 , 2) AS share_of_people_fully_vaccinated
FROM covid_db.vaccinations vaccinations 
INNER JOIN covid_db.fatalities fatalities USING(location, date)
GROUP BY location
ORDER BY share_of_people_fully_vaccinated DESC;

-- share of the population with at least one dose of the vaccine
SELECT location,
ROUND(MAX(vaccinations.people_vaccinated/fatalities.population) * 100 , 2) AS share_of_people_with_single_dose_of_vaccine
FROM covid_db.vaccinations vaccinations 
INNER JOIN covid_db.fatalities fatalities USING(location, date)
GROUP BY location
ORDER BY share_of_people_with_single_dose_of_vaccine DESC;

-- create a view (also known as a temporary table or named query) to use in computations of the uptake of the vaccine in relation to the population
CREATE OR REPLACE VIEW covid_db.new_vaccinations_vs_the_population AS
WITH new_vaccinations_vs_the_population_cte (continent, location, date, population, new_vaccinations, moving_sum_of_new_vaccinations) AS
(SELECT fatalities.continent, fatalities.location, fatalities.date, fatalities.population, vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (PARTITION BY fatalities.location
ORDER BY fatalities.date) AS moving_sum_of_new_vaccinations FROM covid_db.fatalities fatalities
JOIN covid_db.vaccinations vaccinations
ON fatalities.location = vaccinations.location
AND fatalities.continent = vaccinations.continent
AND fatalities.date = vaccinations.date)
SELECT continent, location, date, population, (moving_sum_of_new_vaccinations / population) * 100 AS percentage_of_new_vaccinations_vs_population
FROM new_vaccinations_vs_the_population_cte;

-- select the total percentage of new vaccinations in relation to the population
SELECT * FROM covid_db.new_vaccinations_vs_the_population;

-- get the top 20 countries with max percentage of new vaccinations vs the population as of 2021-07-04 from the view
SELECT location, continent, MAX(percentage_of_new_vaccinations_vs_population) as percentage_of_new_vaccinations_vs_population
FROM covid_db.new_vaccinations_vs_the_population
WHERE date = '2021-07-04'
GROUP BY location, continent
ORDER BY percentage_of_new_vaccinations_vs_population DESC limit 20;

