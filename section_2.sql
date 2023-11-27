-- question 1
SELECT de.department, jo.job, 
	SUM(CASE WHEN QUARTER(hired_dttm) = 1 THEN 1 ELSE 0 END) AS `Q1`,
    SUM(CASE WHEN QUARTER(hired_dttm) = 2 THEN 1 ELSE 0 END) AS `Q2`,
    SUM(CASE WHEN QUARTER(hired_dttm) = 3 THEN 1 ELSE 0 END) AS `Q3`,
    SUM(CASE WHEN QUARTER(hired_dttm) = 4 THEN 1 ELSE 0 END) AS `Q4`
FROM hired_employees he
LEFT JOIN departments de
	ON he.department_id = de.id
LEFT JOIN jobs jo
		ON he.job_id = jo.id
WHERE Year(hired_dttm) = 2021 -- parameterized, from API 
GROUP BY de.department, jo.job
ORDER BY de.department, jo.job
;

-- question 2
SELECT de.id, de.department, COUNT(*) AS hired
FROM hired_employees he
LEFT JOIN departments de
	ON he.department_id = de.id
WHERE Year(hired_dttm) = 2021 -- parameterized, from API
GROUP BY de.department
HAVING COUNT(*) > (
	SELECT COUNT(*)/COUNT(DISTINCT de.id) AS mean_year -- total hire / departments. not all departments hiring for any given year.
	FROM hired_employees he
	LEFT JOIN departments de
		ON he.department_id = de.id
	WHERE Year(hired_dttm) = 2021 -- parameterized, from API
)
ORDER BY hired DESC
;