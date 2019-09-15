DELIMITER //
 
CREATE PROCEDURE GetLicenseCountBySpeciesYear()
BEGIN
	CREATE TEMPORARY TABLE myData 
	SELECT p.species AS species
			, YEAR(license_issue_date) AS license_issue_date_year
			, COUNT(p.license_number) AS pet_license_count
	FROM pet_license p
	GROUP BY species,YEAR(license_issue_date);
	   
	CREATE TEMPORARY TABLE output 
	SELECT species
		, license_issue_date_year
		, 0 as pet_license_count
	FROM 
		  ( SELECT       'Cat' AS species 
			UNION SELECT 'Dog' 
			UNION SELECT 'Goat' 
			UNION SELECT 'Pig'
			) AS myspecies 
	CROSS JOIN  
		  ( SELECT       '2003' AS license_issue_date_year 
			UNION SELECT '2006'
			UNION SELECT '2008'
			UNION SELECT '2011'
			UNION SELECT '2014'
			UNION SELECT '2015'
			UNION SELECT '2016'
			UNION SELECT '2017'
			UNION SELECT '2018'
			UNION SELECT '2019'
			) AS myyears
	ORDER BY species, license_issue_date_year;

	UPDATE output o, myData d
	SET    o.pet_license_count  = d.pet_license_count
	WHERE  o.species = d.species
	AND    o.license_issue_date_year = d.license_issue_date_year;
    
    SELECT * FROM output;
END //
 
DELIMITER ;