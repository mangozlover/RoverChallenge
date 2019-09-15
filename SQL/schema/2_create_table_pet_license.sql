USE rover;

CREATE TABLE IF NOT EXISTS pet_license (
    pet_license_id integer AUTO_INCREMENT,
	license_issue_date datetime NOT NULL,
	license_number varchar(20) NOT NULL,
	animal_s_name varchar(50) NULL,
	species varchar(50) NOT NULL,
	primary_breed varchar(50) NOT NULL,
	secondary_breed varchar(50) NULL,
	zip_code varchar(10) NULL,
    primary key pk_pet_license (pet_license_id)
);
