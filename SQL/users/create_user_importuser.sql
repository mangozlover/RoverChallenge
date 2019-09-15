USE rover;

CREATE USER IF NOT EXISTS 'importuser'@'%' IDENTIFIED BY 'IluvY0u2';

GRANT INSERT ON rover.* TO 'importuser'@'%';
GRANT SELECT ON rover.pet_license TO 'importuser'@'%';
GRANT DROP ON rover.* TO 'importuser'@'%';

FLUSH PRIVILEGES;