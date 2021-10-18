
CREATE PROCEDURE createCountryTable(
IN CountryName VARCHAR(40)
)
BEGIN
SET @table := CountryName;
SET @sql_text:=CONCAT('
CREATE TABLE ',@table, '(
customerName varchar(255) not null,
customerID varchar(18),
customerOpenDate date not null,
lastConsultedDate date,
vaccinationType varchar(5),
doctorConsulted varchar(255),
state char(5),
country char(5),
dateOfBirth date,
activeCustomer char(1),
primary key(customerName)
)');
PREPARE stmt from @sql_text;
EXECUTE stmt;
END;