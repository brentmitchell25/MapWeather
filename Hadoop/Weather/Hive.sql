CREATE TABLE 2013Weather 
(USAF INT,
WBAN INT,
YR INT,
DIR INT,
SPD INT,
GUS INT,
CLG INT,
SKC INT,
L INT,
MO INT,
H INT,
VSB DOUBLE,
MW INT,
MW2 INT,
MW3 INT,
MW4 INT,
AW INT,
AW2 INT,
AW3 INT,
AW4 INT,
TEMP INT,
DEWP INT,
SLP DOUBLE,
ALT INT,
STP DOUBLE,
MAXX INT,
MINN INT,
PCP01 DOUBLE,
PCP06 DOUBLE,
PCP24 DOUBLE,
PCPXX DOUBLE,
SD INT);

LOAD DATA INPATH '.' OVERWRITE INTO TABLE 2013Weather;
SELECT USAF, IF(DIR = '****', 0, AVG(DIR)), IF(SPD = '****', 0, AVG(SPD)), IF(TEMP = '****', 0, AVG(TEMP)), IF(DEWP = '****', 0, AVG(DEWP));