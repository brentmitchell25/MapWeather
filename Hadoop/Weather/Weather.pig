data = LOAD 'noaa.txt' USING PigStorage();
line = GROUP data BY ($0 as USAF, $3 as DIR, $4 as SPD, $21 as TEMP, $22 as DEWP);
result = FOREACH line GENERATE (AFB, B:bag{DIR == '****' : 0 : AVG(line),SPD == '****' : 0 : AVG(line),TEMP == '****' : 0 : AVG(line),DEWP == '****' : 0 : AVG(line)});
STORE result INTO '/pig-result' using PigStorage();