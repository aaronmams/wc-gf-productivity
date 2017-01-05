
library(RODBC)
channel<- odbcConnect(dsn="pacfin",uid="amamula",pw="mam2pac$",believeNRows=FALSE)
vc <- sqlQuery(channel,"select VESSEL_NUM, PACFIN_YEAR, max(CG_VESSEL_LENGTH), max(VESSEL_LENGTH)
FROM PACFIN_MARTS.SWFSC_FISH_TICKETS
GROUP BY VESSEL_NUM, PACFIN_YEAR" )
close(channel)
