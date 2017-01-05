
library(RODBC)
library(dplyr)
library(lubridate)

################################################################################
#start with the full set of logbook data
channel<- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
lb <- sqlQuery(channel, "select * from PACFIN.LBK_TRIP")
lb_ftid <- sqlQuery(channel,"select * from PACFIN.LBK_FTID")
fleets <- sqlQuery(channel,"select VESSEL_NUM, LANDING_DATE,FTID, FLEET_CODE, IS_IFQ_LANDING from PACFIN_MARTS.COMPREHENSIVE_FT WHERE MANAGEMENT_GROUP_CODE = 'GRND'")
close(channel)

lb <- tbl_df(lb) %>% mutate(depart=as.Date(DDAY), return=as.Date(RDAY),DAS=as.numeric(return-depart)+1)



#------------------------------------------------------------------------------
#merge the trip ids with these fleet codes

#first some diagnotics...I think that a TRIP_ID can have many FTIDS but a FTID
# shouldn't have more than 1 TRIP_ID
ftid_multitrip <- lb_ftid %>% group_by(FTID) %>% summarise(tid_count=n_distinct(TRIP_ID)) %>% filter(tid_count>1)

#there are around 1,000 FTIDS with more than one TRIP_ID...there are also some fish tickets id's
# that get reused such that the same fish ticket can map to different vessels at different time
fleets %>% group_by(FTID) %>% summarise(vid_count=n_distinct(VESSEL_NUM)) %>% filter(vid_count > 1)





#------------------------------------------------------------------------------


#map each fish ticket to a fleet code and IFQ flag
fleets <- tbl_df(fleets) %>% mutate(IFQ=ifelse(IS_IFQ_LANDING=='FALSE',0,1),
                                    FLEET=ifelse(FLEET_CODE=='OA',1,
                                                 ifelse(FLEET_CODE=='LE',2,0))) %>%
  group_by(FTID) %>%
  summarise(IFQ=max(IFQ,na.rm=T),FLEET=max(FLEET,na.rm=T)) %>%
  mutate(LE=ifelse(FLEET==2,1,0))

###############################################################################


################################################################################
# get vessel characteristics....

# the new SWFSC_FISH_TICKETS Table that Rob Ames created makes this pretty easy
# Rob has reconciled the CG vessel characteristics and State Registry characteristics
# so that we can easily get what we need from the VESSEL_LENGTH & VESSEL_WEIGHT columns

channel<- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
vc <- sqlQuery(channel,"select VESSEL_NUM, PACFIN_YEAR, max(CG_VESSEL_LENGTH), max(VESSEL_LENGTH), max(VESSEL_WEIGHT)
FROM PACFIN_MARTS.SWFSC_FISH_TICKETS
GROUP BY VESSEL_NUM, PACFIN_YEAR" )
close(channel)

names(vc) <- c('DRVID','YEAR','CG_LEN','LEN','WT')

################################################################################

################################################################################
# for limited entry and IFQ fisheries we are just going to use the designations
# in the COMPREHENSIVE_FT Table


################################################################################

