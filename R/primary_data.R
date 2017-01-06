
library(RODBC)
library(dplyr)
library(lubridate)

################################################################################
#start with the full set of logbook data

#channel<- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
#lb <- sqlQuery(channel,paste("select PACFIN.LBK_TRIP.TRIP_ID,",
#                             "PACFIN.LBK_TRIP.AGID,", 
#                             "PACFIN.LBK_TRIP.DRVID,",
#                             "PACFIN.LBK_TRIP.NCREW,",
#                             "PACFIN.LBK_TRIP.NGAL,", 
#                             "PACFIN.LBK_TRIP.DDAY,", 
#                             "PACFIN.LBK_TRIP.RDAY,", 
#                             "PACFIN.LBK_TOW.TOWNUM,", 
#                             "PACFIN.LBK_TOW.TOW_DATE,",
#                              "PACFIN.LBK_TOW.DURATION,", 
#                             "PACFIN.LBK_TOW.SET_LAT,", 
#                             "PACFIN.LBK_TOW.SET_LONG,", 
#                             "PACFIN.LBK_TOW.UP_LAT,",
#                             "PACFIN.LBK_TOW.UP_LONG,", 
#                              "PACFIN.LBK_TOW.DEPTH1,", 
#                             "PACFIN.LBK_TOW.DEPTH2",
#               "from PACFIN.LBK_TRIP",
#               "inner join PACFIN.LBK_TOW", 
#                "on PACFIN.LBK_TRIP.TRIP_ID=PACFIN.LBK_TOW.TRIP_ID"))
#close(channel)

#Date Pulls:
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
lb_ftid <- sqlQuery(channel,"select * from PACFIN.LBK_FTID")
fleets <- sqlQuery(channel,"select VESSEL_NUM, LANDING_DATE,FTID, FLEET_CODE, IS_IFQ_LANDING from PACFIN_MARTS.COMPREHENSIVE_FT WHERE MANAGEMENT_GROUP_CODE = 'GRND'")
lb <- sqlQuery(channel,"select * from PACFIN.LBK_TRIP")
vc <- sqlQuery(channel,"select VESSEL_NUM, PACFIN_YEAR, max(CG_VESSEL_LENGTH), max(VESSEL_LENGTH), max(VESSEL_WEIGHT)
FROM PACFIN_MARTS.SWFSC_FISH_TICKETS
               GROUP BY VESSEL_NUM, PACFIN_YEAR" )
trips.ports <- sqlQuery(channel,paste("select VESSEL_NUM, LANDING_YEAR, PACFIN_PORT_CODE, count(FTID)",
                                "from PACFIN_MARTS.COMPREHENSIVE_FT",
                                "where MANAGEMENT_GROUP_CODE='GRND'",
                                "group by VESSEL_NUM, LANDING_YEAR, PACFIN_PORT_CODE"))
close(channel)

lb <- tbl_df(lb) %>% mutate(depart=as.Date(DDAY), return=as.Date(RDAY),DAS=as.numeric(return-depart)+1)

#-----------------------------------------------------------------------------
#merge the logbook-ftid map into the logbooks but don't merge by TOW_NUM because
# there are too many NAs for TOW_NUM in the LBK_FTID file

#collapse to just unique FTIDS for each TRIP_ID
lb_ftid <- lb_ftid %>% select(TRIP_ID, FTID, TICKET_DATE) %>% arrange(TRIP_ID, FTID) %>%
              group_by(TRIP_ID, FTID, TICKET_DATE) %>% filter(row_number()==1)

ntix <- lb_ftid %>% group_by(TRIP_ID) %>% summarise(ntix=n_distinct(FTID))
#ggplot(ntix,aes(x=ntix)) + geom_histogram()

       
lb <- tbl_df(lb) %>% left_join(lb_ftid,by=c('TRIP_ID'))
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
# some diagnostics

#first some diagnotics...I think that a TRIP_ID can have many FTIDS but a FTID
# shouldn't have more than 1 TRIP_ID
ftid_multitrip <- lb_ftid %>% group_by(FTID) %>% summarise(tid_count=n_distinct(TRIP_ID)) %>% filter(tid_count>1)

#there are around 1,000 FTIDS with more than one TRIP_ID...there are also some fish tickets id's
# that get reused such that the same fish ticket can map to different vessels at different time
multi_boat <- fleets %>% group_by(FTID) %>% summarise(vid_count=n_distinct(VESSEL_NUM)) %>% filter(vid_count > 1)

#find out if any of the multi boat FTIDS map to more than one trip id
lb_ftid %>% filter(FTID %in% multi_boat$FTID) %>% group_by(FTID) %>%
            summarise(tid_count=n_distinct(TRIP_ID)) %>% filter(tid_count>1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#map each fish ticket to a fleet code and IFQ flag
fleets <- tbl_df(fleets) %>% mutate(IFQ=ifelse(IS_IFQ_LANDING=='FALSE',0,1),
                                    FLEET=ifelse(FLEET_CODE=='OA',1,
                                                 ifelse(FLEET_CODE=='LE',2,0))) %>%
  group_by(VESSEL_NUM,FTID) %>%
  summarise(IFQ=max(IFQ,na.rm=T),FLEET=max(FLEET,na.rm=T)) %>%
  mutate(LE=ifelse(FLEET==2,1,0))
names(fleets) <- c('DRVID','FTID','IFQ','FLEET','LE')
  
lb <- lb %>% left_join(fleets,by=c('DRVID','FTID'))
#------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# NOTE: the problem of having fish tickets that correspond to different trips from
# different vessels is addressed by merging fish tickets and fleet codes with
# logbook trip file using Vessel Identifier and Fish Ticket Identifier....
#  the problem of a single fish ticket corresponding to two different trips by the same
#  vessel is addressed later when aggregate landings and revenue by trips.  Then we'll
# just split the lbs & rev of the fish ticket equally among all TRIP_IDs that the fish 
# ticket corresponds to.


#-------------------------------------------------------------------------------

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################


################################################################################
# get vessel characteristics....

# the new SWFSC_FISH_TICKETS Table that Rob Ames created makes this pretty easy
# Rob has reconciled the CG vessel characteristics and State Registry characteristics
# so that we can easily get what we need from the VESSEL_LENGTH & VESSEL_WEIGHT columns

names(vc) <- c('DRVID','RYEAR','CG_LEN','LEN','WT')
vc <- tbl_df(vc) %>% arrange(DRVID,RYEAR)

#merge vessel characteristics with logbook trips
lb <- lb %>% inner_join(vc,by=c('DRVID','RYEAR'))

################################################################################


###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
# Assign each vessel to a homeport and home region for each year based on 
# plurality of revenue...also save another data frame that has a count of 
# number of trips by each vessel out of each port in each year.

names(trips.ports) <- c('DRVID','RYEAR','PACFIN_PORT_CODE','ntix')

home.ports <- tbl_df(trips.ports) %>% arrange(DRVID,RYEAR,-ntix) %>% 
    group_by(DRVID,RYEAR) %>% filter(row_number()==1)

#now add home region
port.region <- data.frame(
  rbind
  (cbind(rep("CONCEPTION",8),c("OXN","SB","VEN","TRM","AVL","MOR","MRO","OBV")),
    cbind(rep("MONTEREY",16),c("MNT","MOS","SCZ","BDG","OAK","PRN","SF","BOL","BKL","RCH","RYS","SLT","TML",
                               "ALB","BRG","OSF")),
    cbind(rep("EUREKA",7),c("CRS","ERK","FLN","TRN","BRK",
                            "GLD","ORF")),
    cbind(rep("COLUMBIA",24),c("AST","GSS","CNB","CRV","ILW","LWC","NHL","TLL","NTR", "NEW","DPO","FLR","SLZ","PCC",
                               "BDN","COS","WIN","CPL","GRH","OWC","WLB","WES","WPT","OLY")),
    cbind(rep("VANCOUVER",18),c("ANA","BLA","BLL","LAP","BLN","FRI","LAC","N.B","NEA","ONP","PAG",
                                "SEQ","TNS","EVR","OSP","SEA","SHL","TAC"))
  )
)
names(port.region) <- c('REGION','PACFIN_PORT_CODE')
home.ports <- home.ports %>% left_join(port.region,by=c('PACFIN_PORT_CODE'))


trips.ports <- trips.ports %>% filter(DRVID %in% unique(lb$DRVID))

saveRDS(trips.ports,"data/trips_ports.RDA")

#merge homeport with logbook
lb <- lb %>% left_join(home.ports,by=c('DRVID','RYEAR'))

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
