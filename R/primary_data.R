
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

t <- Sys.time()
#Date Pulls:
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
lb_ftid <- sqlQuery(channel,"select * from PACFIN.LBK_FTID")

fleets <- sqlQuery(channel,"select DISTINCT VESSEL_NUM, LANDING_DATE, FTID, FLEET_CODE, IS_IFQ_LANDING from PACFIN_MARTS.COMPREHENSIVE_FT WHERE MANAGEMENT_GROUP_CODE = 'GRND'")

lb <- sqlQuery(channel,"select TRIP_ID, AGID, VIDTYPE, DRVID, NCREW, NGAL, DYEAR, DMONTH, DDAY, DTIME,
               DPORT, RYEAR, RMONTH, RDAY, RTIME, RPORT, NTOWS from PACFIN.LBK_TRIP")

vc <- sqlQuery(channel,"select VESSEL_NUM, PACFIN_YEAR, max(CG_VESSEL_LENGTH), max(VESSEL_LENGTH), max(VESSEL_WEIGHT)
FROM PACFIN_MARTS.SWFSC_FISH_TICKETS
               GROUP BY VESSEL_NUM, PACFIN_YEAR" )
trips.ports <- sqlQuery(channel,paste("select VESSEL_NUM, LANDING_YEAR, PACFIN_PORT_CODE, count(distinct FTID)",
                                "from PACFIN_MARTS.COMPREHENSIVE_FT",
                                "where MANAGEMENT_GROUP_CODE='GRND'",
                                "group by VESSEL_NUM, LANDING_YEAR, PACFIN_PORT_CODE"))
close(channel)
Sys.time() - t

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
names(home.ports) <- c('DRVID','RYEAR','HOMEPORT','ntix','HOMEREGION')

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
#Get some diversity indicators

#-----------------------------------------------------------------------------
# Groundfish Revenue relative to total annual vessel revenue
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
revshares <- sqlQuery(channel,"select VESSEL_NUM, LANDING_YEAR, FLEET_CODE, MANAGEMENT_GROUP_CODE, sum(EXVESSEL_REVENUE) as rev
                      from PACFIN_MARTS.COMPREHENSIVE_FT
                      group by VESSEL_NUM, LANDING_YEAR, FLEET_CODE, MANAGEMENT_GROUP_CODE")
close(channel)
#-----------------------------------------------------------------------------


#----------------------------------------------------------------------------
# revenue shares of key groundfish species relative to groundfish revenue
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
gf.revshares <- sqlQuery(channel,"select VESSEL_NUM, LANDING_YEAR, FLEET_CODE, PACFIN_SPECIES_CODE, sum(EXVESSEL_REVENUE) as rev
                      from PACFIN_MARTS.COMPREHENSIVE_FT
                        where MANAGEMENT_GROUP_CODE = 'GRND'
                         group by VESSEL_NUM, LANDING_YEAR, FLEET_CODE, PACFIN_SPECIES_CODE")
close(channel)
#----------------------------------------------------------------------------

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
#use lat/long of tow sets to approximate distance traveled for each trip


#lat/long coordinates for each port and the key to match
# port identifiers between observer data and PacFIN data
port_codes <- read.csv("data/lbk_port_codes.csv")


#set-up port coordinates - we set this up by departure and
# return port because we will need lat/long for both in order
# to approximate the trip distance

dport_codes <- data.frame(d_port=port_codes$OBS_port,dport_lat=port_codes$LAT,dport_long=port_codes$LONG)
dport_codes <- dport_codes[!duplicated(dport_codes),]
dport_codes <- dport_codes[which(dport_codes$d_port!=""),]

rport_codes <- data.frame(r_port=port_codes$OBS_port,rport_lat=port_codes$LAT,rport_long=port_codes$LONG,rPCID=port_codes$PCID)
rport_codes <- rport_codes[!duplicated(rport_codes),]
rport_codes <- rport_codes[which(rport_codes$r_port!=""),]


#pull the tow level data from logbooks
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
tows <- sqlQuery(channel,"select TRIP_ID, TOWNUM, SET_LAT, SET_LONG, UP_LAT, UP_LONG, DEPTH1 from PACFIN.LBK_TOW")
close(channel)

lb_ports <- lb %>% select(TRIP_ID, AGID, DPORT, RPORT) 

tows <- tbl_df(tows) %>% left_join(lb_ports, by=c("TRIP_ID")) %>% mutate(SET_LONG=SET_LONG*-1)

#-------------------------------------------------------------------------------------------------------------
#before joining the port codes data frame with the tows data frame we have to fix the Astoria code because it's
# '02' which often gets read as 2:
port_codes$LBK_PORT <- as.character(port_codes$LBK_PORT)
port_codes$LBK_PORT[which(port_codes$AGID=='O' & port_codes$PCID=='AST')] <- '02'

#------------------------------------------------------------------------------------------------------------


d_port_codes <- port_codes[,c("LBK_PORT","AGID","LAT","LONG")]
names(d_port_codes) <- c("DPORT","AGID","LAT","LONG")
  
tows <- tows %>% left_join(d_port_codes,by=c('DPORT','AGID'))
names(tows) <- c('TRIP_ID','TOWNUM','SET_LAT','SET_LONG','UP_LAT','UP_LONG','DEPTH1','AGID','DPORT','RPORT','DPORT_LAT','DPORT_LONG')

r_port_codes <- port_codes[,c("LBK_PORT","AGID","LAT","LONG")]
names(d_port_codes) <- c("RPORT","AGID","LAT","LONG")

tows <- tows %>% left_join(d_port_codes,by=c('RPORT','AGID'))
names(tows) <- c('TRIP_ID','TOWNUM','SET_LAT','SET_LONG','UP_LAT','UP_LONG','DEPTH1','AGID','DPORT','RPORT',
                 'DPORT_LAT','DPORT_LONG','RPORT_LAT','RPORT_LONG')
#-----------------------------------------------------------------------------
#calculate the spatial midpoint of tows 
#midpoints <- df.tmp %>% mutate(x=cos(lat*(pi/180))*cos(long*(pi/180)),
#                               y=cos(lat*(pi/180))*sin(long*(pi/180)),
#                               z=sin(lat*(pi/180))) %>% #convert lat/long to radians then cartesian coordinates
#  group_by(trip_id) %>%
#  summarise(mean.x=mean(x),mean.y=mean(y),mean.z=mean(z)) %>%  #get average coordinates
#  mutate(long.center=atan2(mean.y,mean.x),
#         hyp=sqrt((mean.x*mean.x)+(mean.y*mean.y)),
#         lat.center=atan2(mean.z,hyp)) %>% #convert back to lat/long radians
#  mutate(lon.c=long.center*(180/pi),
#         lat.c=lat.center*(180/pi)) %>% #convert back to decimal degrees
#  select(trip_id,lat.c,lon.c)


#get port lat and longs
#tows <- wcop.df %>% left_join(dport_codes,by="d_port")
#tows <- wcop.df %>% left_join(rport_codes,by="r_port")
#-------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#calculate distance per trip by connecting tow sets

#first a diagnostic...number of trips with no good tow locations and number of 
# trips with number of good tow set locations = number of tows
tows.tmp <- tows %>% group_by(TRIP_ID) %>% mutate(bad_pos=ifelse(is.na(SET_LAT),1,0),good_pos=1-bad_pos) %>%
      summarise(townum=max(TOWNUM),good_pos=sum(good_pos),bad_pos=sum(bad_pos))

tows.tmp %>% mutate(all.good=ifelse(good_pos==townum,1,0)) %>% ungroup() %>% summarise(all.good=sum(all.good))

length(tows.tmp$good_pos[tows.tmp$good_pos==0])

dtemp.fn <- function(x,y){
  p1 <- (x*pi)/180
  p2 <- (y*pi)/180
  a <- sin((p2[1]-p1[1])*0.5)*sin((p2[1]-p1[1])*0.5) + sin((p2[2]-p1[2])*0.5)*sin((p2[2]-p1[2])*0.5)*cos(p1[1])*cos(p2[1])
  c <- 2*atan2(sqrt(a),sqrt(1-a))
  d <- c*6371
  return(d)
}

#filter the data set to include only good tow set locations
tow.locs <- tows %>% filter(!is.na(SET_LAT)) %>% filter(!is.na(SET_LONG)) %>% filter(!is.na(DPORT_LAT)) %>%
      filter(!is.na(RPORT_LAT)) %>% filter(!is.na(DPORT_LONG)) %>% filter(!is.na(RPORT_LONG)) %>%
      filter(SET_LAT > 20 & SET_LONG < 20) %>% 
      arrange(TRIP_ID,TOWNUM) 

#get the distance from departure port to first tow set
dstart <- tow.locs %>% arrange(TRIP_ID,TOWNUM) %>% group_by(TRIP_ID) %>% filter(row_number()==1) %>%
    mutate(dstart=dtemp.fn(x=c(DPORT_LONG,DPORT_LAT),y=c(SET_LONG,SET_LAT)))

quantile(dstart$dstart,probs=c(0.001,0.25,0.5,0.75,0.99))

# get distance from last tow to return port
dend <- tow.locs %>% arrange(TRIP_ID,TOWNUM) %>% group_by(TRIP_ID) %>% filter(row_number()==n()) %>%
  mutate(dend=dtemp.fn(x=c(RPORT_LONG,RPORT_LAT),y=c(SET_LONG,SET_LAT)))

# get distance between all tow sets on the trip...in order to do this one we need the trip to have
# at least two good tow set positions...for some reason this doesn't like my user defined function so
# we'll just hardcode the haversine distance
tow.dist <- tow.locs %>% select(TRIP_ID, TOWNUM, SET_LAT, SET_LONG, AGID, DPORT, RPORT) %>%
            group_by(TRIP_ID) %>% 
            arrange(TRIP_ID, TOWNUM) %>%
            mutate(ntows=n_distinct(TOWNUM),
                   lat.now=(SET_LAT*pi)/180, 
                   long.now=(SET_LONG*pi)/180,
                   lag.long=(lag(SET_LONG)*pi)/180,
                   lag.lat=(lag(SET_LAT)*pi/180)) %>%
            mutate(a=sin((long.now-lag.long)*0.5)*sin((long.now-lag.long)*0.5) + sin((lat.now-lag.lat)*0.5)*sin((lat.now-lag.lat)*0.5)*cos(lag.long)*cos(long.now),
                   c= 2*atan2(sqrt(a),sqrt(1-a)),
                   d=c*6371) %>%
            summarise(d=sum(d,na.rm=T))

#put the distances together
d <- dstart %>% select(TRIP_ID,dstart) %>% 
      inner_join(dend,by=c('TRIP_ID')) 
d <- d %>% 
    inner_join(tow.dist,by=c('TRIP_ID')) %>%
    mutate(trip.dist=dstart+dend+d)

#------------------------------------------------------------------------------

#add trip distances to logbooks
trip.dist <- d %>% select(TRIP_ID,trip.dist)  
lb <- lb %>% left_join(trip.dist,by=c('TRIP_ID'))

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


#get lbs and value
t <- Sys.time()
channel <- odbcConnect(dsn="pacfin",uid=paste(uid),pw=paste(pw),believeNRows=FALSE)
catch <- sqlQuery(channel,"select VESSEL_NUM, FTID, PACFIN_SPECIES_CODE, sum(LANDED_WEIGHT_LBS) as totalweight, sum(EXVESSEL_REVENUE) as totalvalue 
                 from PACFIN_MARTS.COMPREHENSIVE_FT
                 group by VESSEL_NUM, FTID, PACFIN_SPECIES_CODE")
close(channel)
Sys.time() - t


#gotta fix a bunch of nominal codes....it's easiest I think just to create a spreadsheet then
# do a read-in and merge
nominal.codes <- read.csv('data/nominal_codes.csv')
catch <- catch %>% mutate(NOMCODE=as.character(PACFIN_SPECIES_CODE))
  left_join(nominal.codes,by=c('NOMCODE'))


#keep every fish ticket in the logbook data
catch <- tbl_df(catch) %>% filter(FTID %in% unique(lb$FTID))

saveRDS(catch,"data/catch_by_ftid.RDA")

#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################


#Final step is to save the lb file


saveRDS(lb,"data/lb_trip.RDA")


#also save the revenue shares and groundfish species revenue shares for each vessel/year
saveRDS(revshares,"data/fishery_rev_shares.RDA")
saveRDS(gf.revshares,"data/gf_rev_shares.RDA")

