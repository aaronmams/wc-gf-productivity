#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#read in the output file

catch <- readRDS('data/catch_by_ftid.RDA')
lb <- readRDS('data/lb_trip.RDA')
########################################################################
########################################################################
########################################################################
########################################################################
########################################################################
########################################################################

#merge species aggregates with catch data frame and declare any species not
# included in the grouping as 'OTH'
names(species.agg) <- c('PACFIN_SPECIES_CODE','GROUP')
catch <- tbl_df(catch) %>% left_join(species.agg,by=c('PACFIN_SPECIES_CODE')) %>%
            mutate(GROUP=ifelse(is.na(GROUP),'OTH',as.character(GROUP)))

#aggregate by vessel and fish ticket id
catch <- catch %>% group_by(VESSEL_NUM, FTID, GROUP) %>% 
          summarize(lbs=sum(TOTALWEIGHT,na.rm=T),val=sum(TOTALVALUE,na.rm=T))

#change names for consistency
names(catch) <- c('DRVID','FTID','GROUP','lbs','val')
########################################################################
########################################################################
########################################################################
########################################################################
########################################################################
########################################################################
#next join the fish ticket lbs and value to the logbooks

lb <- lb %>% left_join(catch,by=c('DRVID','FTID'))

########################################################################
########################################################################
########################################################################
########################################################################
########################################################################
########################################################################

