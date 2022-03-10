#microtox data modernization clean up
#Keleigh Reynolds
#7/24/20

#first load packages and grab files-2 tables will be created
library(dplyr)

#sites file to get the correct site IDs
sites<-read.csv("data/sites.csv",stringsAsFactors = FALSE)

#start with the sediment data
sed1<-read.csv("data/2018 toxicity sediment and water results_sediment.csv",stringsAsFactors = FALSE)
sed2<-read.csv("data/2019 toxicity Delaware_Genesee_StLawrence (SCR)_Wallkill (SS)_sed.csv",stringsAsFactors = FALSE)
sed3<-read.csv("data/sediment.with.lat.lon.csv",stringsAsFactors = FALSE)

#sed 3 is the only one that looks like the stations need correcting, so lets do that
sites.short<-sites %>% 
  select(ADJUST_RIBS_ID,SBU_ID) %>% 
  rename("stationID"=ADJUST_RIBS_ID)

sed3.1<-merge(sed3,sites.short,by="stationID", all.x = TRUE)
#there ares ome that have the correct station ID's lets get them in here too
sed3.1$SBU_ID<-ifelse(is.na(sed3.1$SBU_ID),sed3.1$stationID,sed3.1$SBU_ID)

sed3.1$stationID<-NULL
sed3.1<-sed3.1 %>% 
  rename("stationID"=SBU_ID)


sed.all<-rbind(sed1,sed2,sed3.1)#bind them all together into one file
#take out the weird blank rows
sed.all<-sed.all %>% 
  filter(!station=="")


#split the dates out
library(tidyr)
sed.all<-separate(sed.all,Sediment.Collection.Test.Date,
                  c("TSR_COLLECTION_DATE","TSR_SED_TEST_DATE"),
                  sep = "/")
not.sampled<-sed.all %>% 
  filter(TSR_COLLECTION_DATE=="NOT SAMPLED")#break into 2 dfs, one for the ones that weren'tsampled

sed.all<-sed.all %>% 
  filter(!TSR_COLLECTION_DATE=="NOT SAMPLED")#one for the rest with data

#split the other ones out

sed.all<-separate(sed.all,TSR_SED_TEST_DATE,c("TSR_SED_TEST_DATE","TSR_POREWATER_TEST_DATE"),
                                              sep="&")
#now get the dates fixed, since its 10/1 & 2/2018
#first get rid of the trailing ws
sed.all$TSR_SED_TEST_DATE<-trimws(sed.all$TSR_SED_TEST_DATE)

library(stringr)
sed.all$temp<-str_sub(sed.all$TSR_COLLECTION_DATE,-3,-1)#first gather what we needd to paste in

sed.all<-sed.all %>% 
  mutate(TSR_SED_TEST_DATE=ifelse(nchar(TSR_SED_TEST_DATE)<6,
                                  paste(TSR_SED_TEST_DATE,temp,sep = ""),
                                  TSR_SED_TEST_DATE))
#repeat with other column
sed.all$TSR_POREWATER_TEST_DATE<-trimws(sed.all$TSR_POREWATER_TEST_DATE)

#fillin the NA's
sed.all$TSR_POREWATER_TEST_DATE<-ifelse(is.na(sed.all$TSR_POREWATER_TEST_DATE),
                                        paste(sed.all$TSR_SED_TEST_DATE),
                                        sed.all$TSR_POREWATER_TEST_DATE)
sed.all$temp2<-str_sub(sed.all$TSR_SED_TEST_DATE,1,3)

sed.all<-sed.all %>% 
  mutate(TSR_POREWATER_TEST_DATE=ifelse(nchar(TSR_POREWATER_TEST_DATE)<6,
                                  paste(temp2,TSR_POREWATER_TEST_DATE,sep = ""),
                                  TSR_POREWATER_TEST_DATE))
#make the dates in the correct format
sed.all$TSR_COLLECTION_DATE<-as.Date(sed.all$TSR_COLLECTION_DATE,"%m-%d-%y")
sed.all$TSR_POREWATER_TEST_DATE<-as.Date(sed.all$TSR_POREWATER_TEST_DATE,"%m-%d-%y")
sed.all$TSR_SED_TEST_DATE<-as.Date(sed.all$TSR_SED_TEST_DATE,"%m-%d-%y")

sed.all$TSR_COLLECTION_DATE<-format(sed.all$TSR_COLLECTION_DATE,"%m/%d/%Y")
sed.all$TSR_POREWATER_TEST_DATE<-format(sed.all$TSR_POREWATER_TEST_DATE,"%m/%d/%Y")
sed.all$TSR_SED_TEST_DATE<-format(sed.all$TSR_SED_TEST_DATE,"%m/%d/%Y")
sed.all$date<-paste(sed.all$TSR_COLLECTION_DATE)
sed.all$date<-as.Date(sed.all$date,"%m/%d/%Y")
sed.all$date<-format(sed.all$date,"%Y%m%d")

#SWEET
sed.all$Porewater.EC50....<-str_remove(sed.all$Porewater.EC50....,">")
sed.all<-sed.all %>% 
  mutate(TSR_SEDIMENT_RSLT_QLFR="",TSR_POREWATER_RSLT_QLFR="",EVENT_SMAS_ID=paste(stationID,date,sep = "_")) %>% 
  rename("TSR_SEDIMENT_ASMT"=Sediment.Assessment,
         "TSR_POREWATER_ASMT"=Porewater.Assessment,
         "TSR_SEDIMENT_RSLT"=Sediment.EC50....,
         "TSR_POREWATER_RSLT"=Porewater.EC50....)
#select the columns we want

sed.final<-sed.all %>% 
  select(EVENT_SMAS_ID,
         TSR_COLLECTION_DATE,
         TSR_SED_TEST_DATE,
         TSR_POREWATER_TEST_DATE,
         TSR_SEDIMENT_RSLT,
         TSR_SEDIMENT_RSLT_QLFR,
         TSR_POREWATER_RSLT,
         TSR_POREWATER_RSLT_QLFR,
         TSR_SEDIMENT_ASMT,
         TSR_POREWATER_ASMT
  )
write.csv(sed.final,"outputs/S_TOXICITY_SEDIMENT_RESULT.csv",row.names = FALSE)
#################################################################################################
#water tox results with c.dubia bugs
#clean up

#read files
w1<-read.csv("data/water.with.lat.lon.csv",stringsAsFactors = FALSE)
w2<-read.csv("data/2018 toxicity sediment and water results_water.csv",stringsAsFactors = FALSE)
w3<-read.csv("data/2019 toxicity Delaware_Genesee_StLawrence (SCR)_Wallkill (SS)_dubia.csv",stringsAsFactors = FALSE)

#first correct the station ID's
#sites file to get the correct site IDs
sites<-read.csv("data/sites.csv",stringsAsFactors = FALSE)

sites.short<-sites %>% 
  select(ADJUST_RIBS_ID,SBU_ID) %>% 
  rename("stationID"=ADJUST_RIBS_ID)

sites.short$stationID_num<-as.numeric(sites.short$stationID)

w1.1<-merge(w1,sites.short,by="stationID", all.x = TRUE)
w1.1<-merge(w1.1,sites.short,by.x="stationID",by.y ="stationID_num",all.x = TRUE)

w1.1$SBU_ID<-ifelse(is.na(w1.1$SBU_ID.x),paste(w1.1$SBU_ID.y),w1.1$SBU_ID.x)

w1.1$stationID<-NULL
w1.1<-w1.1 %>% 
  rename("stationID"=SBU_ID)

w1.1$stationID.y<-NULL
w1.1$SBU_ID.x<-NULL
w1.1$SBU_ID.y<-NULL
w1.1$stationID_num<-NULL


w.all<-rbind(w1.1,w2,w3)#bind them all together into one file
#take out the weird blank rows
w.all<-w.all %>% 
  filter(!station=="")

#split dates apart
not.sampled<-w.all %>% 
  filter(Water.Collection.Test.Start.Date=="NOT SAMPLED")#break into 2 dfs, one for the ones that weren'tsampled

w.all<-w.all %>% 
  filter(Water.Collection.Test.Start.Date!="NOT SAMPLED"&
           Water.Collection.Test.Start.Date!="NOT TESTED"&
           Water.Collection.Test.Start.Date!="N/A")#one for the rest with data

w.all<-separate(w.all,Water.Collection.Test.Start.Date,
                c("TWR_COLLECTION_DATE","TWR_TEST_START_DATE"),
                  sep="/")
w.all$TWR_COLLECTION_DATE<-as.Date(w.all$TWR_COLLECTION_DATE,"%m-%d-%y")
w.all$TWR_TEST_START_DATE<-as.Date(w.all$TWR_TEST_START_DATE,"%m-%d-%y")

#create event date to make SMAS event ID field
w.all$event.date<-w.all$TWR_COLLECTION_DATE

#format for db
w.all$TWR_COLLECTION_DATE<-format(w.all$TWR_COLLECTION_DATE,"%m/%d/%Y")
w.all$TWR_TEST_START_DATE<-format(w.all$TWR_TEST_START_DATE,"%m/%d/%Y")
w.all$event.date<-format(w.all$event.date,"%Y%m%d")

#create event ID field
w.all$EVENT_SMAS_ID<-paste(w.all$stationID,w.all$event.date,sep="_")
#take out those that don't have station ID
w.all<-w.all %>% 
  filter(stationID!="NA")


#repro-rate columns
w.all$TWR_PCT_CTRL<-gsub(".*\\((.*)\\).*", "\\1", w.all$Reproductive.Rate..Young..U.2640..7days....Control.)
w.all$TWR_REPRODUCTIVE_RATE<- str_extract(w.all$Reproductive.Rate..Young..U.2640..7days....Control., pattern =  "[^()]+\\(")[]

#remove stray characters
w.all$TWR_REPRODUCTIVE_RATE<-str_remove(w.all$TWR_REPRODUCTIVE_RATE,"[(]")
w.all$TWR_REPRODUCTIVE_RATE<-str_remove(w.all$TWR_REPRODUCTIVE_RATE,"[****]")
#make sure there are no remaining white spaces
w.all$TWR_REPRODUCTIVE_RATE<-trimws(w.all$TWR_REPRODUCTIVE_RATE)

#do the same with the survival items
w.all$TWR_PCT_SURVIVAL<-paste(w.all$X..Survival..7.days.)
w.all$TWR_PCT_SURVIVAL<-str_remove(w.all$TWR_PCT_SURVIVAL,"[*****]") #had to hit this a couple times

w.all<-w.all %>% 
  rename("TWR_ASSESSMENT"=Assessment)

#split them apart
w.qlfy<-w.all %>% 
  filter(grepl("***",X..Survival..7.days.,fixed = TRUE))#give them the qualifier
w.qlfy$TWR_PCT_CTRL_QLFR<-"T"
w.qlfy$TWR_REPRODUCTIVE_RATE_QLFR<-"F"


w.all<-w.all %>% 
  filter(!grepl("***",X..Survival..7.days.,fixed=TRUE)) %>% 
  mutate(TWR_REPRODUCTIVE_RATE_QLFR="F")

w.all$TWR_REPRODUCTIVE_SIG<-ifelse(
  grepl("*",w.all$Reproductive.Rate..Young..U.2640..7days....Control.,fixed = TRUE),"T","F"
)
w.all$TWR_SURVIVAL_SIG<-ifelse(
  grepl("*",w.all$X..Survival..7.days.,fixed = TRUE),"T","F"
)
w.all$TWR_PCT_CTRL_QLFR<-"F"

w.all.f<-rbind(w.all,w.qlfy)#bind them together

w.all.f$TWR_PCT_SURVIVAL_QLFR<-w.all.f$TWR_REPRODUCTIVE_RATE_QLFR#make the other qualifier columkn

final.water<-w.all.f %>% 
  select(EVENT_SMAS_ID,
         TWR_COLLECTION_DATE,
         TWR_TEST_START_DATE,
         TWR_REPRODUCTIVE_RATE,
         TWR_REPRODUCTIVE_RATE_QLFR,
         TWR_REPRODUCTIVE_SIG,
         TWR_PCT_CTRL,
         TWR_PCT_CTRL_QLFR,
         TWR_PCT_SURVIVAL,
         TWR_PCT_SURVIVAL_QLFR,
         TWR_SURVIVAL_SIG,
         TWR_ASSESSMENT
  )

#write to csv
write.csv(final.water,"outputs/S_TOXICITY_WATER_RESULT.csv",row.names = FALSE)


#################################################################################################
#trying to make the new columns for data validation
sed<-read.csv("outputs/fixed_date/S_TOXICITY_SEDIMENT_RESULT.csv",stringsAsFactors = FALSE)
wat<-read.csv("outputs/fixed_date/S_TOXICITY_WATER_RESULT.csv",stringsAsFactors = FALSE)

mer.w<-w.all.f %>% 
  select(EVENT_SMAS_ID,stationID,event.date) %>% 
  rename("SMAS_SITE_HISTORY_ID"=stationID,
         "SMAS_COLLECTION_DATE"=event.date)

mer.s<-sed.all %>% 
  select(EVENT_SMAS_ID,stationID,date) %>% 
  rename("SMAS_SITE_HISTORY_ID"=stationID,
         "SMAS_COLLECTION_DATE"=date)

wat.f<-merge(wat,mer.w,by="EVENT_SMAS_ID",all.x = TRUE)

sed.f<-merge(sed,mer.s,by="EVENT_SMAS_ID",all.x = TRUE)

write.csv(sed.f,"outputs/S_TOXICITY_SEDIMENT_RESULT_adjusted.csv",row.names = FALSE)
write.csv(wat.f,"outputs/S_TOXICITY_WATER_RESULT_adjusted.csv",row.names=FALSE)
