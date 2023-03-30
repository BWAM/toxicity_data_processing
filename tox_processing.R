#get tox data ready for db
#5/3/2021
#Keleigh Reynolds

#read in the 2020 file

#raw<-readxl::read_excel("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/to_be_cleaned/2021_data/SMAS Reporting_TTU Data_2021 Allegheny_SenecaOswego_UpperHudson (SCR)_Ramapo (SS).xlsx",sheet="all")

library(readxl)
path <- here::here("data/2022_water.xlsx")
sheetnames <- readxl::excel_sheets(path)
mylist <- lapply(readxl::excel_sheets(path), readxl::read_excel, path = path)

# name the dataframes
names(mylist) <- sheetnames


#first combine the two that are microtox (ras and screening)
#2021 there is just routine

#microtox<-rbind(mylist$`Microtox_Data Table`,mylist$`Ramapo RAS`)
microtox<-mylist$`Microtox_Data Table`

#split the dates out
library(tidyr)
sed.all<-microtox

sed.all<-tidyr::separate(sed.all,Sediment.Collection.Test.Date,
                  c("TSR_COLLECTION_DATE","TSR_SED_TEST_DATE"),
                  sep = "/")
sed.all<-separate(sed.all,TSR_SED_TEST_DATE,c("TSR_SED_TEST_DATE","TSR_POREWATER_TEST_DATE"),
                  sep="&")
sed.all$TSR_SED_TEST_DATE<-trimws(sed.all$TSR_SED_TEST_DATE)

library(stringr)
sed.all$temp<-stringr::str_sub(sed.all$TSR_COLLECTION_DATE,-3,-1)#first gather what we needd to paste in

library(dplyr)
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
sed.all$temp2<-stringr::str_sub(sed.all$TSR_SED_TEST_DATE,1,3)

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

sed.all$Porewater.EC50....<-stringr::str_remove(sed.all$Porewater.EC50....,">")
sed.all<-sed.all %>% 
  mutate(TSR_SEDIMENT_RSLT_QLFR="",
         TSR_POREWATER_RSLT_QLFR="",
         EVENT_SMAS_ID=paste(stationID,date,sep = "_")) %>% 
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

#need additional columns for the  validation stuff
sed.final<-sed.final %>% 
  rename(TSR_EVENT_SMAS_SAMPLE_DATE=TSR_COLLECTION_DATE)

sed.final$EVENT_SMAS_ID<-substr(sed.final$EVENT_SMAS_ID,1,nchar(sed.final$EVENT_SMAS_ID)-9)

sed.final$TSR_POREWATER_RSLT<-gsub(">","",sed.final$TSR_POREWATER_RSLT)
sed.final$TSR_SEDIMENT_RSLT<-gsub(">","",sed.final$TSR_SEDIMENT_RSLT)

sed.final$TSR_POREWATER_RSLT<-as.numeric(sed.final$TSR_POREWATER_RSLT)
sed.final$TSR_SEDIMENT_RSLT<-as.numeric(sed.final$TSR_SEDIMENT_RSLT)

#round them
sed.final<-sed.final %>% 
  mutate_if(is.numeric,round,2)

write.csv(sed.final,"outputs/20230330_S_TOXICITY_SEDIMENT_RESULT_append.csv",row.names = FALSE)
##########################################################
#C dubia tables
w.all<-mylist$`C.dubia_Data Table`

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
w.all$EVENT_SMAS_ID<-paste(w.all$stationID)
#take out those that don't have station ID
w.all<-w.all %>% 
  filter(stationID!="NA")

#reproductive rate columns - this includes PCT_CTRL and REPRO_RATE
#in nikki's results, the column is combined, the [18.4 (120)] 
#the first number is the TWR_REPRODUCTIVE_RATE
#the one in parenthesis is TWR_PCT_CTRL

w.all$Reproductive.Rate..Young..U.2640..7days....Control._paste<-paste(
  w.all$Reproductive.Rate..Young..U.2640..7days....Control.
)

w.all<-w.all %>% 
  separate(Reproductive.Rate..Young..U.2640..7days....Control.,
           c("TWR_REPRODUCTIVE_RATE", "TWR_PCT_CTRL"),
           " ")


# #remove stray characters
w.all$TWR_PCT_CTRL<-stringr::str_remove(w.all$TWR_PCT_CTRL,"[(]")
w.all$TWR_PCT_CTRL<-stringr::str_remove(w.all$TWR_PCT_CTRL,"[)]")
w.all$TWR_REPRODUCTIVE_RATE<-stringr::str_remove(w.all$TWR_REPRODUCTIVE_RATE,"[****]")
#make sure there are no remaining white spaces
w.all$TWR_REPRODUCTIVE_RATE<-trimws(w.all$TWR_REPRODUCTIVE_RATE)

#do the same with the survival items
w.all$TWR_PCT_SURVIVAL<-paste(w.all$X..Survival..7.days.)
w.all$TWR_PCT_SURVIVAL<-stringr::str_remove(w.all$TWR_PCT_SURVIVAL,"[*****]") #had to hit this a couple times
w.all$TWR_PCT_SURVIVAL<-as.numeric(w.all$TWR_PCT_SURVIVAL)

w.all<-w.all %>% 
  rename("TWR_ASSESSMENT"=Assessment)

#split them apart
w.qlfy<-w.all %>% 
  filter(grepl("***",X..Survival..7.days.,fixed = TRUE))#give them the qualifier
w.qlfy$TWR_PCT_CTRL_QLFR<-"TRUE"
w.qlfy$TWR_REPRODUCTIVE_RATE_QLFR<-"FALSE"


w.all<-w.all %>% 
  filter(!grepl("**",X..Survival..7.days.,fixed=TRUE)) %>% 
  mutate(TWR_REPRODUCTIVE_RATE_QLFR="FALSE")

w.all$TWR_REPRODUCTIVE_SIG<-ifelse(
  grepl("**",w.all$Reproductive.Rate..Young..U.2640..7days....Control._paste,fixed = TRUE),"TRUE","FALSE"
)
w.all$TWR_SURVIVAL_SIG<-ifelse(
  grepl("**",w.all$X..Survival..7.days.,fixed = TRUE),"TRUE","FALSE"
)
w.all$TWR_PCT_CTRL_QLFR<-"FALSE"

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

final.water<-final.water %>% 
  rename(TWR_EVENT_SMAS_SAMPLE_DATE=TWR_COLLECTION_DATE)

#remove any stray *
final.water$TWR_REPRODUCTIVE_RATE<-stringr::str_remove(final.water$TWR_REPRODUCTIVE_RATE,"[****]")
final.water$TWR_PCT_CTRL<-stringr::str_remove(final.water$TWR_PCT_CTRL,"[**]")

#write to csv
write.csv(final.water,"outputs/20230330_S_TOXICITY_WATER_RESULT_append.csv",row.names = FALSE)

#write the final tables
#read in the old dta
sediment.old<-read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Toxicity_ITS/MASTER_S_TOXICITY_SEDIMENT_RESULT.csv")
# sediment.old<-sediment.old %>% 
#   rename(EVENT_SMAS_ID=TSR_EVENT_SMAS_HISTORY_ID)
water.old <- read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Toxicity_ITS/MASTER_S_TOXICITY_WATER_RESULT.csv")
# water.old<-water.old %>% 
#   rename(EVENT_SMAS_ID=TWR_EVENT_SMAS_HISTORY_ID)

#bind to the 2020 data
master.sed<-rbind(sediment.old,sed.final)
master.water<-rbind(water.old,final.water)

#write to csv
write.csv(master.sed,"outputs/2023_03_30_MASTER_S_TOXICITY_SEDIMENT_RESULT.csv",row.names = FALSE)
write.csv(master.water,"outputs/2023_03_30_MASTER_S_TOXICITY_WATER_RESULT.csv",row.names = FALSE)

          