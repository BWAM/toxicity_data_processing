
# grab data from the database ---------------------------------------------

#set up the user

sediment<-read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Toxicity_ITS/MASTER_S_TOXICITY_SEDIMENT_RESULT.csv")
sed.sites<-sediment %>% 
  select(EVENT_SMAS_ID,TSR_EVENT_SMAS_SAMPLE_DATE) %>% 
  dplyr::rename(site_id=EVENT_SMAS_ID,
                date=TSR_EVENT_SMAS_SAMPLE_DATE) %>% 
  distinct()

water<-read.csv("C:/Users/kareynol/New York State Office of Information Technology Services/SMAS - Streams Data Modernization/Cleaned Files/Final_Toxicity_ITS/MASTER_S_TOXICITY_WATER_RESULT.csv")
water_sites<-water %>% 
  select(EVENT_SMAS_ID,TWR_EVENT_SMAS_SAMPLE_DATE) %>% 
  dplyr::rename(site_id=EVENT_SMAS_ID,
                date=TWR_EVENT_SMAS_SAMPLE_DATE) %>% 
  distinct()
sites.l<-rbind(sed.sites,water_sites)

#grab data-from the data base; we are going to grab the sites table, event table and the macroinvertebrate data

# read in sites and initialize sites list to include all sites 
#read in data that have the "master" tag
db_path<-paste("C:/Users/",params$user,"/New York State Office of Information Technology Services/SMAS - Streams Data Modernization",sep = "")

sites_path <- file.path(
  db_path,
  "Cleaned Files",
  "Final_Sites_ITS"
)
# Get the file paths for the filenames with the prefix "MASTER" and
# extension CSV.
sites_csv_list <- list.files(
  path = sites_path,
  pattern = "Master(.+?)csv",
  full.names = TRUE
)
# Identify the appropriate name for each file path.
sites_csv_names <- dplyr::case_when(
  grepl("Master_S_Site", sites_csv_list) ~ "sites",
  TRUE ~ "ERROR"
)
# Assign easy to reference names to filepaths.
names(sites_csv_list) <- sites_csv_names
# Reading in macro data -------------------------------------------------
## Loop through CSV list, import data, store in a list.
sites_raw_list <- lapply(sites_csv_list, function(file_i) {
  # Import data
  read.csv(
    file_i,
    na.strings = c("", "NA"),
    stringsAsFactors = FALSE,
    fileEncoding = "UTF-8-BOM"
  )})

#subset the site table
site.ex.l<-unique(sites.l$site_id)
#read in the sites file

sites.short<-sites_raw_list$sites %>% 
  subset(SITE_HISTORY_ID %in% site.ex.l) #subset based on the list


# Grab chemistry and pcode data (this will likely be used later--------

chem_path <- file.path(
  db_path,
  "Cleaned Files",
  "Final_Chemistry_ITS"
)
# Get the file paths for the filenames with the prefix "MASTER" and
# extension CSV.
chem_csv_list <- list.files(
  path = chem_path,
  pattern = "MASTER(.+?)csv",
  full.names = TRUE
)
# Identify the appropriate name for each file path.
chem_csv_names <- case_when(
  grepl("RESULT", chem_csv_list) ~ "result",
  grepl("SAMPLE", chem_csv_list) ~ "sample",
  grepl("PARAMETER", chem_csv_list) ~ "pcode",
  
  TRUE ~ "ERROR"
)
# Assign easy to reference names to filepaths.
names(chem_csv_list) <- chem_csv_names
# Reading in macro data -------------------------------------------------
## Loop through CSV list, import data, store in a list.
chem_raw_list <- lapply(chem_csv_list, function(file_i) {
  # Import data
  read.csv(
    file_i,
    na.strings = c("", "NA"),
    stringsAsFactors = FALSE
  )})
# Join chem Data ----------------------------------------------------------

chem.all<-merge(chem_raw_list$result,chem_raw_list$sample,
                by.x=c("CHR_SYS_SAMPLE_CDE","CHR_SAMPLE_DEL_GRP"),
                by.y=c("CHS_SYS_SAMPLE_CDE","CHS_SAMPLE_DEL_GRP"))

chem.all<-chem.all %>% 
  subset(gdata::startsWith(CHS_DEC_SAMPLE_TYPE_CDE, "N")) %>%
  subset(CHS_SAMPLE_SOURCE=="Field") %>% 
  subset(CHR_RESULT_TYPE_CDE %in% "TRG")

#change both to numeric
chem_raw_list$pcode$pcode.num<-as.numeric(chem_raw_list$pcode$CHEM_PARAMETER_PCODE)


#merge pcode and chemistry
chem<-merge(chem.all,chem_raw_list$pcode,by.x="CHR_PCODE",by.y="pcode.num",all.x = TRUE) %>% 
  #filter out lab pH, lab temperature, and lab specific conductance
  filter(!(CHR_PCODE %in% c(110, 136, 139, 143, 145)))

#clean up\
rm(chem.all)
#Get the field data, which includes the sample event table
field_path <- file.path(
  db_path,
  "Cleaned Files",
  "Final_SBU_Field_ITS"
)
# Get the file paths for the filenames with the prefix "MASTER" and
# extension CSV.
field_csv_list <- list.files(
  path = field_path,
  pattern = "(.+?)csv",
  full.names = TRUE
)
# Identify the appropriate name for each file path.
field_csv_names <- case_when(
  grepl("User_Perception", field_csv_list) ~ "userp",
  grepl("Habitat", field_csv_list) ~ "habitat",
  grepl("IN_SITU", field_csv_list) ~ "insitu",
  grepl("Sample_Event", field_csv_list) ~ "sample_info",
  TRUE ~ "ERROR"
)
# Assign easy to reference names to filepaths.
names(field_csv_list) <- field_csv_names
# Reading in macro data -------------------------------------------------
## Loop through CSV list, import data, store in a list.
field_raw_list <- lapply(field_csv_list, function(file_i) {
  # Import data
  read.csv(
    file_i,
    na.strings = c("", "NA"),
    stringsAsFactors = FALSE,
    fileEncoding = "UTF-8-BOM")})

#merge insitu and pcode
field_raw_list$insitu$pcode.num<-as.numeric(field_raw_list$insitu$ISWC_CHEM_PARAMETER_PCODE_VALID)

#merge pcode and insitu
field_raw_list$insitu<-merge(field_raw_list$insitu,chem_raw_list$pcode,by="pcode.num",all.x = TRUE)

#Grab bug data
#read in data that have the "master" tag
db_path<-paste("C:/Users/",params$user,"/New York State Office of Information Technology Services/SMAS - Streams Data Modernization",sep = "")

macro_path <- file.path(
  db_path,
  "Cleaned Files",
  "Final_Macro_ITS"
)
# Get the file paths for the filenames with the prefix "MASTER" and
# extension CSV.
macro_csv_list <- list.files(
  path = macro_path,
  pattern = "MASTER(.+?)csv",
  full.names = TRUE
)
# Identify the appropriate name for each file path.
macro_csv_names <- case_when(
  grepl("METRICS", macro_csv_list) ~ "metrics",
  grepl("SPECIES_SAMP_INF", macro_csv_list) ~ "bug_method",
  grepl("SPECIES_DATA_HISTORY",macro_csv_list)~"raw_bugs",
  TRUE ~ "ERROR"
)
# Assign easy to reference names to filepaths.
names(macro_csv_list) <- macro_csv_names
# Reading in macro data -------------------------------------------------
## Loop through CSV list, import data, store in a list.
macro_raw_list <- lapply(macro_csv_list, function(file_i) {
  # Import data
  read.csv(
    file_i,
    na.strings = c("", "NA"),
    stringsAsFactors = FALSE
  )})

# Join Macro Data ----------------------------------------------------------
metrics <- merge(
  x = macro_raw_list$metrics,
  y = macro_raw_list$bug_method,
  by.x = "MMDH_LINKED_ID_VALIDATOR",
  by.y="MSSIH_LINKED_ID_VALIDATOR"
)

bugs_raw<-merge(
  x=macro_raw_list$raw_bugs,
  y=macro_raw_list$bug_method,
  by.x = "MSDH_LINKED_ID_VALIDATOR",
  by.y="MSSIH_LINKED_ID_VALIDATOR")

# update date formats -----------------------------------------------------

#change date on the insitu data and bugs raw
field_raw_list$insitu$ISWC_EVENT_SMAS_SAMPLE_DATE<-as.Date(field_raw_list$insitu$ISWC_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
metrics$MSSIH_EVENT_SMAS_SAMPLE_DATE<-as.Date(metrics$MSSIH_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
chem$CHS_EVENT_SMAS_SAMPLE_DATE<-as.Date(chem$CHS_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
chem$CHS_SAMPLE_DATETIME<-as.POSIXct(chem$CHS_SAMPLE_DATETIME, format="%m/%d/%Y %H:%M")
field_raw_list$sample_info$SEIH_EVENT_SMAS_SAMPLE_DATE<-as.Date(field_raw_list$sample_info$SEIH_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
field_raw_list$userp$UPFDH_EVENT_SMAS_SAMPLE_DATE<-as.Date(field_raw_list$userp$UPFDH_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
macro_raw_list$bug_method$MSSIH_EVENT_SMAS_SAMPLE_DATE<-as.Date(macro_raw_list$bug_method$MSSIH_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
field_raw_list$habitat$HFDH_EVENT_SMAS_SAMPLE_DATE<-as.Date(field_raw_list$habitat$HFDH_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
library(lubridate)
field_raw_list$insitu$year<-as.character(year(field_raw_list$insitu$ISWC_EVENT_SMAS_SAMPLE_DATE))


# subset the data to the sites list ---------------------------------------

sites_dates<-sites.l
sites.l<-unique(sites.l$site_id)

#read in the ribs site id's
ribs_sites<-read.csv(here::here("data/ribs_sites.csv"))


#get the sites first and then merge with the raw bugs and metrics by the validator.
macro.method<-macro_raw_list$bug_method %>% 
  subset(MSSIH_EVENT_SMAS_HISTORY_ID %in% sites.l)

#now get the linked id validator for the bug stuff
bug.sites<-unique(macro.method$MSSIH_LINKED_ID_VALIDATOR)

metrics.df<-metrics %>% 
  subset(MSSIH_EVENT_SMAS_HISTORY_ID %in% sites.l)
metrics.final<-merge(metrics.df,sites.short,
                     by.x="MSSIH_EVENT_SMAS_HISTORY_ID",
                     by.y="SITE_HISTORY_ID") %>% 
  dplyr::rename(latitude="SITE_LONGITUDE",
                longitude="SITE_LATITUDE")


bugr.df<-bugs_raw %>% 
  subset(MSDH_LINKED_ID_VALIDATOR %in% bug.sites)

chem.df<-chem %>% 
  subset(CHS_EVENT_SMAS_HISTORY_ID %in% sites.l) %>% 
  filter(CHR_VALIDATOR_QUAL!="R")

habitat.df<-field_raw_list$habitat %>% 
  subset(HFDH_EVENT_SMAS_HISTORY_ID %in% sites.l)

insitu.df<-field_raw_list$insitu %>% 
  subset(ISWC_EVENT_SMAS_HISTORY_ID %in% sites.l)

userp.df<-field_raw_list$userp %>% 
  subset(UPFDH_EVENT_SMAS_HISTORY_ID %in% sites.l)

sample_info.df<-field_raw_list$sample_info %>% 
  subset(SEIH_EVENT_SMAS_HISTORY_ID %in% sites.l)


# subset to the dates we want, and then do some magic ---------------------

sites_dates$date<-as.Date(sites_dates$date,"%m/%d/%Y")

#create lookup field that concatenates the two

sites_dates$match<-paste(sites_dates$site_id,sites_dates$date,sep = "_")

#function for creating the match id
create_match<-function(df,x,y){
  df$match<-paste0(df$x,df$y,sep = "_")
}
#this still isn't working!!

#really just need the bugs for now,and chemistry
# create_match(chem.df,CHS_EVENT_SMAS_HISTORY_ID,CHS_EVENT_SMAS_SAMPLE_DATE)
# create_match(metrics.final,MSSIH_EVENT_SMAS_HISTORY_ID,MSSIH_EVENT_SMAS_SAMPLE_DATE)

#fine, i'll do it manually. 
chem.df$match<-paste(chem.df$CHS_EVENT_SMAS_HISTORY_ID,
                     chem.df$CHS_EVENT_SMAS_SAMPLE_DATE,
                     sep = "_")
metrics.final$match<-paste(metrics.final$MSSIH_EVENT_SMAS_HISTORY_ID,
                           metrics.final$MSSIH_EVENT_SMAS_SAMPLE_DATE,
                           sep = "_")

water$TWR_EVENT_SMAS_SAMPLE_DATE<-as.Date(water$TWR_EVENT_SMAS_SAMPLE_DATE,"%m/%d/%Y")
water$match<-paste(water$EVENT_SMAS_ID,
                   water$TWR_EVENT_SMAS_SAMPLE_DATE,
                   sep = "_")

chem.df<-chem.df %>% 
  mutate(CHR_RESULT_VALUE=case_when(
    CHR_VALIDATOR_QUAL=="U"~0.5 * as.numeric(CHR_METHOD_DETECT_LIMIT),
    TRUE~CHR_RESULT_VALUE
  ))

#need to make chem.df wide instead of long
chem.df2<-chem.df %>% 
  select(match,CHEM_PARAMETER_NAME,
         CHEM_PARAMETER_UNIT_NOSP,
         CHR_RESULT_VALUE, 
         CHEM_PARAMETER_FRACTION) %>% 
  mutate(chem_unit=paste(CHEM_PARAMETER_NAME,
                         CHEM_PARAMETER_FRACTION,
                         CHEM_PARAMETER_UNIT_NOSP,
                         sep ="_")) %>% 
  select(-c(CHEM_PARAMETER_NAME,
            CHEM_PARAMETER_UNIT_NOSP)) %>% 
  group_by(match,chem_unit) %>% 
  summarize(median=median(CHR_RESULT_VALUE,na.rm = TRUE)) #and create median values



  chem.df2<-chem.df2 %>% 
    tidyr::pivot_wider(names_from=chem_unit,values_from=median)


water_chem<-merge(water,chem.df2,
                  by="match")


water_chem_short_all<-water_chem %>%
  mutate(year=format(TWR_EVENT_SMAS_SAMPLE_DATE,"%Y"))

write.csv(water_chem_short_all,here::here("outputs/water_chem_short_all.csv"))




#  Filter the bugs --------------------------------------------------------


#maybe change this to by year instead of date?
metrics_sum<-metrics.final %>% 
  mutate(year=format(MSSIH_EVENT_SMAS_SAMPLE_DATE,"%Y")) %>% 
  group_by(MSSIH_EVENT_SMAS_HISTORY_ID,year) %>% 
  summarise_at(-c(1,2,20:54),
               mean,na.rm=TRUE)


metrics_water<-merge(water,metrics_sum,
                     by.x ="EVENT_SMAS_ID",
                     by.y="MSSIH_EVENT_SMAS_HISTORY_ID")

write.csv(metrics_water,here::here("outputs/metrics_water.csv"))

#clean up the environment
rm(list=ls())
