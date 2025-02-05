#------------------------------------------------------------------------------------------------------------------------------------
#Luca 26/10/2023
#------------------------------------------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------------------------------------------------------
#HOUSHOLDING
#------------------------------------------------------------------------------------------------------------------------------------

#Clean out all user defined data
rm(list = ls())

#------------------------------------------------------------------------------------------------------------------------------------
#PACKAGES
#------------------------------------------------------------------------------------------------------------------------------------


library('Hmisc')
library('dplyr')
library('stringr')
library('sf')
library('tidyverse')
library('data.table')
library('DescTools')
library('parallel')

setwd("~/bulk/hynsjo_shared")

year <- 1940


cleaning_input_revised <- list.files(paste0("revised/clean_census_input/files_", year))
cleaning_input_revised <- str_remove(cleaning_input_revised, "dta_")
cleaning_input_revised <- str_remove(cleaning_input_revised, ".csv")


load('revised/scripts/light_all_full.RData')

input_dir <- paste0("revised/clean_census_input/files_",year)
output_dir <- paste0("revised/clean_census_output/files_",year)


clean_1940_file <- function(county){
  
  county_data <- fread(paste0(input_dir,"/dta_", county , ".csv"), stringsAsFactors = FALSE, na.strings= c("NA","nan",""))
  county_data <- as.data.frame(county_data)
  
  # (1) drop unnecessary variables
  # drop q variables
  county_data <- county_data[,!grepl("^q", names(county_data))]
  
  # drop other variables
  drop <- c("rectype", "rectypep", "datanump", "slwtreg", "momrule_hist", "poprule_hist",
            "sprule_hist", "perwt", "datanum", "pageno", "perwtreg", "imageied",
            "momloc", "poploc", "stepmom", "steppop", "bplstr", "mbplstr", "fbpstr", "occstr", "mtongstr", "mcit5str", "indstr", "fbplstr",
            "relstr")
  county_data <- county_data[, !(names(county_data) %in% drop)]
  
  #Disa:
  #The is statement conditions on the value of the variable being greater than 1
  #I didn't change it, but I don't know why we need this?
  
  #It seems like for the variables where there is no explicit code for "missing" in the IPUMS website,
  #we are not doing anything? Like family size
  #Should we remove outliers?
  #We can revisit later
  
  #Notes on what I did:
  
  #Variables that were not in 1940 census and that did have NA values
  #(Any variables in 1940 but not in 1930 that did not have an explicit code for NA in IPUMS
  #I didn't do anything with)
  #marrno -> 0 is NA, 99 is missing
  #chborn -> 0 is NA, and the all values are recorded value - 1, https://usa.ipums.org/usa-action/variables/CHBORN#codes_section
  #higrade -> 0 is NA, 999 is missing
  #wkswork1 -> 0 is NA
  #wkswork2 -> 0 is NA
  #hrswork1 -> 0 is NA
  #hrswork2 -> 0 is NA
  #incwage -> 999998 is missing, 999999 is NA
  #incnonwg -> 0 is NA, 9 is missing
  #migcity5 -> NOTE: the code for missing in IPUMS clearly don't correspond to the code in the data
  #clearly 9999 is missing in data, but IPUMS says 0 and 1.
  #educ -> 0 is NA, 999 is missing
  #vetstat -> 0 is NA, 99 is missing
  #migrate5 -> 90 is missing
  #migplac5 -> 0 is NA, 999 is missing
  #migmet5 -> 0 is NA, 9999 is missing
  #migtype5 -> 0 is NA, 9 is missing
  #migsea5 -> 996, 997, 998, 999 are NA or missing
  #migfarm5 -> IPUMS doesn't tell the codes, but I assume that 9 is NA
  #durunemp -> 999 is NA
  
  #These were only asked from a subset of people in 1940:
  #classwkr - the IPUMS codes look incorrect, 98 must be that is missing
  #uclasswk
  #uocc
  #uocc95
  #uind
  
  #sameplac5  -> 0 and 9 are missing/NA
  #samemet5 -> no codes in IPUMS, I didn't do anything
  #samesea5 -> 0 is NA, 9 is missing
  #vetwwi -> 0 is NA
  #vet1940 -> 0 is NA, 8 is missing
  #vetper -> 0 is NA, 8 is missing
  #vetchild -> 0 is NA, 9 is missing
  #ssenroll -> 0 is NA
  
  #Variables that I set to missing in 1940 but were not set to missing in 1930
  #NOTE:
  #In 1940 we have variable slrec, about 95% is == 1, 5% == 2, 2 means "sample line person"
  #these are the people for whom extra variables about heritage is asked.
  #I.e., these are the people for whom we have:
  #nativity - 0 is NA
  #mtongue - 0 is NA, 96 is missing
  #mbpl - 0 is NA, >= 9500 are missing
  #fbpl - 0 is NA, >= 9500 are missing
  
  
  
  # (2) replace missing values to NA
  if (sum(county_data$citypop == 99999 |county_data$citypop == 0 ,na.rm=T)>0){
    county_data[county_data$citypop == 99999 |county_data$citypop == 0 , ]$citypop <- NA}
  if (sum(county_data$eldch == 99,na.rm=T)>0){
    county_data[county_data$eldch == 99, ]$eldch <- NA}
  if (sum(county_data$yngch == 99,na.rm=T)>0){
    county_data[county_data$yngch == 99, ]$yngch <- NA}
  if (sum(county_data$marrno == 0,na.rm=T)>0 | sum(county_data$marrno == 9,na.rm=T)>0 ){
    county_data[county_data$marrno == 0 | county_data$marrno == 9, ]$marrno <- NA}
  #Note: chborn has value 99, this should clearly be missing, but is not indicated at IPUMS
  if (sum(county_data$chborn == 0,na.rm=T)>0 | sum(county_data$chborn == 99,na.rm=T)>0 ){
    county_data[county_data$chborn == 0 | county_data$chborn == 99, ]$chborn <- NA}
  #We have missing values in this variable for 1940 because only a subset of the population was asked about this
  if (sum(county_data$nativity == 0,na.rm=T)>0){
    county_data[county_data$nativity == 0, ]$nativity <- NA}
  if (sum(county_data$citizen == 0 | county_data$citizen == 5, na.rm=T)>0){
    county_data[county_data$citizen == 0 | county_data$citizen == 5, ]$citizen <- NA}
  #Not included in 1940:
  #if (sum(county_data$yrsusa2 == 0,na.rm=T)>1){
  #  county_data[county_data$yrsusa2 == 0 | county_data$yrsusa2 == 9, ]$yrsusa2 <- NA}
  #mtongue was only asked for a subset of people in 1940
  if (sum(county_data$mtongue == 0 | county_data$mtongue >= 9500,na.rm=T)>0){
    county_data[county_data$mtongue == 0 | county_data$mtongue >= 9500, ]$mtongue <- NA}
  if (sum(county_data$higrade == 0 | county_data$higrade == 999,na.rm=T)>0){
    county_data[county_data$higrade == 0 | county_data$higrade == 999, ]$higrade <- NA}
  #Not in 1940
  #if (sum(county_data$language == 0,na.rm=T)>1){
  #  county_data[county_data$language == 0, ]$language <- NA}
  #if (sum(county_data$speakeng == 0,na.rm=T)>1){
  #  county_data[county_data$speakeng == 0,]$speakeng <- NA}
  #if (sum(county_data$lit == 0,na.rm=T)>1){
  #  county_data[county_data$lit == 0,]$lit <- NA}
  if (sum(county_data$labforce == 0,na.rm=T)>0){
    county_data[county_data$labforce == 0, ]$labforce <- NA}
  #Disa March 2021
  #Adding NA for == 979 -> "Not yet classified"
  #Mostly men in this category. 
  #Or actually I reverse this, I think that "not yet classified" is a subset of "Laborers"
  #see https://usa.ipums.org/usa-action/variables/OCC1950#codes_section
  if (sum(county_data$occ1950 >= 995,na.rm=T)>0){
    county_data[county_data$occ1950 >= 995, ]$occ1950 <- NA}
  #Disa March 2021
  #Previously we were setting the top code 80 to missing here. I don't know if that's what we want to do
  #so I deleted this.
  if (sum(county_data$occscore == 0,na.rm=T)>0){
    county_data[county_data$occscore == 0,]$occscore <- NA}
  if (sum(county_data$sei == 0,na.rm=T)>0){
    county_data[county_data$sei == 0, ]$sei <- NA}
  
  #Disa:
  #instead put all above 976 to missing
  #Alternatively, we may want to measure the number of housewives using
  #ind1950 == 982
  if (sum(  (county_data$ind1950 > 976 & county_data$ind1950 <= 999)  | county_data$ind1950 == 0, na.rm=T) ){
    county_data[(county_data$ind1950 > 976 & county_data$ind1950 <= 999) | county_data$ind1950 == 0 , ]$ind1950 <- NA}
  
  #98 is not indicated at IPUMS. But it's in the data, I assume it is missing
  if (sum(county_data$classwkr == 0 | county_data$classwkr == 98,na.rm=T)>0){
    county_data[county_data$classwkr == 0 | county_data$classwkr == 98,]$classwkr <- NA}
  
  #Asked for only 1% of people
  if (sum(county_data$wkswork1 == 0,na.rm=T)>0){
    county_data[county_data$wkswork1 == 0,]$wkswork1 <- NA}
  if (sum(county_data$wkswork2 == 0,na.rm=T)>0){
    county_data[county_data$wkswork2 == 0,]$wkswork2 <- NA}
  #It says that hours was asked for everyone who worked in the last year
  if (sum(county_data$hrswork1 == 0,na.rm=T)>0){
    county_data[county_data$hrswork1 == 0,]$hrswork1 <- NA}
  if (sum(county_data$hrswork2 == 0,na.rm=T)>0){
    county_data[county_data$hrswork2 == 0,]$hrswork2 <- NA}
  
  #Note that when including 0s the median is 0. Probably because incwage is not reported for self employed
  #For Aiken, SC the max is 12450, even though IPUMS says it is topcoded at 5,001.
  county_data$incwage_NAtopcodes <- county_data$incwage
  if (sum(county_data$incwage > 4999 , na.rm=T)>0){
    county_data[county_data$incwage > 4999, ]$incwage_NAtopcodes <- NA}
  
  if (sum(county_data$incwage == 999998 | county_data$incwage == 999999, na.rm=T)>0){
    county_data[county_data$incwage == 999998 | county_data$incwage == 999999, ]$incwage <- NA}
  

  if (sum(county_data$incnonwg == 0 | county_data$incnonwg == 9,na.rm=T)>1){
    county_data[county_data$incnonwg == 0 | county_data$incnonwg == 9,]$incnonwg <- NA}
  
  if (sum(county_data$presgl == 0,na.rm=T)>0){
    county_data[county_data$presgl == 0, ]$presgl <- NA}
  if (sum(county_data$erscor50 >= 999 ,na.rm=T)>0){
    county_data[county_data$erscor50 >= 999 ,]$erscor50 <- NA}
  if (sum(county_data$edscor50>=999,na.rm=T)>0){
    county_data[county_data$edscor50 >=999 ,]$edscor50 <- NA}
  if (sum(county_data$npboss50 >= 999,na.rm=T)>0){
    county_data[county_data$npboss50 >= 999, ]$npboss50 <- NA}
  
  if (sum(county_data$vetstat == 0 | county_data$vetstat == 99,na.rm=T)>0){
    county_data[county_data$vetstat == 0 | county_data$vetstat == 99, ]$vetstat <- NA}
  
  #Disa March 2021
  #Migration variables - I am not going to clean the migration variables because it's hard to know how to clean them without
  #knowing what variables we might want to make with them.
  
  #Not in 1940 data
  #if (sum(county_data$yrimmig == 0,na.rm=T)>1){
  #  county_data[county_data$yrimmig == 0, ]$yrimmig <- NA}
  if (sum(county_data$agemarr == 0,na.rm=T)>0){
    county_data[county_data$agemarr == 0, ]$agemarr <- NA}
  
  #Asked of sample 
  if (sum(county_data$mbpl >= 90000 | county_data$mbpl == 0,na.rm=T)>0){
    county_data[county_data$mbpl >= 90000 | county_data$mbpl == 0, ]$mbpl <- NA}
  if (sum(county_data$fbpl >= 90000 | county_data$fbpl == 0,na.rm=T)>0){
    county_data[county_data$fbpl >= 90000 | county_data$fbpl == 0, ]$fbpl <- NA}
  #Asked of everyone
  if (sum(county_data$bpl >= 90000 ,na.rm=T)>0){
    county_data[county_data$bpl >= 90000 , ]$bpl <- NA}
  
  if (sum(county_data$durunemp == 999,na.rm=T)>0){
    county_data[county_data$durunemp == 999, ]$durunemp <- NA}
  
  
  if (sum(county_data$migrate5 == 90,na.rm=T)>0){
    county_data[county_data$migrate5 == 90, ]$migrate5 <- NA}
  #Additional migration variables that I ignore for now:
  #Migplac5 -> Which state lived in 5 yrs ago
  #Migmet5 -> Metropolitan area 5 yrs ago
  #Migtype5 -> Metroplitan status 5 yrs ago
  #Migsea5 -> SEA 5 yrs ago
  #Migrfarm5 -> Farm status 5 yrs ago
  
  
  if (sum(county_data$uclasswk == 0,na.rm=T)>1 | sum(county_data$uclasswk == 7,na.rm=T)>1 | sum(county_data$uclasswk == 8,na.rm=T)>1){
    county_data[county_data$uclasswk == 0 | county_data$uclasswk == 7 | county_data$uclasswk == 8, ]$uclasswk <- NA}
  
  if (sum(county_data$uocc == 995,na.rm=T)>1){
    county_data[county_data$uocc >= 995, ]$uocc <- NA}
  if (sum(county_data$uocc95 == 995,na.rm=T)>1){
    county_data[county_data$uocc95 >= 995, ]$uocc95 <- NA}
  if (sum(county_data$uind == 995,na.rm=T)>1){
    county_data[county_data$uind >= 995, ]$uind <- NA}
  if (sum(county_data$sameplac == 0,na.rm=T)>1 | sum(county_data$sameplac == 9,na.rm=T)>1){
    county_data[county_data$sameplac == 0 | county_data$sameplac == 9, ]$sameplac <- NA}
  if (sum(county_data$samesea5 == 0,na.rm=T)>1 | sum(county_data$samesea5 == 9,na.rm=T)>1){
    county_data[county_data$samesea5 == 0 | county_data$samesea5 == 9, ]$samesea5 <- NA}
  #This looks wrong
  #if (sum(county_data$agemonth == 0,na.rm=T)>1){
  #  county_data[county_data$agemonth == 0, ]$agemonth <- NA}
  if (sum(county_data$agemonth == 98,na.rm=T)>1 | sum(county_data$agemonth == 99,na.rm=T)>1){
    county_data[county_data$agemonth == 98 | county_data$agemonth == 99, ]$agemonth <- NA}
  if (sum(county_data$vetwwi == 0,na.rm=T)>1){
    county_data[county_data$vetwwi == 0, ]$vetwwi <- NA}
  if (sum(county_data$vet1940 == 0,na.rm=T)>1 | sum(county_data$vet1940 == 8,na.rm=T)>1){
    county_data[county_data$vet1940 == 0 | county_data$vet1940 == 8, ]$vet1940 <- NA}
  if (sum(county_data$vetper == 0,na.rm=T)>1 | sum(county_data$vetper == 8,na.rm=T)>1){
    county_data[county_data$vetper == 0 | county_data$vetper == 8, ]$vetper <- NA}
  if (sum(county_data$vetchild == 0,na.rm=T)>1 | sum(county_data$vetchild == 9,na.rm=T)>1){
    county_data[county_data$vetchild == 0 | county_data$vetchild == 9, ]$vetchild <- NA}
  if (sum(county_data$ssenroll == 0,na.rm=T)>1){
    county_data[county_data$ssenroll == 0, ]$ssenroll <- NA}
  
  if (sum(county_data$migcounty == 9999,na.rm=T)>1){
    county_data[county_data$migcounty == 9999, ]$migcounty <- NA}
  
  #Why not dwsize included in 1930 cleaning?
  if (sum(county_data$dwsize == 0,na.rm=T)>1 | sum(county_data$dwsize == 9999,na.rm=T)>1){
    county_data[county_data$dwsize == 0 | county_data$dwsize == 9999, ]$dwsize <- NA}
  if (sum(county_data$city == 0,na.rm=T)>1 | sum(county_data$city == 9999,na.rm=T)>1){
    county_data[county_data$city == 0 | county_data$city == 9999, ]$city <- NA}
  if (sum(county_data$sizepl == 0,na.rm=T)>1){
    county_data[county_data$sizepl == 0, ]$citypop <- NA}
  #Note in 1940 data
  #if (sum(county_data$vet1930 == 0,na.rm=T)>1){
  #  county_data[county_data$vet1930 == 6, ]$vet1930 <- NA}
  if (sum(county_data$urban == 0,na.rm=T)>1){
    county_data[county_data$urban == 0, ]$urban <- NA}
  #Not in 1940 data
  #if (sum(county_data$urbarea == 0,na.rm=T)>1){
  #  county_data[county_data$urbarea == 0, ]$urbarea <- NA}
  if (sum(county_data$gqtype == 0,na.rm=T)>1){
    county_data[county_data$gqtype == 0, ]$gqtype <- NA}
  if (sum(county_data$gqfunds == 0,na.rm=T)>1 | sum(county_data$gqfunds == 99,na.rm=T)>1){
    county_data[county_data$gqfunds == 0 | county_data$gqfunds == 99, ]$gqfunds <- NA}
  if (sum(county_data$ownershp == 0,na.rm=T)>1){
    county_data[county_data$ownershp == 0, ]$ownershp <- NA}
  if (sum(county_data$hhtype == 0,na.rm=T)>1 | sum(county_data$hhtype == 9,na.rm=T)>1){
    county_data[county_data$hhtype == 0 | county_data$hhtype == 9, ]$hhtype <- NA}
  
  if (sum(county_data$valueh == 9999998,na.rm=T)>1 | sum(county_data$valueh == 9999999,na.rm=T)>1){
    county_data[county_data$valueh == 9999998 | county_data$valueh == 9999999,]$valueh <- NA}
  
  if (sum(county_data$multgen == 0,na.rm=T)>1){
    county_data[county_data$multgen == 0, ]$multgen <- NA}
  #if (sum(county_data$rent30 == 0,na.rm=T)>1){
  #  county_data[county_data$rent30 == 0 | county_data$rent30 >9995  ,]$rent30 <- NA}
  if (sum(county_data$rent == 0,na.rm=T)>1 | sum(county_data$rent >= 9998,na.rm=T)>1){
    county_data[county_data$rent == 0 | county_data$rent >=9998,]$rent <- NA}
  
  # (3) create new full population variables ----------------------------------------------------------------------
  #Disa March 2021
  #Dummy for if household head is black
  #This creates dummy:
  #1) if household head and black
  #0) if not both household head and black
  #So need to set to missing if not household head
  county_data$black_hh <- as.numeric(county_data$race == 200 & county_data$relate == 101)
  county_data[county_data$relate != 101, ]$black_hh <- NA
  
  #Following Shertzer, Twinam and Walsh, 2016 (page 231)
  #we indicate if a black person is living in the household as 'household worker' based on
  #whether relationship to head is "Employee." I also include "relative to employee" in this
  #Note that most of those coded as 'hh_employee' have household services as occ1950, but not 
  #all, some have 'laborer.' Either way let's stick with definition based on relationship to head
  county_data$hh_employee <- as.numeric(county_data$relate >= 1210 & county_data$relate <= 1219)
  #NOTE
  #I will set race to missing for people who are black and household employees. This so that 
  #only the people who live in households 'not as employees' areVie counted in our total number of
  #black people in the neighborhood variable.
  #We can also include the variable 'fraction of ppl in neighborhood that are hh employees'
  
  # Black
  county_data$black <- as.numeric(county_data$race == 200)
  if (sum(county_data$black == 1 & county_data$hh_employee == 1,na.rm=T)>0){
    county_data[county_data$black == 1 & county_data$hh_employee == 1, ]$black <- NA
  }
  
  #Make variable for black hh employee and hispanic hh employee
  
  # Hispanic
  #Disa March 2021, I do the same as above and set hispanic household employee as having hispanic missing
  county_data$hisp <- as.numeric(county_data$hispan != 0)
  if (sum(county_data$hisp == 1 & county_data$hh_employee == 1,na.rm=T)>0){
    county_data[county_data$hisp == 1 & county_data$hh_employee == 1, ]$hisp <- NA
  }
  
  # literate
  #county_data$lit <- as.numeric(county_data$lit == 4)
  
  # female
  county_data$fem <- as.numeric(county_data$sex == 2)
  
  # Yiddish
  #Disa March 2021
  #Note that only foreign-born individuals are asked for their mother tongue
  #I will therefore set yidd = 0 if the person was not foreign born
  #This is not a very good measure of 'jewish,' we should try to use the index proposed by
  #Abrimitzky, Connor and Boustan 2020
  #Right now we just have a measure of 'foreign born jewish'
  county_data$yidd <- as.numeric(county_data$mtongue == 300|county_data$mtongue == 310 | county_data$mtongue == 320)
  if (sum(county_data$bpl >= 100 & county_data$bpl <= 12092,na.rm=T)>0){
    county_data[county_data$bpl >= 100 & county_data$bpl <= 12092 & !is.na(county_data$bpl), ]$yidd <- 0
  }
  
  
  # non-English speaking household
  #county_data$no_eng <- as.numeric(county_data$language != 1)
  
  # home owner
  county_data$own <- as.numeric(county_data$ownershp == 10)
  
  # 4-categories census regions
  county_data$region_4_cat <- county_data$region%/% 10
  
  
  # (4) create new working age variables, split by gender
  
  #Disa March 2021
  #I think the idea of this is to get the economic characteristics of the population, but
  #for an appropriate group, based on age.
  #Should we also just exclude on whether the individual is in school for this? I think it
  #still makes sense to restrict on age and for example not coding person who is 16 yrs old and not working
  #as 'unemployed.' 
  #I add condition on schooling
  
  # working age women ------------ START
  wkage_f <- county_data[which(county_data$age >= 18 & county_data$age <= 65 & county_data$fem == 1 & county_data$school != 2), ]
  # employment
  wkage_f$out_lf_f <- as.numeric(wkage_f$empstat >= 30)
  wkage_f$emp_f <- as.numeric((wkage_f$empstat >= 10 & wkage_f$empstat <20))
  wkage_f$unemp_f <- as.numeric((wkage_f$empstat >= 20 & wkage_f$empstat < 30))
  
  wkage_f$selfemp_f <- as.numeric((wkage_f$classwkr >= 10 & wkage_f$classwkr <20))
  wkage_f$wageemp_f <- as.numeric(wkage_f$classwkr >= 20 & wkage_f$classwkr < 29)
  #Disa March 2021
  #I code as not self employed and not wage employed if out of the labor force or unemployed
  #It's not obvious to me that this is correct, I just think it is better than losing those areas that
  #may exist where none of the women work in the labor market 
  wkage_f[wkage_f$empstat > 20, ]$selfemp_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$wageemp_f <- 0
  
  
  # industry
  wkage_f$ag_f <- as.numeric((wkage_f$ind1950 > 0 & wkage_f$ind1950 < 200))
  wkage_f$mining_f <- as.numeric((wkage_f$ind1950 >= 200 & wkage_f$ind1950 < 246))
  wkage_f$constr_f <- as.numeric(wkage_f$ind1950 == 246)
  wkage_f$mfg_f <- as.numeric((wkage_f$ind1950 > 300 & wkage_f$ind1950 < 500))
  wkage_f$transport_f <- as.numeric((wkage_f$ind1950 > 500 & wkage_f$ind1950 < 600))
  wkage_f$wholesale_f <- as.numeric((wkage_f$ind1950 > 600 & wkage_f$ind1950 < 700))
  wkage_f$finance_f <- as.numeric((wkage_f$ind1950 > 700 & wkage_f$ind1950 < 800))
  wkage_f$bus_f <- as.numeric((wkage_f$ind1950 > 800 & wkage_f$ind1950 < 820))
  wkage_f$personal_f <- as.numeric((wkage_f$ind1950 > 820 & wkage_f$ind1950 < 850))
  wkage_f$ent_f <- as.numeric((wkage_f$ind1950 > 850 & wkage_f$ind1950 < 860))
  wkage_f$profserv_f <- as.numeric((wkage_f$ind1950 > 860 & wkage_f$ind1950 < 900))
  wkage_f$public_f <- as.numeric((wkage_f$ind1950 > 900 & wkage_f$ind1950 < 990))
  
  #Disa March 2021
  #Industry is missing if the individual isn't working, so I think each industry
  #should be set to 0 if the person is not working.
  #NOTE: this sets to 0 if the person is not in the labor force. So we can make a consistent
  #variable for 1920. So then what we are measuring is 'fraction of adult female population in agriculture', for example
  wkage_f[wkage_f$empstat > 20, ]$ag_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$mining_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$constr_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$mfg_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$transport_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$wholesale_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$finance_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$bus_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$personal_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$ent_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$profserv_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$public_f <- 0
  
  
  # occupation
  wkage_f$proftech_f <- as.numeric(wkage_f$occ1950 < 100)
  wkage_f$farmer_f <- as.numeric((wkage_f$occ1950 >= 100 & wkage_f$occ1950 < 200))
  wkage_f$mgrs_f <- as.numeric((wkage_f$occ1950 >= 200 & wkage_f$occ1950 < 300))
  wkage_f$clerical_f <- as.numeric((wkage_f$occ1950 >= 300 & wkage_f$occ1950 < 400))
  wkage_f$sales_f <- as.numeric((wkage_f$occ1950 >= 400 & wkage_f$occ1950 < 500))
  wkage_f$craft_f <- as.numeric((wkage_f$occ1950 >= 500 & wkage_f$occ1950 < 600))
  wkage_f$oper_f <- as.numeric((wkage_f$occ1950 >= 600 & wkage_f$occ1950 < 700))
  wkage_f$service_f <- as.numeric((wkage_f$occ1950 >= 700 & wkage_f$occ1950 < 800))
  wkage_f$farmlabor_f <- as.numeric((wkage_f$occ1950 >= 800 & wkage_f$occ1950 < 900))
  wkage_f$laborer_f <- as.numeric((wkage_f$occ1950 >= 900 & wkage_f$occ1950 < 970))
  
  #Disa March 2021
  #Same comment as above
  wkage_f[wkage_f$empstat > 20, ]$proftech_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$farmer_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$mgrs_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$clerical_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$sales_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$craft_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$oper_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$service_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$farmlabor_f <- 0
  wkage_f[wkage_f$empstat > 20, ]$laborer_f <- 0
  # working age women ------------ END
  
  # working age men ------------ START
  #Disa March 2021
  #Adding conditional on schooling
  wkage_m <- county_data[which(county_data$age >= 18 & county_data$age <= 65 & county_data$fem == 0 & county_data$school != 2), ]
  # employment
  wkage_m$out_lf_m <- as.numeric(wkage_m$empstat >= 30)
  wkage_m$emp_m <- as.numeric((wkage_m$empstat >= 10 & wkage_m$empstat <20))
  wkage_m$unemp_m <- as.numeric((wkage_m$empstat >= 20 & wkage_m$empstat < 30))
  
  wkage_m$selfemp_m <- as.numeric((wkage_m$classwkr >= 10 & wkage_m$classwkr <20))
  wkage_m$wageemp_m <- as.numeric(wkage_m$classwkr >= 20)
  #Disa March 2021
  #This matters less here because most men work, but for consistency, I think we should do the same
  #as for women
  wkage_m[wkage_m$empstat > 20, ]$selfemp_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$wageemp_m <- 0
  
  # industry
  wkage_m$ag_m <- as.numeric((wkage_m$ind1950 > 0 & wkage_m$ind1950 < 200))
  wkage_m$mining_m <- as.numeric((wkage_m$ind1950 >= 200 & wkage_m$ind1950 < 246))
  wkage_m$constr_m <- as.numeric(wkage_m$ind1950 == 246)
  wkage_m$mfg_m <- as.numeric((wkage_m$ind1950 > 300 & wkage_m$ind1950 < 500))
  wkage_m$transport_m <- as.numeric((wkage_m$ind1950 > 500 & wkage_m$ind1950 < 600))
  wkage_m$wholesale_m <- as.numeric((wkage_m$ind1950 > 600 & wkage_m$ind1950 < 700))
  wkage_m$finance_m <- as.numeric((wkage_m$ind1950 > 700 & wkage_m$ind1950 < 800))
  wkage_m$bus_m <- as.numeric((wkage_m$ind1950 > 800 & wkage_m$ind1950 < 820))
  wkage_m$personal_m <- as.numeric((wkage_m$ind1950 > 820 & wkage_m$ind1950 < 850))
  wkage_m$ent_m <- as.numeric((wkage_m$ind1950 > 850 & wkage_m$ind1950 < 860))
  wkage_m$profserv_m <- as.numeric((wkage_m$ind1950 > 860 & wkage_m$ind1950 < 900))
  wkage_m$public_m <- as.numeric((wkage_m$ind1950 > 900 & wkage_m$ind1950 < 990))
  
  #Disa March 2021
  #Same comment as for women 
  wkage_m[wkage_m$empstat > 20, ]$ag_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$mining_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$constr_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$mfg_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$transport_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$wholesale_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$finance_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$bus_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$personal_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$ent_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$profserv_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$public_m <- 0
  
  # occupation
  wkage_m$proftech_m <- as.numeric(wkage_m$occ1950 < 100)
  wkage_m$farmer_m <- as.numeric((wkage_m$occ1950 >= 100 & wkage_m$occ1950 < 200))
  wkage_m$mgrs_m <- as.numeric((wkage_m$occ1950 >= 200 & wkage_m$occ1950 < 300))
  wkage_m$clerical_m <- as.numeric((wkage_m$occ1950 >= 300 & wkage_m$occ1950 < 400))
  wkage_m$sales_m <- as.numeric((wkage_m$occ1950 >= 400 & wkage_m$occ1950 < 500))
  wkage_m$craft_m <- as.numeric((wkage_m$occ1950 >= 500 & wkage_m$occ1950 < 600))
  wkage_m$oper_m <- as.numeric((wkage_m$occ1950 >= 600 & wkage_m$occ1950 < 700))
  wkage_m$service_m <- as.numeric((wkage_m$occ1950 >= 700 & wkage_m$occ1950 < 800))
  wkage_m$farmlabor_m <- as.numeric((wkage_m$occ1950 >= 800 & wkage_m$occ1950 < 900))
  wkage_m$laborer_m <- as.numeric((wkage_m$occ1950 >= 900 & wkage_m$occ1950 < 970))
  
  #Disa March 2021
  #Same comment as above
  wkage_m[wkage_m$empstat > 20, ]$proftech_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$farmer_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$mgrs_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$clerical_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$sales_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$craft_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$oper_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$service_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$farmlabor_m <- 0
  wkage_m[wkage_m$empstat > 20, ]$laborer_m <- 0
  # working age men ------------ END
  
  # recombine subsets
  county_data <- merge(county_data, wkage_f[, c("serial","pernum", grep("_f", names(wkage_f), val=T))], by=c("serial", "pernum") ,all.x = TRUE)
  county_data <- merge(county_data, wkage_m[,c("serial","pernum", grep("_m", names(wkage_m), val=T))], by=c("serial", "pernum"),all.x = TRUE)
  #Disa March 2021
  #Note that all variables for all people who were not included in the above are NA, this is what we want.
  
  
  #Disa March 2021
  #Note that for 1940 this information will only be available for a subset of 
  #the population since parents' birthplace is only asked for for a subset
  #Own birthplace was asked from everyone
  #I still keep the code the same as in 1930
  
  
  # (5) reference population: population over age 18 -------- START
  over18 <- county_data[which(county_data$age >= 18), ]
  
  # immigration
  over18$firstgen_nat <- as.numeric(over18$nativity == 5)
  over18$secondgen_nat <- as.numeric((over18$nativity >= 2 & over18$nativity <=4))
  over18$nativeborn_nat<- as.numeric(over18$nativity == 1)
  #reference population: population over age 18 -------- END
  
  # recombine subset
  county_data <- merge(county_data, over18[, c("serial","pernum", grep("_nat", names(over18), val=T))], by=c("serial", "pernum"), all.x = TRUE)
  
  
  # (6) reference population: population over age 18 who are 1st or 2nd gen migrants -------- START
  immig <- over18[which(over18$firstgen_nat == 1 | over18$secondgen_nat == 1), ]
  
  #Disa March 2021
  #I didn't understand if this code was correct or not,
  #I think it assumed that over18 and over18 are ordered in the same way. But I don't understand 
  #why we assume this when immig is a subset of over18. I change this, I think this is correct?
  immig$origin <- NA
  
  immig[immig$firstgen_nat == 1,]$origin <-immig[immig$firstgen_nat == 1,]$bpl 
  immig[immig$nativity == 2 | immig$nativity == 4,]$origin <-immig[immig$nativity == 2 | immig$nativity == 4,]$fbpl 
  immig[immig$nativity == 3,]$origin <-immig[immig$nativity == 3,]$mbpl 
  

  # for (i in 1:length(immig$serial)) {
  #   if (immig$firstgen_nat[i] == 1) {
  #     immig$origin[i] <- immig$bpl[i]
  #   } else if (immig$nativity[i] == 2 | immig$nativity[i] == 4) {
  #     immig$origin[i] <- immig$fbpl[i]
  #   } else if (immig$nativity[i] == 3) {
  #     immig$origin[i] <- immig$mbpl[i]
  #   }
  # }
  
  #Not available in 1940
  # non-English speaking household
  #immig$no_eng_bpl <- as.numeric(immig$language != 100)
  
  immig$puertorico_bpl <- as.numeric(immig$origin == 11000)
  immig$canada_bpl <- as.numeric((immig$origin >= 15000 & immig$origin < 15500))
  immig$NAmer_other_bpl <- as.numeric((immig$puertorico == 0 & immig$canada == 0 & immig$origin >= 10000 & immig$origin < 20000))
  immig$mex_CAmer_bpl <- as.numeric((immig$origin >= 20000 & immig$origin < 25000))
  immig$cuba_bpl <- as.numeric(immig$origin == 25000)
  immig$SAmer_bpl <- as.numeric((immig$origin >= 30000 & immig$origin < 31000))
  immig$WIndies_Afr_bpl <- as.numeric((immig$origin >= 26000 & immig$origin <= 26091) | (immig$origin == 26094 | immig$origin == 26095) | (immig$origin >= 60000 & immig$origin < 70000))
  immig$NEur_bpl <- as.numeric(immig$origin >= 40000 & immig$origin <= 40500)
  immig$ireland_bpl <- as.numeric(immig$origin >= 41400 & immig$origin < 41500)
  immig$uk_bpl <- as.numeric(immig$origin >= 41000 & immig$origin <= 41300)
  immig$WEur_bpl <- as.numeric(immig$origin >= 42000 & immig$origin < 43000)
  immig$italy_bpl <- as.numeric(immig$origin == 43400)
  immig$SEur_bpl <- as.numeric(immig$italy == 0 & (immig$origin >= 43000 & immig$origin <= 44000))
  immig$austria_ger_bpl <- as.numeric((immig$origin >= 45000 & immig$origin < 45100) | (immig$origin >= 45300 & immig$origin < 45400))
  immig$poland_bpl <- as.numeric(immig$origin >= 45500 & immig$origin < 45600)
  immig$ussr_bpl <- as.numeric(immig$origin >= 46500 & immig$origin < 47000)
  immig$CEEur_bpl <- as.numeric(immig$origin == 45100 | (immig$origin >= 45200 & immig$origin < 45300) | immig$origin == 45400 | (immig$origin >= 45600 & immig$origin <= 46300))
  immig$japan_bpl <- as.numeric(immig$origin == 50100)
  immig$asia_bpl <- as.numeric((immig$origin >= 50000 & immig$origin <= 52400 & immig$origin != 50100) | immig$origin == 54800 | immig$origin == 55000 | immig$origin == 59900)
  immig$mideast_bpl <- as.numeric((immig$origin >= 53000 & immig$origin <= 54700) | immig$origin == 54900)
  immig$aus_nz_pacific_bpl <- as.numeric(immig$origin >= 70000 & immig$origin < 72000)
  #reference population: population over age 18 -------- END
  
  # recombine subset
  county_data <- merge(county_data, immig[, c("serial","pernum", grep("_bpl", names(immig), val=T))], by=c("serial", "pernum"), all.x = TRUE)
  
  #Disa March 2021
  #I think we should consider all people who are either born in the US or have both parents born in the US as not being "from"
  #any of the countries/regions coded above, i.e., these people are considered to just be from the US.
  #So I recode all individuals over age 18 who have nativity == 1 as having each of the above country/region codes = 0
  #Note
  #b18 <- county_data[which(county_data$age >= 18), ]
  #table(b18$puertorico_bpl, b18$nativity, exclude=F)
  #             nativity
  #         1     2     3     4     5
  #0        0   1090   273  1942  1267
  #1        0     1     0     0     0
  #<NA>   19094   0     0     0     0
  #b18[b18$nativity == 1 & b18$age >= 18, ]$puertorico_bpl <- 0
  #table(b18$puertorico_bpl, b18$nativity, exclude=F)
  #             nativity
  #     1       2      3      4     5
  #0    19094   1090   273    1942  1267
  #1     0      1      0      0     0
  
  #county_data[county_data$nativity == 1 & county_data$age >= 18, ]$no_eng_bpl <- 0
  
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$puertorico_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$canada_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$NAmer_other_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mex_CAmer_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$cuba_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$SAmer_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$WIndies_Afr_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$NEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$ireland_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$uk_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$WEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$italy_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$SEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$austria_ger_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$poland_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$ussr_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$CEEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$japan_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$asia_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mideast_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$aus_nz_pacific_bpl <- 0
  
  
  # (7) reference population: population over age18 who are US born ------- START
  
  native <- county_data[which(county_data$age >= 18 & county_data$nativity==1), ]
  
  #Disa March 2021
  #I didn't double-check these codes.
  native$bpl_NE <- as.numeric(native$bpl == 900 | native$bpl == 2300 |native$bpl == 2500 |native$bpl == 3300 |
                                native$bpl == 4400 | native$bpl == 5000 |native$bpl == 3400 |native$bpl == 3600 |
                                native$bpl == 4200 )
  
  native$bpl_MW <- as.numeric(native$bpl == 1800 | native$bpl == 1700 |native$bpl == 2600 |native$bpl == 3900 |
                                native$bpl == 5500 | native$bpl == 1900 |native$bpl == 2000 |native$bpl == 2700|
                                native$bpl == 2900 |native$bpl == 3100 |native$bpl == 3800 |native$bpl == 4600 )
  
  native$bpl_S <- as.numeric(native$bpl == 1000 | native$bpl == 1100 |native$bpl == 1200 |native$bpl == 1300 |
                               native$bpl == 2400 | native$bpl == 3700 |native$bpl == 4500 |native$bpl == 5100 |
                               native$bpl == 5400 |  native$bpl == 100 | native$bpl == 2100 | native$bpl == 2800 |
                               native$bpl == 4700 | native$bpl == 500 | native$bpl == 2200 |  native$bpl == 4000 |
                               native$bpl == 4800 )
  
  native$bpl_W <- as.numeric(native$bpl == 400 | native$bpl == 800 |native$bpl == 1600 |native$bpl == 3500 |
                               native$bpl == 3000 | native$bpl == 4900 |native$bpl == 3200 |native$bpl == 5600 |
                               native$bpl == 200 | native$bpl == 600 | native$bpl == 1500 | native$bpl == 4100 |
                               native$bpl == 5300 )
  
  
  native$bpl_region <- NA
  if (sum(native$bpl_NE==1, na.rm=T) > 0){
    native[native$bpl_NE==1,]$bpl_region <- 1}
  if (sum(native$bpl_MW==1, na.rm=T) > 0){
    native[native$bpl_MW==1,]$bpl_region <- 2}
  if (sum(native$bpl_S==1, na.rm=T) > 0){
    native[native$bpl_S==1,]$bpl_region <- 3}
  #Adding this because no one born in West in South_Norfolk, VA
  if (sum(native$bpl_W==1,na.rm=T)>0){
    native[native$bpl_W==1,]$bpl_region <- 4}
  
  native$mig_bpl_NE <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 1 )
  native$mig_bpl_MW <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 2 )
  native$mig_bpl_S  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 3 )
  native$mig_bpl_W  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 4 )
  
  
  native$mig_bpl_NE_black <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 1 & native$black==1)
  native$mig_bpl_MW_black <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 2 & native$black==1)
  native$mig_bpl_S_black  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 3 & native$black==1)
  native$mig_bpl_W_black  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 4 & native$black==1)
  native$mig_bpl_NE_white <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 1 & native$race==100)
  native$mig_bpl_MW_white <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 2 & native$race==100)
  native$mig_bpl_S_white  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 3 & native$race==100)
  native$mig_bpl_W_white  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 4 & native$race==100)
  
  #reference population: population over age18 who are US born ------- END
  
  # recombine subset
  county_data <- merge(county_data, native[, c("serial","pernum", grep("bpl_", names(native), val=T))], by=c("serial", "pernum") ,all.x = TRUE)
  
  #Disa March 2021
  #I'm not sure how likely there is to be a neighborhood with only first or second generation immigrants?
  #Just in case, I will recode the native bpl and migration variables to 0 for immigrants. So the variables are 
  #then 'born in NE' and 0 for everyone outside of NE, even if in another country.
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$bpl_NE <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$bpl_MW <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$bpl_S <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$bpl_W <- 0
  
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_NE <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_MW <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_S <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_W <- 0
  
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_NE_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_MW_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_S_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_W_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_NE_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_MW_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_S_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18 & !is.na(county_data$nativity), ]$mig_bpl_W_white <- 0
  
  
  #Disa March 2021
  #Implementing the income imputation - START
  #----------------------------------------------------------------------------------------------
  #Load the light version of the estimates
  #load('/homes/nber/hynsjo/bulk/shared/ImputationRegressionResults/light_all_full.RData')
  #load('/homes/nber/hynsjo/shared/RCode/IncomeImputationResults/light_all_full.RData')
  ####
  
  #We will only predict using the same sample selection that we used for the regression, so I create a sample 
  #with men aged 25-55 who are employed.
  men_impute <- county_data[which(county_data$age >= 25 & county_data$age <= 55 & county_data$fem == 0 & (county_data$empstat == 10 | county_data$empstat == 13)), ]
  #men_impute <- select(men_impute, serial, pernum, age, occ1950, statefip, black, hisp, bpl)
  
  #We run the regression on a sample in which we have subtracted 25 from the age of everyone, so we need to do that here as well
  men_impute$age <- men_impute$age - 25
  men_impute$agesq <- men_impute$age^2
  #I need to create the immigrant variable in the same way as for the imputation regression
  men_impute$native_born <- as.numeric(men_impute$bpl >= 100 & men_impute$bpl <= 9900)
  #Make 'immigrant' variable so that can use 'native-born' as base-case
  men_impute$immigrant <- as.numeric(men_impute$native_born == 0)
  
  #Make the occupation variable into a factor variable 
  men_impute$occ1950f <- factor(men_impute$occ1950)
  #print(is.factor(men_impute$occ1950f))
  
  #1 digit occupational codes
  men_impute$occ1950_1d <- floor(men_impute$occ1950/100)
  men_impute$occ1950_1df <- factor(men_impute$occ1950_1d)
  
  #Make state a factor variable
  men_impute$statefipf <- factor(men_impute$statefip)
  
  #NOTE:
  #Some occupations that are present in the 1930 data are not there in the 1940 data
  #I guess we will just have to set them to missing
  #I need to delete all occupations that are not in the 1940 data here
  if (sum(men_impute$occ1950 == 0 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==0 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 >= 10 & men_impute$occ1950 <= 28,na.rm=T)>0){
    men_impute[men_impute$occ1950 >= 10 & men_impute$occ1950 <= 28 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 34 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==34 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 41 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==41 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 48 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==48 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 49 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==49 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 53 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==53 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 >= 59 & men_impute$occ1950 <= 69,na.rm=T)>0){
    men_impute[men_impute$occ1950 >= 59 & men_impute$occ1950 <= 69 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 72 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==72 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 77 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==77 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 >= 81 & men_impute$occ1950 <= 84,na.rm=T)>0){
    men_impute[men_impute$occ1950 >= 81 & men_impute$occ1950 <= 84 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 94 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==94 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 305 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==305 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 320 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==320 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 322 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==322 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 502 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==502 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 522 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==522 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 524 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==524 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 535 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==535 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 551 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==551 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 552 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==552 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 594 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==594 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 600 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==600 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 601 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==601 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 605 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==605 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 612 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==612 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 625 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==625 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 675 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==675 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 682 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==682 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 684 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==684 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 760 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==760 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 772 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==772 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 840 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==840 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 == 979 ,na.rm=T)>0){
    men_impute[men_impute$occ1950==979 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  if (sum(men_impute$occ1950 >= 980 & men_impute$occ1950 <= 999,na.rm=T)>0){
    men_impute[men_impute$occ1950 >= 980 & men_impute$occ1950 <= 999 & !is.na(men_impute$occ1950), ]$occ1950f <- NA}
  
  #See https://stackoverflow.com/questions/9028662/predict-maybe-im-not-understanding-it
  #men_impute$income_score <- predict(samp_bl_1, men_impute, na.action="na.pass")
  #This took no time at all
  men_impute$income_score <- predict(light_all_full, men_impute, na.action="na.pass")
  
  #I also impute a 'non-age-varying' income score. The idea is to measure the 'permanent value' of a person's
  #occupation, which cannot vary by age. I allow state and race to be allowed to vary but I reassign all
  #men's ages to be 40, 40-25 = 15
  men_impute$age <- 15
  men_impute$agesq <- 225
  men_impute$income_score_perm <- predict(light_all_full, men_impute, na.action="na.pass")
  
  #Now recombine dataset with original
  county_data <- merge(county_data, men_impute[, c("serial","pernum", grep("income_", names(men_impute), val=T))], by=c("serial", "pernum") ,all.x = TRUE)
  
  #I use the coefficient estimates to see if I got the expected predictions. I did this using the 
  #'baseline' model which doesn't have any of the interactions.
  #summary(samp_bl_1)
  #Call:
  #  lm(formula = loginc ~ age + agesq + black + hisp + immigrant + 
  #       occ1950f + statefipf, data = samp)
  #Coefficients:
  #  Estimate Std. Error t value Pr(>|t|)    
  #(Intercept)  5.314e+00  1.308e-01  40.636  < 2e-16 ***
  #  age          7.806e-02  2.326e-03  33.566  < 2e-16 ***
  #  agesq       -8.905e-04  2.965e-05 -30.031  < 2e-16 ***
  #  black       -3.028e-01  9.434e-03 -32.102  < 2e-16 ***
  #  hisp        -3.932e-01  6.890e-03 -57.077  < 2e-16 ***
  #  immigrant   -3.917e-02  5.902e-03  -6.637 3.22e-11 ***
  
  #occ1950f970 = -0.7242889
  #occ1950f564 = -0.5506960 
  #occ1950f690 = -0.3616084
  #occ1950f574 = -0.1281839
  
  #statefip9 =  0.3324597 
  
  #First four observations in data: 
  #   age   occ1950 statefip black hisp bpl agesq native_born
  #1   28     970        9     1    0 2200   784           1
  #6   53     564        9     0    0 2200  2809           1
  #15  33     690        9     0    0 2200  1089           1
  #20  28     574        9     0    0 2200   784           1
  #24  51     631        9     0    0 2200  2601           1
  #31  25     542        9     0    0 2200   625           1
  #       immigrant occ1950f occ1950_1d occ1950_1df statefipf
  #1          0      970          9           9         9
  #6          0      564          5           5         9
  #15         0      690          6           6         9
  #20         0      574          5           5         9
  #24         0      631          6           6         9
  #31         0      542          5           5         9
  #     income_score
  #1      6.107158
  #6      6.731787
  #15     6.891371
  #20     7.006104
  #24     7.651850
  #31     7.245216
  
  #Observation 1: 5.3143214 + 28*(0.0780598) + 784*(-0.0008905) + 1*(-0.3028418) + -0.7242889 + 0.3324597 =  6.107173
  #Observation 2: 5.3143214 + 53*(0.0780598) + 2809*(-0.0008905) + -0.5506960 + 0.3324597 = 6.73184
  #Observation 3: 5.3143214 + 33*(0.0780598) + 1089*(-0.0008905) + -0.3616084 + 0.3324597 = 6.891392
  #Observation 4: 5.3143214 + 28*(0.0780598) + 784*(-0.0008905) + -0.1281839+ 0.3324597 = 7.00612
  #So it yields the expected results
  #Implementing the income imputation - END
  #----------------------------------------------------------------------------------------------
  
  #### Miscellanea dummies
  
  #-------------------
  county_data$married_f <- as.numeric((county_data$marst == 2 |county_data$marst == 1) & county_data$fem==1)
  county_data[county_data$fem==0 | county_data$age < 18  ,]$married_f <-  NA
  county_data$married_m <- as.numeric((county_data$marst == 2 |county_data$marst == 1) & county_data$fem==0)
  county_data[county_data$fem==1 | county_data$age < 18 ,]$married_m <-  NA
  
  #-------------------
  county_data$child_sch <- as.numeric(county_data$school == 2)
  county_data[county_data$age < 6 | county_data$age > 15  ,]$child_sch <-  NA
  
  county_data$ado_sch <- as.numeric(county_data$school == 2)
  county_data[county_data$age < 16 | county_data$age > 18  ,]$ado_sch <-  NA
  
  #-------------------
  county_data$mom_teen <- as.numeric(county_data$nchild >0 )
  county_data[county_data$age < 13 | county_data$age > 19  ,]$mom_teen <-  NA
  
  #-------------------
  
  
  if (sum(county_data$dwsize>1000)>0) {
    county_data[county_data$dwsize>1000,]$dwsize <- NA
  }
  if (sum(county_data$famsize>29, na.rm=T)>0) {
    county_data[county_data$famsize>29,]$famsize <- NA
  }
  if (sum(county_data$numperhh>1000, na.rm=T)>0) {
    county_data[county_data$numperhh>1000,]$numperhh <- NA
  }
  
  
  #-------------------
  
  county_data$multi_fam <- as.numeric(county_data$nfams > 2)
  
  #-------------------
  
  county_data$multi_fam <- as.numeric(county_data$nfams > 2)
  
  #-------------------
  
  #county_data$radio <-  county_data$radio30 -1
  
  #-------------------
  #Disa March 2021
  #hhtype == 9 should be NA
  #Made changes here to correct
  if (sum(county_data$hhtype == 9,na.rm=T)>0){
    county_data[county_data$hhtype == 9,]$hhtype <- NA}
  
  county_data$hhtype_married_both <-  as.numeric(county_data$hhtype == 1)
  county_data$hhtype_married_no_w <-  as.numeric(county_data$hhtype == 2)
  county_data$hhtype_married_no_h <-  as.numeric(county_data$hhtype == 3)
  county_data$hhtype_single_m     <-  as.numeric(county_data$hhtype == 4) #This is single man living alone
  county_data$hhtype_single_f     <-  as.numeric(county_data$hhtype == 6) #This is single woman living alone
  
  
  #-------------------
  county_data$multgen_3_gen <-as.numeric(county_data$multgen >= 30)
  
  #-------------------
  #Disa March 2021
  #Variable for whether the individual is a 'boarder, lodger, roomer, tenant' in the household
  #https://usa.ipums.org/usa-action/variables/RELATE#codes_section
  county_data$hh_lodger <- as.numeric(county_data$relate >= 1201 & county_data$relate <= 1205)
  
  #------------------
  #Disa March 2021
  #No idea if this matters. But, we had asked Chelsea to make dummy variables for some things that were in the address
  #information, like if the house is in the "REAR" and some other things. We don't have those variables that she created
  #in these data since then we saved the cleaned address files separately, so I add the code that she wrote for this here. 
  # convents
  county_data$convent <- as.numeric(grepl("CONVENT", county_data$street))
  # hospitals
  county_data$hospital <- as.numeric(grepl("HOSP", county_data$street))
  # group homes
  county_data$grphome <- as.numeric(grepl("[[:upper:]]+ HOME", county_data$street))
  
  # rear
  # assumed that EAR was a mistranscription of REAR
  # county_data$rear <- 0
  # 
  # i <- 1
  # for (i in 1:length(county_data$street)) {
  #   if (grepl("\\<R*EAR\\>|\\<RAER\\>", county_data$rawhnum[i]) == TRUE || grepl("\\<REAR\\>", county_data$street[i]) == TRUE) {
  #     county_data$rear[i] <- 1
  #     #print(county_data$street[i])
  #     #print(county_data$rawhnu[i])
  #   }
  # }
  #Relationhips in San Fransisco is as expected, but very few observations
  #data2<-county_data[county_data$fem==0 & county_data$age>= 25 & county_data$age <=55,]
  #print(summary(lm(rear ~ occscore+age+black+hisp, data=data2)))
  
  
  #Disa
  #Additional variables that we could use as outcomes in 1940 (but we don't have in 1930)
  #------------------------------------------------------------------------------------------------------
  #Dummy for if have at least some college and dummy for if have HS or more
  #reference population: I take population 24-65 just because in the oldest cohorts basically no one will
  #have gone to college
  over24 <- county_data[which(county_data$age >= 24 & county_data$age <= 65), ]
  
  over24$some_col_edc <- as.numeric(over24$educ >= 70 & over24$educ < 900)
  over24$HS_edc  <- as.numeric(over24$educ >= 60 & over24$educ < 900)
  
  # recombine subset
  county_data <- merge(county_data, over24[, c("serial","pernum", grep("_edc", names(over24), val=T))], by=c("serial", "pernum"), all.x = TRUE)
  
  #incwage
  #We may want to compute the average incwage, I assume that we will do this only for men aged 18-65, conditional on having a positive
  #wage 
  #For now, I will just set to missing if incwage is 0 or if person is not man aged 18-65
  #NOTE: incwage is actually earnings.
  #To get hourly wages, I divide this variable by hours*weeks
  
  #I have no idea why, but this is giving me an error here even when there are 0s
  #I don't know what to do.
  #county_data[county_data$incwage == 0, ]$incwage <- NA
  #county_data[county_data$incwage < 1, ]$incwage <- NA
  #NOTE: I reset to NA if == 0 above bc when there is ANY NA in the variable this replace function doesn't work
  
  county_data[county_data$fem == 1, ]$incwage <- NA
  county_data[county_data$age < 25 | county_data$age > 60, ]$incwage <- NA
  #hrswork1 is censored at 98
  #I don't censor it more for now although of course it would be hard to work 98 hours/week every week.
  #but maybe some people did work crazy hours.
  county_data$hourly_wage = county_data$incwage/(county_data$wkswork1*county_data$hrswork1)
  
  #incnonwg
  #change coding so that is a dummy indicating whether had > 50$ nonwage income
  #I set to NA if woman or not adult
  #I only use individuals up to age 55. If the idea is to measure nonretirement wealth, then it makes sense. 
  county_data[county_data$fem == 1 | county_data$age < 18 | county_data$age > 55,]$incnonwg <- NA
  county_data$incnonwg <- county_data$incnonwg - 1 
  
  #migrate5
  #Make one dummy for if lived in same house as 5 years ago and one for if moved within the county
  #This should probably just be one per household head. But for now I do this for all over age 24
  over24$stayed_house <- as.numeric(over24$migrate5 == 10)
  over24$moved_house  <- as.numeric(over24$migrate5 == 21)
  
  
  # recombine subset
  county_data <- merge(county_data, over24[, c("serial","pernum", grep("_house", names(over24), val=T))], by=c("serial", "pernum"), all.x = TRUE)
  
  outdir_census <- paste0(output_dir,"/", county,"_1940_cleaned.csv")
  fwrite(county_data, outdir_census,na = 'NA')
  gc()
  print(paste0("Done with ", county))
  
}



cl <- makePSOCKcluster(12, outfile ="" )
clusterEvalQ(cl, c(library('dplyr'),library('stringr'),library('sf'),
                   library('tidyverse'), library('data.table'),library('DescTools')))
clusterExport(cl, c("year","cleaning_input_revised", "light_all_full",
                    "input_dir","output_dir","clean_1940_file"))
clusterApplyLB(cl,cleaning_input_revised, clean_1940_file)
stopCluster(cl)
gc()


output_list <- list.files("revised/clean_census_output/files_1940")
output_list <- str_remove(output_list, "_1940_cleaned.csv")
setdiff(cleaning_input_revised,output_list)
setdiff(output_list,cleaning_input_revised)

# for (i in 1:length(cleaning_input_revised)){
# file.rename(paste0( cleaning_input_revised[i], "_1930_cleaned.csv"),
#             paste0(cleaning_input_revised[i],"_1940_cleaned.csv"))
# }
