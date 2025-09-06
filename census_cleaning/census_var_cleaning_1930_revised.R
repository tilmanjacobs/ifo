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

year <- 1930

#############
## Cleaning Input
cleaning_input_trt <- list.files(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/countyFiles/counties", year,"_final"))
cleaning_input_trt <- str_remove(cleaning_input_trt, "dta_")
cleaning_input_trt <- str_remove(cleaning_input_trt, ".csv")
cleaning_input_trt <- setdiff(cleaning_input_trt,grep("city_",cleaning_input_trt, value=T))


cleaning_input_cntrl <- list.files(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/countyFiles/countiesControl", year,"_pop"))
cleaning_input_cntrl <- str_remove(cleaning_input_cntrl, "dta_")
cleaning_input_cntrl <- str_remove(cleaning_input_cntrl, ".csv")

cleaning_input_trt_cntrl <- unique(c(cleaning_input_trt,cleaning_input_cntrl))



## Geocode Output - Treated
geocode_output_trt <- list.files(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/geocode_output_v3/output_files_", year))
geocode_output_trt <- str_remove(geocode_output_trt, paste0("_output_",year))
geocode_output_trt <- str_remove(geocode_output_trt, "_output")
geocode_output_trt <- str_remove(geocode_output_trt, paste0("_",year))

## Geocode Output - Control
geocode_output_cntrl <- list.files(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/geocode_output_v4_control/pop/output_files_", year))
geocode_output_cntrl <- str_remove(geocode_output_cntrl, "_output")
geocode_output_cntrl <- str_remove(geocode_output_cntrl, paste0("_",year))

## These should be coherent with
geocode_output_revised <- list.files(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/revised/geocode_output/output_files_", year))
geocode_output_revised <- str_remove(geocode_output_revised, "_output")
geocode_output_revised <- str_remove(geocode_output_revised, paste0("_",year))

geocode_output_trt_cntrl <- unique(c(geocode_output_trt,geocode_output_cntrl))

# Verified for 1910,1920,1930,1940
setdiff(geocode_output_trt_cntrl,geocode_output_revised)
setdiff(geocode_output_revised,geocode_output_trt_cntrl)


## Check Discrepancies between Geocoded output and Cleaning Input
setdiff(geocode_output_trt_cntrl,cleaning_input_trt_cntrl)
setdiff(cleaning_input_trt_cntrl,geocode_output_trt_cntrl)


# Discrepancies 
c("Autauga_AL" , "Joplin_2_MO")


##
cleaning_input_revised <- list.files(paste0("revised/clean_census_input/files_", year))
cleaning_input_revised <- str_remove(cleaning_input_revised, "dta_")
cleaning_input_revised <- str_remove(cleaning_input_revised, ".csv")
setdiff(cleaning_input_trt_cntrl,cleaning_input_revised)
setdiff(cleaning_input_revised,cleaning_input_trt_cntrl)


setwd(paste0("/disk/homedirs/nber/hynsjo/bulk/shared/countyFiles/counties", year,"_final"))
all_counties <- list.files()
all_counties <- str_remove(all_counties, "city_dta_")
all_counties <- str_remove(all_counties, "dta_")
all_counties <- str_remove(all_counties, ".csv")

setdiff(selected_counties,all_counties )
setdiff(all_counties,selected_counties )

####

#------------------------------------------------------------------------------------------------------------------------------------
#Function Definition
#--------------
# The function takes as an input the county names , sets the directory, load the data
# and does all the cleanning. At the end, it saved the cleaned file in the appropriate directory. 
#  It prints a message confirming it finished working on the county, other than that,
#  the function does not have output within R, becuase it automatically writes the file in the folder.

#  The cleaning follows the exact same order outlined in the instructions for Chelsea,
#  so it should be pretty clear when comparing it with the instructions.

cleaning_input_revised <- list.files(paste0("revised/clean_census_input/files_", year))
cleaning_input_revised <- str_remove(cleaning_input_revised, "dta_")
cleaning_input_revised <- str_remove(cleaning_input_revised, ".csv")


load('revised/scripts/light_all_full.RData')

input_dir <- paste0("revised/clean_census_input/files_",year)
output_dir <- paste0("revised/clean_census_output/files_",year)

clean_1930_file <- function(county){
  
  
  county_data <- fread(paste0(input_dir,"/dta_", county , ".csv"), stringsAsFactors = FALSE, na.strings= c("NA","nan",""))
  county_data <- as.data.frame(county_data)
  
  # (1) drop unnecessary variables ----------------------------------------------------------------------
  # drop q variables
  county_data <- county_data[,!grepl("^q", names(county_data))]

  
  # drop other variables
  drop <- c("rectype", "rectypep", "datanump", "slwtreg", "momrule_hist", "poprule_hist",
            "sprule_hist", "hisprule", "perwt", "datanum", "pageno", "perwtreg", "imageied",
            "momloc", "poploc", "stepmom", "steppop", "bplstr", "mbplstr", "fbpstr", "occstr")
  county_data <- county_data[, !(names(county_data) %in% drop)]
  
  # (2) replace missing values to NA -------------------------------------------------------------------
  
  if (sum(county_data$citypop == 99999|county_data$citypop == 0 ,na.rm=T)>0){
    county_data[county_data$citypop == 99999 |county_data$citypop == 0 , ]$citypop <- NA}
  # Added By Luca
  if (sum(county_data$city == 0,na.rm=T)>1 | sum(county_data$city == 9999,na.rm=T)>1){
    county_data[county_data$city == 0 | county_data$city == 9999, ]$city <- NA}
  
  if (sum(county_data$eldch == 99,na.rm=T)>0){
    county_data[county_data$eldch == 99, ]$eldch <- NA}
  if ( sum(county_data$yngch == 99,na.rm=T)>0){
    county_data[county_data$yngch == 99, ]$yngch <- NA}
  if (sum(county_data$citizen == 0,na.rm=T)>0){
    county_data[county_data$citizen == 0 | county_data$yrsusa2 == 9, ]$citizen <- NA}
  if (sum(county_data$yrsusa2 == 0 | county_data$yrsusa2 == 9,na.rm=T)>0){
    county_data[county_data$yrsusa2 == 0 | county_data$yrsusa2 == 9, ]$yrsusa2 <- NA}
  #Disa March 2021
  #Adding yrsusa1. See explanation between yrsusa1 and yrsusa2 
  #https://usa.ipums.org/usa-action/variables/YRSUSA2#codes_section
  #yrsusa1 == 0 is NA if born in the US and means 0 if not born in the US
  if (sum(county_data$yrsusa1 == 0 & county_data$bpl < 10000,na.rm=T)>0){
    county_data[county_data$yrsusa1 == 0 & county_data$bpl < 10000, ]$yrsusa1 <- NA}
  if (sum(county_data$mtongue == 0,na.rm=T| county_data$mtongue > 9500)>0){
    county_data[county_data$mtongue == 0 | county_data$mtongue >= 9500, ]$mtongue <- NA}
  if (sum(county_data$language == 0 | county_data$language >= 9500,na.rm=T)>0){
    county_data[county_data$language == 0 | county_data$language >= 9500, ]$language <- NA}
  if (sum(county_data$speakeng == 0 | county_data$speakeng >= 7,na.rm=T)>0){
    county_data[county_data$speakeng == 0 | county_data$speakeng >= 7,]$speakeng <- NA}
  if (sum(county_data$lit == 0,na.rm=T)>0){
    county_data[county_data$lit == 0,]$lit <- NA}
  if (sum(county_data$labforce == 0,na.rm=T)>0){
    county_data[county_data$labforce == 0, ]$labforce <- NA}
  if (sum(county_data$occ1950 >= 995,na.rm=T)>0){
    county_data[county_data$occ1950 >= 995, ]$occ1950 <- NA}
  if (sum(county_data$occscore == 0| county_data$occscore > 80,na.rm=T)>0){
    county_data[county_data$occscore == 0 | county_data$occscore > 80 ,]$occscore <- NA}
  if (sum(county_data$sei == 0,na.rm=T)>0){
    county_data[county_data$sei == 0, ]$sei <- NA}
  #if (sum(county_data$ind1950 == 0 | county_data$ind1950 == 998 | county_data$ind1950 == 999,na.rm=T)>0){
  #  county_data[county_data$ind1950 == 0 | county_data$ind1950 == 998 | county_data$ind1950 == 999, ]$ind1950 <- NA}
  
  #Disa:
  #instead put all above 976 to missing
  #Alternatively, we may want to measure the number of housewives using
  #ind1950 == 982
  if (sum(  (county_data$ind1950 > 976 & county_data$ind1950 <= 999)  | county_data$ind1950 == 0, na.rm=T) ){
    county_data[(county_data$ind1950 > 976 & county_data$ind1950 <= 999) | county_data$ind1950 == 0 , ]$ind1950 <- NA}
  
  if (sum(county_data$classwkr == 0,na.rm=T)>0){
    county_data[county_data$classwkr == 0,]$classwkr <- NA}
  if (sum(county_data$presgl == 0,na.rm=T)>0){
    county_data[county_data$presgl == 0, ]$presgl <- NA}
  if (sum(county_data$erscor50 >= 999 ,na.rm=T)>0){
    county_data[county_data$erscor50 >= 999 ,]$erscor50 <- NA}
  if (sum(county_data$edscor50>=999,na.rm=T)>0){
    county_data[county_data$edscor50 >=999 ,]$edscor50 <- NA}
  if (sum(county_data$npboss50 >= 999,na.rm=T)>0){
    county_data[county_data$npboss50 >= 999, ]$npboss50 <- NA}
  if (sum(county_data$yrimmig == 0,na.rm=T)>0){
    county_data[county_data$yrimmig == 0, ]$yrimmig <- NA}
  #NOTE:
  #In county Middlesex_MA in 1930, there is one observation with missing agemarr, that's why
  #I added this extra code here
  if (sum(county_data$agemarr == 0,na.rm=T)>0){
    county_data[county_data$agemarr == 0 & !is.na(county_data$agemarr), ]$agemarr <- NA}
  if (sum(county_data$agemonth >= 98,na.rm=T)>0){
    county_data[county_data$agemonth >= 98 & !is.na(county_data$agemonth), ]$agemonth <- NA}
  if (sum(county_data$vet1930 == 6 | county_data$vet1930 == 0,na.rm=T)>0){
    county_data[(county_data$vet1930 == 6 | county_data$vet1930 == 0) & !is.na(county_data$vet1930), ]$vet1930 <- NA}
  if (sum(county_data$urbarea == 0,na.rm=T)>0){
    county_data[county_data$urbarea == 0, ]$urbarea <- NA}
  if (sum(county_data$gqtype == 0,na.rm=T)>0){
    county_data[county_data$gqtype == 0, ]$gqtype <- NA}
  if (sum(county_data$gqfunds == 0 | county_data$gqfunds == 99,na.rm=T)>0){
    county_data[county_data$gqfunds == 0 | county_data$gqfunds == 99, ]$gqfunds <- NA}
  if (sum(county_data$ownershp == 0,na.rm=T)>0){
    county_data[county_data$ownershp == 0, ]$ownershp <- NA}
  if (sum(county_data$hhtype == 0 | county_data$hhtype == 9,na.rm=T)>0){
    county_data[county_data$hhtype == 0 | county_data$hhtype == 9, ]$hhtype <- NA}
  if (sum(county_data$valueh == 0 | county_data$valueh == 9999999,na.rm=T)>0){
    county_data[county_data$valueh == 0 | county_data$valueh == 9999999  ,]$valueh <- NA}
  ## Added in last round of revision
  county_data$valueh_topcoded <- county_data$valueh
  if (sum( county_data$valueh_topcoded >30000,na.rm=T)>0){
    county_data[ county_data$valueh_topcoded >30000   & !is.na(county_data$valueh_topcoded) ,]$valueh_topcoded <- NA}
  
  if (sum(county_data$multgen == 0,na.rm=T)>0){
    county_data[county_data$multgen == 0, ]$multgen <- NA}
  if (sum(county_data$rent30 == 0 | county_data$rent30 >9995,na.rm=T)>0){
    county_data[county_data$rent30 == 0 | county_data$rent30 >9995  ,]$rent30 <- NA}
  
  #Disa March 2021
  #I made some changes in the below which I make notes for
  # (3) create new full population variables ----------------------------------------------------------------------
  #Disa March 2021
  #What happens when using as.numeric() when there are missing values? - want it to assign as missing
  #table(county_data$citypop)
  #   1    4   23 
  #189  450 2331    ->  2970 rows with nonmissing information on citypop
  #nrow(county_data)
  #19693
  #county_data$b <- as.numeric(county_data$citypop == 4)
  #table(county_data$b)
  #   0    1 
  #2520  450
  #In total 2331 + 189 = 2520 rows set to 0 and 450 set to 1. 
  #So using as.numeric set missing to missing.
  
  
  
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
  county_data$lit <- as.numeric(county_data$lit == 4)
  
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
  county_data[county_data$bpl >= 100 & county_data$bpl <= 12092, ]$yidd <- 0
  
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
  immig$origin <- as.integer(NA)
  
  # for (i in 1:length(immig$serial)) {
  #   if (immig$firstgen_nat[i] == 1) {
  #     immig$origin[i] <- immig$bpl[i]
  #   } else if (immig$nativity[i] == 2 | immig$nativity[i] == 4) {
  #     immig$origin[i] <- immig$fbpl[i]
  #   } else if (immig$nativity[i] == 3) {
  #     immig$origin[i] <- immig$mbpl[i]
  #   }
  # }
  
  immig[immig$firstgen_nat == 1,]$origin <-immig[immig$firstgen_nat == 1,]$bpl 
  immig[immig$nativity == 2 | immig$nativity == 4,]$origin <-immig[immig$nativity == 2 | immig$nativity == 4,]$fbpl 
  immig[immig$nativity == 3,]$origin <-immig[immig$nativity == 3,]$mbpl 
  
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
  
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$puertorico_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$canada_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$NAmer_other_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$mex_CAmer_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$cuba_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$SAmer_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$WIndies_Afr_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$NEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$ireland_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$uk_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$WEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$italy_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$SEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$austria_ger_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$poland_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$ussr_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$CEEur_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$japan_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$asia_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$mideast_bpl <- 0
  county_data[county_data$nativity == 1 & county_data$age >= 18, ]$aus_nz_pacific_bpl <- 0
  
  
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
  native[native$bpl_NE==1,]$bpl_region <- 1
  native[native$bpl_MW==1,]$bpl_region <- 2
  native[native$bpl_S==1,]$bpl_region <- 3
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
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$bpl_NE <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$bpl_MW <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$bpl_S <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$bpl_W <- 0
  
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_NE <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_MW <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_S <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_W <- 0
  
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_NE_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_MW_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_S_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_W_black <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_NE_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_MW_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_S_white <- 0
  county_data[county_data$nativity != 1 & county_data$age >= 18, ]$mig_bpl_W_white <- 0
  
  
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
  
  county_data$radio <-  county_data$radio30 -1
  
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
  
  #stop()
  
  #Saving file
  #------------------
  outdir_census <- paste0(output_dir,"/", county,"_1930_cleaned.csv")
  fwrite(county_data, outdir_census,na = 'NA')
  gc()
  print(paste0("Done with ", county))
}


needed_counties <- c("19_1130", "23_50","25_30","25_270",
                     "34_310","36_130","48_4850","55_310")

county_names<- c("Linn_IA", "Cumberland_ME",
                 "Berkshire_MA", "Worcester_MA",
                 "Passaic_NJ","Chautauqua_NY",
                 "Wichita_TX","Douglas_WI")


cl <- makePSOCKcluster(8, outfile ="" )
clusterEvalQ(cl, c(library('dplyr'),library('stringr'),library('sf'),
                   library('tidyverse'), library('data.table'),library('DescTools')))
clusterExport(cl, c("year","cleaning_input_revised", "light_all_full",
                    "input_dir","output_dir","clean_1930_file","county_names"))
clusterApplyLB(cl,county_names ,clean_1930_file)
stopCluster(cl)
gc()

output_list <- list.files("revised/clean_census_output/files_1930")
output_list <- str_remove(output_list, "_1930_cleaned.csv")


setdiff(cleaning_input_revised,output_list)
clean_1930_file("Passaic_NJ")
