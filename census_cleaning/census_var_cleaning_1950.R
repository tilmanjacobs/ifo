#------------------------------------------------------------------------------------------------------------------------------------
#Tilman 21/11/2024
#------------------------------------------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------------------------------------------------------
#HOUSEHOLDING
#------------------------------------------------------------------------------------------------------------------------------------

#Clean out all user defined data
rm(list = ls())

#------------------------------------------------------------------------------------------------------------------------------------
#PACKAGES
#------------------------------------------------------------------------------------------------------------------------------------

library('Hmisc')
library('dplyr')
library('stringr')
#library('sf')
library('tidyverse')
library('data.table')
library('DescTools')
library('parallel')

setwd("/Users/tilmanjacobs/Documents/ifo/census_cleaning")

#set this to either '/IPUMS_' or '/dta_' depending on the data you are working with
ipums_or_census <- '/IPUMS_'

year <- 1950

cleaning_input <- list.files(paste0("input/", year)) %>% 
  str_remove("IPUMS_") %>%
  str_remove(".csv")


#hanover_data <- read.csv('input/1950/IPUMS_New Hanover_NC.csv')

input_dir <- paste0("input/", year)
output_dir <- paste0("output/", year)

clean_1950_file <- function(county){
  # Read the data and convert all column names to lowercase
  county_data <- fread(paste0(input_dir,ipums_or_census, county , ".csv"), stringsAsFactors = FALSE, na.strings= c("NA","nan","")) %>%  
                as.data.frame() %>%
                setnames(tolower(names(.)))
  
  county_data <- county_data[,!grepl("^q", names(county_data))]

  drop <- c("rectype", "rectypep", "datanump", "slwtreg",    
            "momrule_hist", "poprule_hist",
            "sprule_hist", "perwt", "datanum", "pageno", "perwtreg", "imageied",
            "momloc", "poploc", "stepmom", "steppop", "bplstr", "mbplstr", "fbpstr", "occstr", "mtongstr", "mcit5str", "indstr", "fbplstr",
            "relstr")
  county_data <- county_data[, !(names(county_data) %in% drop)]
  


  ###########################################################################################
  ########### In this section I convert missing/unknown/NA codes to NA ######################
  ###########################################################################################

  missing_vars <- c()
  if("farm" %in% names(county_data)) {
    county_data$farm[county_data$farm == 0] <- NA 
  } else {
    missing_vars <- c(missing_vars, "farm")
  }
  if("bpl" %in% names(county_data)) {
    county_data$bpl[county_data$bpl %in% c(997, 998, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "bpl")
  }
  if("gqtype" %in% names(county_data)) {
    county_data$gqtype[county_data$gqtype == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "gqtype") 
  }
  if("hadnjob" %in% names(county_data)) {
    county_data$hadnjob[county_data$hadnjob == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "hadnjob")
  }
  if("higrade" %in% names(county_data)) {
    county_data$higrade[county_data$higrade == 99] <- NA
  } else {
    missing_vars <- c(missing_vars, "higrade")
  }
  if("hrswork1" %in% names(county_data)) {
    county_data$hrswork1[county_data$hrswork1 == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "hrswork1")
  }
  if("hrswork2" %in% names(county_data)) {
    county_data$hrswork2[county_data$hrswork2 == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "hrswork2")
  }
  if("incbusfm" %in% names(county_data)) {
    county_data$incbusfm[county_data$incbusfm %in% c(99999, 99998)] <- NA
  } else {
    missing_vars <- c(missing_vars, "incbusfm")
  }
  if("incother" %in% names(county_data)) {
    county_data$incother[county_data$incother %in% c(99999, 99998)] <- NA
  } else {
    missing_vars <- c(missing_vars, "incother")
  }
  if("inctot" %in% names(county_data)) {
    county_data$inctot[county_data$inctot  %in% c(000000, 99999, 99998)] <- NA
  } else {
    missing_vars <- c(missing_vars, "inctot")
  }
  if("incwage" %in% names(county_data)) {
    county_data$incwage[county_data$incwage %in% c(999999, 999998)] <- NA
  } else {
    missing_vars <- c(missing_vars, "incwage")
  }
  if("ind" %in% names(county_data)) {
    county_data$ind[county_data$ind %in% c(0, 995, 997, 998,999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "ind")
  }
  if("ind1950" %in% names(county_data)) {
    county_data$ind1950[county_data$ind1950 %in% c(0, 995, 997, 998,999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "ind1950")
  }
  if("labforce" %in% names(county_data)) {
    county_data$labforce[county_data$labforce %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "labforce")
  }
  if("marmo" %in% names(county_data)) {
    county_data$marmo[county_data$marmo %in% c(0, 7, 8, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "marmo")
  }
  if("marst" %in% names(county_data)) {
    county_data$marst[county_data$marst == 9] <- NA
  } else {
    missing_vars <- c(missing_vars, "marst")
  }
  if("mbpl" %in% names(county_data)) {
    county_data$mbpl[county_data$mbpl %in% c(0, 997, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "mbpl")
  }
  if("momloc" %in% names(county_data)) {
    county_data$momloc[county_data$momloc == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "momloc")
  }
  if("ncouple" %in% names(county_data)) {
    county_data$ncouple[county_data$ncouple == 0] <- NA #this is one of the vars with bad coding (0 <=> 0 couples OR NA)
  } else {
    missing_vars <- c(missing_vars, "ncouple")
  }
  if("nfams" %in% names(county_data)) {
    county_data$nfams[county_data$nfams == 01] <- NA #same as above (01 <=> 1 family OR NA)
  } else {
    missing_vars <- c(missing_vars, "nfams")
  }
  if("nfathers" %in% names(county_data)) {
    county_data$nfathers[county_data$nfathers == 0] <- NA #same as above (0 <=> 0 fathers OR NA)
  } else {
    missing_vars <- c(missing_vars, "nfathers")
  }
  if("nmothers" %in% names(county_data)) {
    county_data$nmothers[county_data$nmothers == 0] <- NA #same as above (0 <=> 0 mothers OR NA)
  } else {
    missing_vars <- c(missing_vars, "nmothers")
  }
  if("nsubfam" %in% names(county_data)) {
    county_data$nsubfam[county_data$nsubfam == 0] <- NA #0 - No subfamilies or N/A (GQ/vacant unit)
  } else {
    missing_vars <- c(missing_vars, "nsubfam")
  }
  if("numperh" %in% names(county_data)) {
    county_data$numperh[county_data$numperh == 9999] <- NA
  } else {
    missing_vars <- c(missing_vars, "numperh")
  }
  if("occ" %in% names(county_data)) {
    county_data$occ[county_data$occ %in% c(997, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "occ")
  }
  if("occ1950" %in% names(county_data)) {
    county_data$occ1950[county_data$occ1950 %in% c(997, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "occ1950")
  }
  if("occscore" %in% names(county_data)) {
    county_data$occscore[county_data$occscore == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "occscore")
  }
  if("pernum" %in% names(county_data)) {
    county_data$pernum[county_data$pernum == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "pernum")
  }
  if("prent" %in% names(county_data)) {
    county_data$prent[county_data$prent == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "prent")
  }
  if("region" %in% names(county_data)) {
    county_data$region[county_data$region == 97] <- NA
  } else {
    missing_vars <- c(missing_vars, "region")
  }
  if("acrenonf" %in% names(county_data)) {
    county_data$acrenonf[county_data$acrenonf %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "acrenonf")
  }
  if("activity" %in% names(county_data)) {
    county_data$activity[county_data$activity %in% c(0)] <- NA
  } else {
    missing_vars <- c(missing_vars, "activity")
  }
  if("age" %in% names(county_data)) {
    county_data$age[county_data$age == 999] <- NA
  } else {
    missing_vars <- c(missing_vars, "age")
  }
  if("birthmo" %in% names(county_data)) {
    county_data$birthmo[county_data$birthmo %in% c(0, 99)] <- NA
  } else {
    missing_vars <- c(missing_vars, "birthmo")
  }
  if("birthqtr" %in% names(county_data)) {
    county_data$birthqtr[county_data$birthqtr %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "birthqtr")
  }
  if("birthyr" %in% names(county_data)) {
    county_data$birthyr[county_data$birthyr %in% c(9996, 9997, 9998, 9999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "birthyr")
  }
  if("chborn" %in% names(county_data)) {
    county_data$chborn[county_data$chborn %in% c(0, 98, 99)] <- NA
  } else {
    missing_vars <- c(missing_vars, "chborn")
  }
  if("citizen" %in% names(county_data)) {
    county_data$citizen[county_data$citizen %in% c(0, 8, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "citizen")
  }
  if("city" %in% names(county_data)) {
    county_data$city[county_data$city == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "city")
  }
  if("classwkr" %in% names(county_data)) {
    county_data$classwkr[county_data$classwkr %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "classwkr")
  }
  if("countyicp" %in% names(county_data)) {
    county_data$countyicp[county_data$countyicp == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "countyicp")
  }
  if("durmarr" %in% names(county_data)) {
    county_data$durmarr[county_data$durmarr %in% c(98, 99)] <- NA
  } else {
    missing_vars <- c(missing_vars, "durmarr")
  }
  if("durunemp" %in% names(county_data)) {
    county_data$durunemp[county_data$durunemp %in% c(998, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "durunemp")
  }
  if("edscor50" %in% names(county_data)) {
    county_data$edscor50[county_data$edscor50 == 9999] <- NA
  } else {
    missing_vars <- c(missing_vars, "edscor50")
  }
  if("edscor90" %in% names(county_data)) {
    county_data$edscor90[county_data$edscor90 == 9999] <- NA
  } else {
    missing_vars <- c(missing_vars, "edscor90")
  }
  if("educ" %in% names(county_data)) {
    county_data$educ[county_data$educ %in% c(0, 99)] <- NA
  } else {
    missing_vars <- c(missing_vars, "educ")
  }
  if("eldch" %in% names(county_data)) {
    county_data$eldch[county_data$eldch == 99] <- NA
  } else {
    missing_vars <- c(missing_vars, "eldch")
  }
  if("empstat" %in% names(county_data)) {
    county_data$empstat[county_data$empstat %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "empstat")
  }
  if("erscor50" %in% names(county_data)) {
    county_data$erscor50[county_data$erscor50 == 9999] <- NA
  } else {
    missing_vars <- c(missing_vars, "erscor50")
  }
  if("erscor90" %in% names(county_data)) {
    county_data$erscor90[county_data$erscor90 == 9999] <- NA
  } else {
    missing_vars <- c(missing_vars, "erscor90")
  }
  if("fbpl" %in% names(county_data)) {
    county_data$fbpl[county_data$fbpl %in% c(0, 997, 998, 999)] <- NA
  } else {
    missing_vars <- c(missing_vars, "fbpl")
  }
  if("gqfunds" %in% names(county_data)) {
    county_data$gqfunds[county_data$gqfunds %in% c(00,99)] <- NA
  } else {
    missing_vars <- c(missing_vars, "gqfunds")
  }
  if("hadnajob" %in% names(county_data)) {
    county_data$hadnajob[county_data$hadnajob == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "hadnajob")
  }
  if("hispan" %in% names(county_data)) {
    county_data$hispan[county_data$hispan == 9] <- NA
  } else {
    missing_vars <- c(missing_vars, "hispan")
  }
  if("marrno" %in% names(county_data)) {
    county_data$marrno[county_data$marrno %in% c(0,7,8,9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "marrno")
  }
  if("school" %in% names(county_data)) {
    county_data$school[county_data$school %in% c(0, 8, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "school")
  }
  if("sex" %in% names(county_data)) {
    county_data$sex[county_data$sex == 9] <- NA
  } else {
    missing_vars <- c(missing_vars, "sex")
  }
  if("statefip" %in% names(county_data)) {
    county_data$statefip[county_data$statefip == 99] <- NA
  } else {
    missing_vars <- c(missing_vars, "statefip")
  }
  if("urban" %in% names(county_data)) {
    county_data$urban[county_data$urban == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "urban")
  }
  if("vetstat" %in% names(county_data)) {
    county_data$vetstat[county_data$vetstat %in% c(0, 9)] <- NA
  } else {
    missing_vars <- c(missing_vars, "vetstat")
  }
  if("vetwwi" %in% names(county_data)) {
    county_data$vetwwi[county_data$vetwwi == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "vetwwi")
  }
  if("vetwwii" %in% names(county_data)) {
    county_data$vetwwii[county_data$vetwwii == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "vetwwii")
  }
  if("wkswork1" %in% names(county_data)) {
    county_data$wkswork1[county_data$wkswork1 == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "wkswork1")
  }
  if("wkswork2" %in% names(county_data)) {
    county_data$wkswork2[county_data$wkswork2 == 0] <- NA
  } else {
    missing_vars <- c(missing_vars, "wkswork2")
  }
  if("yngch" %in% names(county_data)) {
    county_data$yngch[county_data$yngch == 99] <- NA
  } else {
    missing_vars <- c(missing_vars, "yngch")
  }
  print(paste("Missing variables (", paste(county), "):", paste(missing_vars, collapse = ", ")))




  ###########################################################################################
  ### In this section I take categorical variables and turn them into a number of dummies ###
  ###########################################################################################

  county_data <- county_data %>% #figure out how to count citizens
    {if("citizen" %in% names(.)) mutate(.,
      citizen_born_abroad_american_parents = ifelse(citizen == 1, 1, 0),
      citizen_naturalized = ifelse(citizen == 2, 1, 0), 
      citizen_not = ifelse(citizen == 3, 1, 0),
      citizen_not_first_papers = ifelse(citizen == 4, 1, 0),
      citizen_foreign_born_unknown = ifelse(citizen == 5, 1, 0)
    ) else .} %>%
    {if("classwkr" %in% names(.)) mutate(.,
      worker_self_employed = ifelse(classwkr == 1, 1, 0),
      worker_wage_earner = ifelse(classwkr == 2, 1, 0)
    ) else .} %>%
    {if("educ" %in% names(.)) mutate(.,
      no_schooling = ifelse(educ == 0, 1, 0), #includes N/A
      nursery_to_grade4 = ifelse(educ == 1, 1, 0),
      grade5_to_8 = ifelse(educ == 2, 1, 0),
      grade9 = ifelse(educ == 3, 1, 0),
      grade10 = ifelse(educ == 4, 1, 0),
      grade11 = ifelse(educ == 5, 1, 0),
      grade12 = ifelse(educ == 6, 1, 0),
      college_1yr = ifelse(educ == 7, 1, 0),
      college_2yr = ifelse(educ == 8, 1, 0),
      college_3yr = ifelse(educ == 9, 1, 0),
      college_4yr = ifelse(educ == 10, 1, 0),
      college_5plus_yr = ifelse(educ == 11, 1, 0)
    ) else .} %>%
    {if("empstat" %in% names(.)) mutate(.,
      employed = ifelse(empstat == 1, 1, 0),
      unemployed = ifelse(empstat == 2, 1, 0),
      not_in_labor_force = ifelse(empstat == 3, 1, 0)
    ) else .} %>%
    {if("farm" %in% names(.)) mutate(.,
      farm_residence = ifelse(farm == 2, 1, 0)
    ) else .} %>%
    {if("hispan" %in% names(.)) mutate(.,
      not_hispanic = ifelse(hispan == 0, 1, 0),
      hispanic_mexican = ifelse(hispan == 1, 1, 0),
      hispanic_puerto_rican = ifelse(hispan == 2, 1, 0),
      hispanic_cuban = ifelse(hispan == 3, 1, 0),
      hispanic_other = ifelse(hispan == 4, 1, 0)
    ) else .} %>%
    {if("labforce" %in% names(.)) mutate(.,
      in_labor_force = ifelse(labforce == 2, 1, 0)
    ) else .} %>%
    {if("marst" %in% names(.)) mutate(.,
      married_spouse_present = ifelse(marst == 1, 1, 0),
      married_spouse_absent = ifelse(marst == 2, 1, 0),
      separated = ifelse(marst == 3, 1, 0),
      divorced = ifelse(marst == 4, 1, 0),
      widowed = ifelse(marst == 5, 1, 0),
      never_married = ifelse(marst == 6, 1, 0)
    ) else .} %>%
    {if("race" %in% names(.)) mutate(.,
      race_white = ifelse(race == 1, 1, 0),
      race_black = ifelse(race == 2, 1, 0),
      race_native = ifelse(race == 3, 1, 0),
      race_chinese = ifelse(race == 4, 1, 0),
      race_japanese = ifelse(race == 5, 1, 0),
      race_api = ifelse(race == 6, 1, 0), #asian or pacific islander
      race_multiple = ifelse(race > 7, 1, 0)
    ) else .} %>%
    {if("race_black" %in% names(.)) mutate(.,
      black_hh = ifelse(race_black == 1 & related == 101, 1, 0),
      black_hh[relate != 101] <- NA  # Sets black_hh to NA (missing) for anyone who is not the household head (relate != 1)
    ) else .} %>%
    mutate(.,
      hh_employee = ifelse(related >= 1210 & related <= 1219, 1, 0)) %>% 
    #Missing var, TJ
    {if("school" %in% names(.)) mutate(.,
      in_school = ifelse(school == 2, 1, 0)
    ) else .} %>%
    {if("urban" %in% names(.)) mutate(.,
      urban_dummy= ifelse(urban == 2, 1, 0)
    ) else .} %>%
    {if("vetstat" %in% names(.)) mutate(.,
      veteran = ifelse(vetstat == 2, 1, 0)
    ) else .} %>% 
    {if("sex" %in% names(.)) mutate(.,
      fem = ifelse(sex == 2, 1, 0)
    ) else .}

 # home owner
  
  # 4-categories census regions
  # Creates a 4-category census region variable by integer dividing region code by 10
  # For example, if region=31, region_4_cat=3 (South)
  # 1=Northeast, 2=Midwest, 3=South, 4=West
  county_data$region_4_cat <- county_data$region %/% 10
  
  
  wkage_f <- county_data[which(county_data$age >= 18 & county_data$age <= 65 & county_data$fem == 1 & county_data$school != 2), ]
# employment
  print(nrow(wkage_f))

  wkage_f$out_lf_f <- as.numeric(wkage_f$empstat >= 30)
  wkage_f$emp_f <- as.numeric((wkage_f$empstat >= 10 & wkage_f$empstat <20))
  wkage_f$unemp_f <- as.numeric((wkage_f$empstat >= 20 & wkage_f$empstat < 30))
  
  wkage_f$selfemp_f <- as.numeric((wkage_f$classwkr >= 10 & wkage_f$classwkr <20))
  wkage_f$wageemp_f <- as.numeric(wkage_f$classwkr >= 20 & wkage_f$classwkr < 29)

 wkage_f$selfemp_f[wkage_f$empstat > 20] <- 0
 wkage_f$wageemp_f[wkage_f$empstat > 20] <- 0




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
  wkage_f$ag_f[wkage_f$empstat > 20] <- 0
  wkage_f$mining_f[wkage_f$empstat > 20] <- 0
  wkage_f$constr_f[wkage_f$empstat > 20] <- 0
  wkage_f$mfg_f[wkage_f$empstat > 20] <- 0
  wkage_f$transport_f[wkage_f$empstat > 20] <- 0
  wkage_f$wholesale_f[wkage_f$empstat > 20] <- 0
  wkage_f$finance_f[wkage_f$empstat > 20] <- 0
  wkage_f$bus_f[wkage_f$empstat > 20] <- 0
  wkage_f$personal_f[wkage_f$empstat > 20] <- 0
  wkage_f$ent_f[wkage_f$empstat > 20] <- 0
  wkage_f$profserv_f[wkage_f$empstat > 20] <- 0
  wkage_f$public_f[wkage_f$empstat > 20] <- 0

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
  wkage_f$proftech_f[wkage_f$empstat > 20] <- 0
  wkage_f$farmer_f[wkage_f$empstat > 20] <- 0
  wkage_f$mgrs_f[wkage_f$empstat > 20] <- 0
  wkage_f$clerical_f[wkage_f$empstat > 20] <- 0
  wkage_f$sales_f[wkage_f$empstat > 20] <- 0
  wkage_f$craft_f[wkage_f$empstat > 20] <- 0
  wkage_f$oper_f[wkage_f$empstat > 20] <- 0
  wkage_f$service_f[wkage_f$empstat > 20] <- 0
  wkage_f$farmlabor_f[wkage_f$empstat > 20] <- 0
  wkage_f$laborer_f[wkage_f$empstat > 20] <- 0
  # working age women ------------ END
  
  # working age men ------------ START

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
  wkage_m$selfemp_m[wkage_m$empstat > 20] <- 0
  wkage_m$wageemp_m[wkage_m$empstat > 20] <- 0
  
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

  wkage_m$ag_m[wkage_m$empstat > 20] <- 0
  wkage_m$mining_m[wkage_m$empstat > 20] <- 0
  wkage_m$constr_m[wkage_m$empstat > 20] <- 0
  wkage_m$mfg_m [wkage_m$empstat > 20]<- 0
  wkage_m$transport_m[wkage_m$empstat > 20] <- 0
  wkage_m$wholesale_m[wkage_m$empstat > 20] <- 0
  wkage_m$finance_m[wkage_m$empstat > 20] <- 0
  wkage_m$bus_m[wkage_m$empstat > 20] <- 0
  wkage_m$personal_m[wkage_m$empstat > 20] <- 0
  wkage_m$ent_m[wkage_m$empstat > 20] <- 0
  wkage_m$profserv_m[wkage_m$empstat > 20] <- 0
  wkage_m$public_m[wkage_m$empstat > 20] <- 0
  
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
  wkage_m$proftech_m[wkage_m$empstat > 20] <- 0
  wkage_m$farmer_m[wkage_m$empstat > 20] <- 0
  wkage_m$mgrs_m[wkage_m$empstat > 20] <- 0
  wkage_m$clerical_m[wkage_m$empstat > 20] <- 0
  wkage_m$sales_m[wkage_m$empstat > 20] <- 0
  wkage_m$craft_m[wkage_m$empstat > 20] <- 0
  wkage_m$oper_m[wkage_m$empstat > 20] <- 0
  wkage_m$service_m[wkage_m$empstat > 20] <- 0
  wkage_m$farmlabor_m[wkage_m$empstat > 20] <- 0
  wkage_m$laborer_m[wkage_m$empstat > 20] <- 0
  # working age men ------------ END
  
  # recombine subsets
  county_data <- merge(county_data, wkage_f[, c("serial","pernum", grep("_f", names(wkage_f), val=T))], by=c("serial", "pernum") ,all.x = TRUE)
  county_data <- merge(county_data, wkage_m[,c("serial","pernum", grep("_m", names(wkage_m), val=T))], by=c("serial", "pernum"),all.x = TRUE)
 

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
  
  native <- county_data[which(county_data$age >= 18 & county_data$nativity == 1), ]
  
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
  
  # Check if region_4_cat exists before proceeding
  if (!"region_4_cat" %in% names(native)) {
    stop("Error: region_4_cat variable not found in dataset")
  }
  # Creates a variable indicating if someone is a migrant from the Northeast region (region 1)
  # Equals 1 if:
  # 1) Person was born in Northeast (bpl_region == 1) 
  # 2) Currently lives in a different region (bpl_region != region_4_cat)
  # Equals 0 otherwise
  # Note: The error occurs if there are no matches in the data - need to handle NA values
  native$mig_bpl_NE <- as.numeric(native$bpl_region == 1 & native$region_4_cat == 1)
  
  native$mig_bpl_MW <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region == 2)
  native$mig_bpl_S  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region == 3)
  native$mig_bpl_W  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region == 4)
  
  
  native$mig_bpl_NE_black <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 1 & native$race_black==1)
  native$mig_bpl_MW_black <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 2 & native$race_black==1)
  native$mig_bpl_S_black  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 3 & native$race_black==1)
  native$mig_bpl_W_black  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 4 & native$race_black==1)
  native$mig_bpl_NE_white <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 1 & native$race==100)
  native$mig_bpl_MW_white <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 2 & native$race==100)
  native$mig_bpl_S_white  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 3 & native$race==100)
  native$mig_bpl_W_white  <-  as.numeric((native$bpl_region != native$region_4_cat) &  native$bpl_region== 4 & native$race==100)
  
  #reference population: population over age18 who are US born ------- END
  
  # recombine subset
  county_data <- merge(county_data, native[, c("serial", "pernum", grep("bpl_", names(native), val=T))], by=c("serial", "pernum") ,all.x = TRUE)


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

  ##### I don't have a hhtype variable (Missing var, TJ)


  #  if (sum(county_data$hhtype == 9,na.rm=T)>0){
  #    county_data[county_data$hhtype == 9,]$hhtype <- NA}
  
  #  county_data$hhtype_married_both <-  as.numeric(county_data$hhtype == 1)
  #  county_data$hhtype_married_no_w <-  as.numeric(county_data$hhtype == 2)
  #  county_data$hhtype_married_no_h <-  as.numeric(county_data$hhtype == 3)
  #  county_data$hhtype_single_m     <-  as.numeric(county_data$hhtype == 4) #This is single man living alone
  #  county_data$hhtype_single_f     <-  as.numeric(county_data$hhtype == 6) #This is single woman living alone
  
  
  #------------------- (Missing var, TJ)
  #county_data$multgen_3_gen <-as.numeric(county_data$multgen >= 30)
  
  #-------------------
  #Disa March 2021
  #Variable for whether the individual is a 'boarder, lodger, roomer, tenant' in the household
  #https://usa.ipums.org/usa-action/variables/RELATE#codes_section
  county_data$hh_lodger <- as.numeric(county_data$relate >= 1201 & county_data$relate <= 1205)
  

  
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
  county_data$incnonwg[county_data$fem == 1 | county_data$age < 18 | county_data$age > 55] <- NA
  county_data$incnonwg <- county_data$incnonwg - 1 
  
  #migrate5 (Missing var, TJ)
  #Make one dummy for if lived in same house as 5 years ago and one for if moved within the county
  #This should probably just be one per household head. But for now I do this for all over age 24
  #over24$stayed_house <- as.numeric(over24$migrate5 == 10)
  #over24$moved_house  <- as.numeric(over24$migrate5 == 21)
  
  
  # recombine subset
  county_data <- merge(county_data, over24[, c("serial","pernum", grep("_house", names(over24), val=T))], by=c("serial", "pernum"), all.x = TRUE)

  







  return(county_data)
}

# Assign the function output to a variable
hanover_data <- clean_1950_file('New Hanover_NC')



# Print all variable names in the dataset
#cat("\nAll variables in the dataset:\n")
#print(names(hanover_data))

