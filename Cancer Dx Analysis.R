source("D:/OHDSI/CodeToRunInputs.R")
library(odbc)
library(DBI)
library(dplyr)

Folder <- "D:/OHDSI/NCI"
log <- file.path(getwd(), "errorReport.txt")
ParallelLogger::addDefaultFileLogger(fileName = log)

conn <- dbConnect(odbc(), driver = 'sql server', server = 'nypcdwdbtst1.sis.nyp.org')
cancerTable <- dbGetQuery(conn, "select * from ohdsi_cumc_2020q4r1.results.cancer_types")
head(cancerTable)


Cancers <- c("BreastCancer", 'LungCancer', 'ProstateCancer', 'ColonCancer', 'BrainCancer', 'Lymphoma', 'Leukemia', 'Myeloma')
AncestorIds <- c(4112853, 443388, 4163261, 4180790, 443588, 432571, 317510, 437233)

BreastCancerCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 4112853")
LungCancerCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 443388")
ProstateCancerCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 4163261")
ColonCancerCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 4180790")
BrainCancerCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 443588")
LymphomaCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 432571")
LeukemiaCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 317510")
MyelomaCodes <- dbGetQuery(conn, "select c.concept_id, c.concept_name from ohdsi_cumc_2020q4r1.dbo.concept c 
                           join ohdsi_cumc_2020q4r1.dbo.concept_ancestor ca on c.concept_id=ca.descendant_concept_id
                           where ca.ancestor_concept_id = 437233")

head(cancerTable)


concept_db<-tbl(conn, sql(paste0("SELECT * FROM ", cdmDatabaseSchema, ".concept")))
concept_ancestor_db<-tbl(conn, sql(paste0("SELECT * FROM ", cdmDatabaseSchema, ".concept_ancestor")))

Cancers <- c("BreastCancer", 'LungCancer', 'ProstateCancer', 'ColonCancer', 'BrainCancer', 'Lymphoma', 'Leukemia', 'Myeloma')
AncestorIds <- c(4112853, 443388, 4163261, 4180790, 443588, 432571, 317510, 437233)

library(dplyr)
Cancer_codes<-list()
for (i in 1:length(Cancers)) {
  working.AncestorIds<-AncestorIds[[i]]
  # add all descendants
  working.AncestorIds_w_desc<-concept_ancestor_db %>%
    filter(ancestor_concept_id  %in% working.AncestorIds) %>%
    select(descendant_concept_id) %>%
    rename(concept_id=descendant_concept_id) %>%
    left_join(concept_db) %>%
    select(concept_id, concept_name) %>%
    collect() %>%
    distinct()
  working.AncestorIds_w_desc<-working.AncestorIds_w_desc %>%
    mutate(Cancer=Cancers[[i]])
  #add to list
  Cancer_codes[[i]]<-working.AncestorIds_w_desc
}
Cancer_codes<-bind_rows(Cancer_codes)
head(Cancer_codes)

