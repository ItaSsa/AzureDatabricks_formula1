# AzureDatabricks_formula1
This repository host the code used to generate Data Analysis on Azure Databricks over the Formula1 data. 
This data was getting by Ergast API (https://ergast.com/mrd/).

# From the Ergast API
## Data model
The Ergast API provided data of Formula1 over this data model:
![Ergast Database Model](utilities/ergast_db.png)

## The data on Azure Account Storage 
We create a Azure Storage account to save user data, and organize it using Medalion Architeture:
![Data in Azure Data Lake](utilities/data_azureDatalake.png)

## Acessing Azure Account Storage from Databricks
In order to access the user data from storage account, we've created a service aplication and used a Key Vault to save the secrets. 
This project has this components on databricks :

![Component architetura on Azure](utilities/project_architeture.png)

## Mounting the paths using Service Authentication
We've create a Scope on Databricks and one function to use a Service Principal authentication to mount the paths. This way, we can use relative paths instead of the whole path using the abfs protocol.

[Mounting Paths](environment)

## Files Ingestion
The ingestion of the raw files had done in separated notebooks. The files have stored in the processed folder. 

[Ingestion folder](ingestion)

## Goals of the tranformations
First goal was to create parquet files that could feed web pages as shown in the next images.


![Drivers Standings](utilities/drivers_standings.png)


![Constructors Standings](utilities/constructors_standings.png)

## The drivers and constructos standing files
First we've grouped the race results over the notebook 1.race_results_file , after that, the parquet files driver_standing and constructor_standing was created using the notebooks 2.transforming_driver_standing and 3.transforming_contructors_standing. We've created the rank of drivers and constructor using Window function. [transformation folder](transformation)

