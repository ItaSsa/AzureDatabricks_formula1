# AzureDatabricks_formula1
This repository host the code used to generate Data Analysis on Azure Databricks over the Formula1 data getting by Ergast API (https://ergast.com/mrd/).

# From the Ergast API
## Data model
The Ergast API provided data of Formula1 over this data model:
![Ergast Database Model](utilities/ergast_db.png)

## The data on Azure Account Storage 
We create a Azure Storage account to save user data, and organize it using Medalion Architeture:
![Data in Azure Data Lake](utilities/data_azureDatalake.png)

## Acessing Azure Account Storage from Databricks
In order to access the user data from storage account, we've created a service aplication and used a Key Vault to save the secrets. So our project in databricks have this components:
![Component architetura on Azure](utilities/project_architeture.png)


