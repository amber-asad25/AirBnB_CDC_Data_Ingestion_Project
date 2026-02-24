-- This is needed to setup linked service in ADF pipeline successfully for dedicated SQL pool
-- Create a user for the ADF pipeline
CREATE USER [<your_adf_pipeline_name>] FROM EXTERNAL PROVIDER;

EXEC sp_addrolemember 'db_datareader', [<your_adf_pipeline_name>];
EXEC sp_addrolemember 'db_datawriter', [<your_adf_pipeline_name>];
EXEC sp_addrolemember 'db_owner', '<your_adf_pipeline_name>';
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '***';