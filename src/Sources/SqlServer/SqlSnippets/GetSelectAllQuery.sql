declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()

SELECT
{ChangeTrackingColumnsStatement},
@currentVersion AS 'ChangeTrackingVersion',
lower(convert(nvarchar(128), HashBytes('SHA2_256', {MERGE_EXPRESSION}),2)) as [{MERGE_KEY}]
FROM [{dbName}].[{schema}].[{tableName}] tq
