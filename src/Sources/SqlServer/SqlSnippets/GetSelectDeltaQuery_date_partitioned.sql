﻿declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()

SELECT
{ChangeTrackingColumnsStatement},
@currentVersion AS 'ChangeTrackingVersion',
lower(convert(nvarchar(128), HashBytes('SHA2_256', {MERGE_EXPRESSION}),2)) as [{MERGE_KEY}],
{DATE_PARTITION_EXPRESSION} as [{DATE_PARTITION_KEY}]
FROM [{dbName}].[{schema}].[{tableName}] tq
RIGHT JOIN (SELECT ct.* FROM CHANGETABLE (CHANGES [{dbName}].[{schema}].[{tableName}], {lastId}) ct ) ct ON {ChangeTrackingMatchStatement}
