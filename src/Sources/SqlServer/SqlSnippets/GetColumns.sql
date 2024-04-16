select
    c.COLUMN_NAME,
    case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey
from
    [{dbName}].INFORMATION_SCHEMA.COLUMNS c
    left join [{dbName}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on c.TABLE_SCHEMA = tc.TABLE_SCHEMA and c.TABLE_NAME = tc.TABLE_NAME
    left join [{dbName}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu on tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME and c.COLUMN_NAME = kcu.COLUMN_NAME
where
    tc.CONSTRAINT_TYPE = N'PRIMARY KEY'
    and tc.TABLE_NAME = N'{table}'
    and tc.TABLE_SCHEMA = N'{schema}'
