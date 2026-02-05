-- Script to sync tables from FIMS_Production on linked server to local database
-- It creates tables if they do not exist and inserts new records based on primary keys

-- Set your linked server name and source database
DECLARE @LinkedServer NVARCHAR(128) = N'SQLSERVER';
DECLARE @SourceDB NVARCHAR(128) = N'FIMS_Production';
DECLARE @SourceSchema NVARCHAR(128) = N'dbo';

-- Temp table for primary keys
IF OBJECT_ID('tempdb..#TablePKs') IS NOT NULL DROP TABLE #TablePKs;
CREATE TABLE #TablePKs (
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    OrdinalPosition INT
);

-- Temp table for all tables
IF OBJECT_ID('tempdb..#AllTables') IS NOT NULL DROP TABLE #AllTables;
CREATE TABLE #AllTables (
    TableName NVARCHAR(128)
);


-- Get tables and their PKs from linked server
INSERT INTO #TablePKs
--SELECT * FROM OPENQUERY( [@LinkedServer],
	SELECT
		KU.TABLE_NAME AS TableName,
		KU.COLUMN_NAME AS ColumnName,
		KU.ORDINAL_POSITION
	FROM OPENQUERY( [SQLSERVER], '
	SELECT
		KU.TABLE_NAME,
		KU.COLUMN_NAME,
		KU.ORDINAL_POSITION
	FROM FIMS_Production.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
	INNER JOIN FIMS_Production.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
		ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
	WHERE TC.CONSTRAINT_TYPE = ''PRIMARY KEY''
		AND KU.TABLE_SCHEMA = ''dbo''
	') AS KU;

--SELECT * from @TablePKs

-- Get all unique table names with PKs
INSERT INTO #AllTables
SELECT DISTINCT TableName FROM #TablePKs;

-- Cursor to loop through tables
DECLARE @TableName NVARCHAR(128);
DECLARE @ExistenceCondition NVARCHAR(MAX);
DECLARE @CreateTableSQL NVARCHAR(MAX);
DECLARE @SyncScript NVARCHAR(MAX);
DECLARE @ErrorMessage NVARCHAR(4000);

DECLARE table_cursor CURSOR FOR
SELECT TableName FROM #AllTables;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
        -- 1. Check if table exists locally; if not, create it
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            WHERE t.name = @TableName AND SCHEMA_NAME(t.schema_id) = @SourceSchema
        )
        BEGIN
            -- Generate CREATE TABLE script from linked server
            SET @CreateTableSQL = N'SELECT TOP 0 * INTO ' 
                + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@TableName)
                + N' FROM [' + @LinkedServer + N'].' + QUOTENAME(@SourceDB) + N'.' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@TableName) + N';';

            EXEC sp_executesql @CreateTableSQL;

            INSERT INTO dbo.SyncLog (TableName, Action, Status, Message)
            VALUES (@TableName, 'CREATE TABLE', 'Success', 'Table created successfully.');
        END
        ELSE

		BEGIN
            INSERT INTO dbo.SyncLog (TableName, Action, Status, Message)
            VALUES (@TableName, 'CREATE TABLE', 'Skipped', 'Table already exists.');
        END

        -- 2. Build PK-based existence condition
        SELECT @ExistenceCondition = STRING_AGG(
            'l.' + QUOTENAME(ColumnName) + ' = r.' + QUOTENAME(ColumnName), ' AND '
        )
        FROM #TablePKs
        WHERE TableName = @TableName;

        -- 3. Insert new records only
        SET @SyncScript = N'
            INSERT INTO ' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@TableName) + N'
            SELECT r.* 
            FROM [' + @LinkedServer + N'].' + QUOTENAME(@SourceDB) + N'.' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@TableName) + N' r
            WHERE NOT EXISTS (
                SELECT 1 
                FROM ' + QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@TableName) + N' l 
                WHERE ' + @ExistenceCondition + N'
            );';

        EXEC sp_executesql @SyncScript;

        INSERT INTO dbo.SyncLog (TableName, Action, Status, Message)
        VALUES (@TableName, 'SYNC DATA', 'Success', 'Data synced successfully.');

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        INSERT INTO dbo.SyncLog (TableName, Action, Status, Message)
        VALUES (@TableName, 'ERROR', 'Failed', @ErrorMessage);
    END CATCH

    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

DROP TABLE #TablePKs;
DROP TABLE #AllTables;