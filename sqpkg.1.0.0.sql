--Uninstall
IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_drop_package_dependency'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_drop_package_dependency
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_create_package_dependency'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_create_package_dependency
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_deregister_package_object'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_deregister_package_object
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_register_package_object'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_register_package_object
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_drop_package_object'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_drop_package_object
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_drop_package'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_drop_package
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.procedures
		WHERE NAME = 'sp_create_package'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP PROCEDURE sqpkg.sp_create_package
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE NAME = 'package_object'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP TABLE sqpkg.package_object
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE NAME = 'package_dependency'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP TABLE sqpkg.package_dependency
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE NAME = 'package'
			AND schema_id = SCHEMA_ID('sqpkg')
		)
BEGIN
	DROP TABLE sqpkg.package
END
GO

IF EXISTS (
		SELECT 1
		FROM sys.schemas
		WHERE NAME = 'sqpkg'
		)
BEGIN
	DROP SCHEMA sqpkg
END
GO

--Install
CREATE SCHEMA sqpkg
GO

CREATE TABLE sqpkg.package (
	[id] INT NOT NULL IDENTITY(1, 1) PRIMARY KEY
	,[name] NVARCHAR(256) NOT NULL
	,[schema] SYSNAME NOT NULL
	,[version] NVARCHAR(16) NOT NULL
	,[created_on_date] DATETIME NOT NULL
	,[upgraded_on_date] DATETIME NOT NULL
	,CONSTRAINT [uk_name] UNIQUE ([name])
	)
GO

CREATE TABLE sqpkg.package_dependency (
	[id] INT NOT NULL IDENTITY(1, 1)
	,[package_id] INT NOT NULL FOREIGN KEY REFERENCES sqpkg.package(id)
	,[depends_on_package_id] INT NOT NULL FOREIGN KEY REFERENCES sqpkg.package(id)
	)
GO

CREATE TABLE sqpkg.package_object (
	[package_id] INT NOT NULL FOREIGN KEY REFERENCES sqpkg.package(id)
	,[object_id] INT NOT NULL PRIMARY KEY
	,[name] SYSNAME NOT NULL
	,[sequence] INT NOT NULL
	,[revision] INT NOT NULL
	,
	)
GO

CREATE PROCEDURE sqpkg.sp_create_package (
	@schema SYSNAME
	,@name NVARCHAR(256)
	,@version NVARCHAR(16)
	)
AS
SET NOCOUNT ON

BEGIN TRY
	IF EXISTS (
			SELECT 1
			FROM sqpkg.package
			WHERE NAME = @name
			)
	BEGIN
		RAISERROR (
				'Package %s has already been created or installed in this database'
				,16
				,- 1
				,@name
				)
	END

	BEGIN TRANSACTION

	IF NOT EXISTS (
			SELECT 1
			FROM sys.schemas
			WHERE NAME = @schema
			)
	BEGIN
		DECLARE @sql NVARCHAR(MAX)

		PRINT 'Creating schema ' + @schema

		SET @sql = 'CREATE SCHEMA [' + @schema + ']'

		EXECUTE sp_sqlexec @sql
	END

	INSERT sqpkg.package (
		[schema]
		,[name]
		,[version]
		,[created_on_date]
		,[upgraded_on_date]
		)
	VALUES (
		@schema
		,@name
		,@version
		,GETDATE()
		,GETDATE()
		)

	COMMIT TRANSACTION
END TRY

BEGIN CATCH
	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
	DECLARE @ErrorState INT = ERROR_STATE()

	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRANSACTION
	END

	RAISERROR (
			@ErrorMessage
			,@ErrorSeverity
			,@ErrorState
			)
END CATCH
GO

CREATE PROCEDURE sqpkg.sp_drop_package (@name NVARCHAR(256))
AS
SET NOCOUNT ON

BEGIN TRY
	BEGIN TRANSACTION

	DECLARE @id INT
	DECLARE @schema SYSNAME

	SELECT @id = id
		,@schema = [schema]
	FROM sqpkg.package
	WHERE NAME = @name

	IF @id IS NOT NULL
	BEGIN
		WHILE EXISTS (
				SELECT 1
				FROM sqpkg.package_object
				WHERE package_id = @id
				)
		BEGIN
			DECLARE @package_object_name NVARCHAR(255)

			SELECT TOP 1 @package_object_name = [name]
			FROM sqpkg.package_object
			WHERE package_id = @id
			ORDER BY [sequence] DESC

			EXECUTE sqpkg.sp_drop_package_object @name
				,@package_object_name

			IF @@ERROR <> 0
			BEGIN
				RETURN
			END
		END

		IF NOT EXISTS (
				SELECT 1
				FROM sys.objects
				WHERE [schema_id] = SCHEMA_ID(@schema)
				)
		BEGIN
			DECLARE @sql NVARCHAR(MAX)

			PRINT 'Dropping schema ' + @schema

			SET @sql = 'DROP SCHEMA [' + @schema + ']'

			EXECUTE sp_sqlexec @sql
		END

		DELETE sqpkg.package_dependency
		WHERE package_id = @id

		DELETE sqpkg.package
		WHERE id = @id
	END

	COMMIT TRANSACTION
END TRY

BEGIN CATCH
	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
	DECLARE @ErrorState INT = ERROR_STATE()

	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRANSACTION
	END

	RAISERROR (
			@ErrorMessage
			,@ErrorSeverity
			,@ErrorState
			)
END CATCH
GO

CREATE PROCEDURE sqpkg.sp_register_package_object (
	@package NVARCHAR(256)
	,@name SYSNAME
	,@schema SYSNAME = NULL
	,@revision INT = 0
	,@overwrite BIT = 0
	)
AS
SET NOCOUNT ON

BEGIN TRY
	BEGIN TRANSACTION

	DECLARE @package_id INT
	DECLARE @package_schema SYSNAME
	DECLARE @object_id INT
	DECLARE @sequence INT

	SELECT @package_id = id
		,@package_schema = [schema]
	FROM sqpkg.package
	WHERE [name] = @package

	IF @package_id IS NULL
	BEGIN
		RAISERROR (
				'Package %s does not exist'
				,16
				,- 1
				,@package
				)
	END

	SET @schema = COALESCE(@schema, @package_schema, 'dbo')

	SELECT @object_id = o.[object_id]
	FROM sys.objects o
	WHERE o.[schema_id] = SCHEMA_ID(@schema)
		AND o.[name] = @name
		AND o.is_ms_shipped = 0

	IF @object_id IS NULL
	BEGIN
		RAISERROR (
				'Object %s does not exist in schema %s'
				,16
				,- 1
				,@name
				,@schema
				)
	END

	DECLARE @sql NVARCHAR(MAX)

	IF @package_schema != @schema
	BEGIN
		IF @overwrite = 0
		BEGIN
			IF EXISTS (
					SELECT 1
					FROM sys.objects
					WHERE [schema_id] = SCHEMA_ID(@package_schema)
						AND NAME = @name
					)
			BEGIN
				RAISERROR (
						'%s already exists in schema %s package %s cannot be created or bound to supplied schema, try a different schema or remove the conflicting object'
						,16
						,- 1
						,@name
						,@package_schema
						,@package
						)
			END
		END
		ELSE
		BEGIN
			DECLARE @exists BIT = NULL

			SELECT TOP 1 @sql = 'DROP ' + CASE 
					WHEN o.type IN (
							'PC'
							,'P'
							)
						THEN 'PROCEDURE'
					WHEN o.type IN (
							'FN'
							,'FS'
							,'FT'
							,'IF'
							,'TF'
							)
						THEN 'FUNCTION'
					WHEN o.type IN ('AF')
						THEN 'AGGREGATE'
					WHEN o.type IN ('U')
						THEN 'TABLE'
					WHEN o.type IN ('V')
						THEN 'VIEW'
					END + ' [' + SCHEMA_NAME(schema_id) + '].[' + o.NAME + ']'
				,@exists = 1
			FROM sys.objects o
			WHERE o.[schema_id] = SCHEMA_ID(@package_schema)
				AND o.[name] = @name

			IF @exists = 1
			BEGIN
				PRINT 'Overwriting [' + @package_schema + '].[' + @name + ']'

				EXECUTE sp_sqlexec @sql
			END
		END

		PRINT 'Transfering [' + @schema + '].[' + @name + '] to [' + @package_schema + '].[' + @name + ']'

		SET @sql = 'ALTER SCHEMA [' + @package_schema + '] TRANSFER [' + @schema + '].[' + @name + ']'

		EXECUTE sp_sqlexec @sql
	END

	SELECT @sequence = ISNULL(MAX(po.[sequence]), 0) + 1
	FROM sqpkg.package_object po
	WHERE po.package_id = @package_id

	INSERT sqpkg.package_object (
		package_id
		,[object_id]
		,[name]
		,[sequence]
		,[revision]
		)
	VALUES (
		@package_id
		,@object_id
		,@name
		,@sequence
		,@revision
		)

	COMMIT TRANSACTION
END TRY

BEGIN CATCH
	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
	DECLARE @ErrorState INT = ERROR_STATE()

	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRANSACTION
	END

	RAISERROR (
			@ErrorMessage
			,@ErrorSeverity
			,@ErrorState
			)
END CATCH
GO

CREATE PROCEDURE sqpkg.sp_deregister_package_object (
	@package NVARCHAR(256)
	,@name SYSNAME
	)
AS
SET NOCOUNT ON

DECLARE @package_id INT
DECLARE @object_id INT

SELECT @package_id = p.id
	,@object_id = po.[object_id]
FROM sqpkg.package p
INNER JOIN sqpkg.package_object po ON po.package_id = p.id
	AND po.NAME = @name
WHERE p.NAME = @package

IF @package_id IS NOT NULL
	AND @object_id IS NOT NULL
BEGIN
	DELETE sqpkg.package_object
	WHERE package_id = @package_id
		AND [object_id] = @object_id
END
GO

CREATE PROCEDURE sqpkg.sp_drop_package_object (
	@package NVARCHAR(256)
	,@name SYSNAME
	)
AS
SET NOCOUNT ON

DECLARE @package_id INT
DECLARE @object_id INT
DECLARE @schema AS SYSNAME
DECLARE @of NVARCHAR(16)

SELECT @package_id = p.id
	,@object_id = o.[object_id]
	,@schema = p.[schema]
	,@of = CASE 
		WHEN o.type IN (
				'PC'
				,'P'
				)
			THEN 'PROCEDURE'
		WHEN o.type IN (
				'FN'
				,'FS'
				,'FT'
				,'IF'
				,'TF'
				)
			THEN 'FUNCTION'
		WHEN o.type IN ('AF')
			THEN 'AGGREGATE'
		WHEN o.type IN ('U')
			THEN 'TABLE'
		WHEN o.type IN ('V')
			THEN 'VIEW'
		END
FROM sqpkg.package p
INNER JOIN sqpkg.package_object po ON po.package_id = p.id
	AND po.NAME = @name
INNER JOIN sys.objects o ON o.schema_id = SCHEMA_ID(p.[schema])
	AND o.[object_id] = po.[object_id]
	AND o.is_ms_shipped = 0
WHERE p.NAME = @package

IF @package_id IS NOT NULL
	AND @object_id IS NOT NULL
	AND @of IS NOT NULL
	AND @schema IS NOT NULL
BEGIN
	DECLARE @sql NVARCHAR(MAX) = 'DROP ' + @of + ' [' + @schema + '].[' + @name + ']'

	DELETE sqpkg.package_object
	WHERE [package_id] = @package_id
		AND [object_id] = @object_id

	EXECUTE sp_sqlexec @sql
END
ELSE
BEGIN
	EXECUTE sqpkg.sp_deregister_package_object @package
		,@name
END
GO

CREATE PROCEDURE sqpkg.sp_create_package_dependency (
	@name NVARCHAR(256)
	,@depends_on NVARCHAR(256)
	)
AS
SET NOCOUNT ON

DECLARE @package_id INT
DECLARE @depends_on_package_id INT

SELECT TOP 1 @package_id = id
FROM sqpkg.package
WHERE NAME = @name

SELECT TOP 1 @depends_on_package_id = id
FROM sqpkg.package
WHERE NAME = @depends_on

IF @package_id IS NOT NULL
	AND @depends_on_package_id IS NOT NULL
BEGIN
	INSERT sqpkg.package_dependency (
		[package_id]
		,[depends_on_package_id]
		)
	VALUES (
		@package_id
		,@depends_on_package_id
		)
END
GO

CREATE PROCEDURE sqpkg.sp_drop_package_dependency (
	@name NVARCHAR(256)
	,@depends_on NVARCHAR(256)
	)
AS
SET NOCOUNT ON

DECLARE @package_dependency_id INT

SELECT TOP 1 @package_dependency_id = d.id
FROM sqpkg.package_dependency d
INNER JOIN sqpkg.package pp ON pp.id = d.package_id
	AND pp.NAME = @name
INNER JOIN sqpkg.package dp ON dp.id = d.depends_on_package_id
	AND dp.NAME = @depends_on

IF @package_dependency_id IS NOT NULL
BEGIN
	DELETE package_dependency
	WHERE id = @package_dependency_id
END
GO

EXECUTE sqpkg.sp_create_package 'sqpkg'
	,'SqPkg'
	,'1.0.0'
GO

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'package'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'package_object'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'package_dependency'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_create_package'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_drop_package'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_register_package_object'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_deregister_package_object'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_drop_package_object'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_create_package_dependency'

EXECUTE sqpkg.sp_register_package_object 'SqPkg'
	,'sp_drop_package_dependency'
GO