

BEGIN
	SET NOCOUNT ON ;
	DECLARE @DML nvarchar(max)
	DECLARE @DDL nVarchar(max)
	DECLARE @Data varchar(max)
	DECLARE @Objectid BIGINT
	DECLARE @ProjectName VARCHAR(512)
	DECLARE @AppZoneId VARCHAR(128)
	SET @AppZoneId = ?

	DECLARE @Users TABLE (Objectid BIGINT, ProjectName VARCHAR(512), UserData nvarchar(max))

	DECLARE @SysColumns  TABLE (Id  Bigint IDENTITY (1,1) PRIMARY KEY, TableName Varchar(512)
										, ColumnName Varchar(512), AliasName varchar(512), DataType varchar(128)
										)

	INSERT INTO @SysColumns (TableName, ColumnName, AliasName, DataType)
	VALUES  ('Users','u.Objectid','UserId', 'INTEGER' )
		    ,('Users','u.DisplayName','Name', 'STRING')
			,('Users','u.Email','Email', 'STRING')
			,('Users','u.Phone','Phone', 'STRING')
			,('Users','u.Mobile','Mobile', 'STRING')
			,('Users','u.ThumbnailId','ProfilePicture', 'STRING')
			,('Users','u.Barcode','Barcode', 'STRING')
			,('Users','u.Company','CompanyId', 'STRING')
			,('Users','u.CreatedDate','CreatedDate', 'DATETIME')

 	SELECT @DDL=( SELECT * FROM (	SELECT 'TimeStamp' as [name], 'TIMESTAMP' as [type], 'NULLABLE' as [mode]
									UNION ALL
									SELECT 'AppZoneId' as [name], 'STRING' as [type], 'NULLABLE' as [mode]
									UNION ALL
									SELECT 'ProjectName' as [name], 'STRING' as [type], 'NULLABLE' as [mode]
									UNION ALL
									SELECT  AliasName as [name] , DataType as [type], 'NULLABLE' as [mode]
									FROM @SysColumns WHERE TableName='Users'

								) Users FOR Json PATH , INCLUDE_NULL_VALUES
						)


	----- USERS
	SET @DML = NULL

	INSERT INTO @Users (Objectid, ProjectName )
	SELECT u.ObjectId, psb.Name	FROM IQFrame_ProjectTeamMember pm WITH (NOLOCK)
    	JOIN IQFrame_ProjectSmartFolderBase psb WITH (NOLOCK) ON psb.PMODocument = pm.ParentId AND psb.FolderStatus=1
    	JOIN IQFrame_User u WITH (NOLOCK) on u.objectid=pm.contact AND ISNULL(u.IsContact,0)=0
	WHERE U.Status='Active'


	SET @DML=N' @ProjectName as ProjectName '

	SELECT @DML=CASE WHEN @DML IS NULL THEN N'' ELSE  @DML+N', 'END
	             +ColumnName+N' AS ['+AliasName+N']'
	FROM @SysColumns WHERE TableName='Users'

	SET @DML=N'SELECT '+@DML+N' , GETUTCDATE() as TimeStamp, @AppZoneId as AppZoneId FROM IQFrame_User U WITH (NOLOCK) WHERE  u.Objectid=@Objectid FOR JSON PATH , INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER '

	SET @DML=N'SELECT @Data=('+@DML+N')'

	--PRINT @DML
	SET @Objectid=NULL

	SELECT TOP  1 @Objectid=Objectid, @ProjectName=ProjectName FROM @Users ORDER BY Objectid

	WHILE @Objectid IS NOT NULL
	BEGIN

		EXECUTE SP_EXECUTESQL @DML, N'@Objectid BIGINT, @ProjectName VARCHAR(512), @AppZoneId VARCHAR(128) , @Data varchar(max) OUTPUT', @Objectid, @ProjectName, @AppZoneId,  @Data OUTPUT

		UPDATE @Users SET UserData=@Data WHERE Objectid=@Objectid

		IF EXISTS (SELECT 1 FROM @Users WHERE Objectid>@Objectid )
		BEGIN
			SELECT TOP 1 @Objectid=Objectid, @ProjectName=ProjectName FROM @Users WHERE Objectid>@Objectid ORDER BY Objectid
		END
		ELSE
		BEGIN
			SET @Objectid=NULL
		END

	END

	--- DDL IS Hardcoded in Python
	--SELECT @DDL
	SELECT UserData  FROM @Users ORDER BY  Objectid

END

