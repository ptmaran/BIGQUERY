
BEGIN

	SET NOCOUNT ON;

	DECLARE @DML nvarchar(max)
	DECLARE @DDL nVarchar(max)
	DECLARE @Data varchar(max)
	DECLARE @Objectid BIGINT
	DECLARE @Trades VARCHAR(max)
	DECLARE @ProjectName VARCHAR(512)
	DECLARE @AppZoneId VARCHAR(128)
	SET @AppZoneId = ? -- DEBUG

	DECLARE @Companies TABLE (Objectid BIGINT, ProjectName VARCHAR(512), CompanyData nvarchar(max))

	DECLARE @SysColumns  TABLE (Id  Bigint IDENTITY (1,1) PRIMARY KEY, TableName Varchar(512)
										, ColumnName Varchar(512), AliasName varchar(512), DataType varchar(128)
										)

	SELECT @ProjectName=Name FROM IQFrame_ProjectSmartFolderBase WITH (NOLOCK) WHERE FolderStatus=1

	INSERT INTO @SysColumns (TableName, ColumnName, AliasName, DataType)
	VALUES  ('Companies','c.Objectid','CompanyId', 'INTEGER' )
		,('Companies','c.Name','Name', 'STRING')
		,('Companies','c.Color','Color', 'STRING')
		,('Companies','c.ThumbnailID','Thumbnailurl', 'STRING')
		,('Companies','c.WebPage','WebPage', 'STRING')
		,('Companies','c.VendorID','VendorID', 'STRING')
		,('Companies','c.CompanyType','CompanyType', 'INTEGER')
		,('Companies','c.PrimeContractor','PrimeContractor', 'INTEGER')
		,('Companies','c.IsImportedFromOrg','IsImportedFromOrg', 'BOOLEAN')
		,('Companies','c.CreatedDate','CreatedDate', 'DATETIME')

 	SELECT @DDL=( SELECT * FROM (	 SELECT 'TimeStamp' as [name], 'TIMESTAMP' as [type], 'NULLABLE' as [mode]
									 UNION ALL
									 SELECT 'AppZoneId' as [name], 'STRING' as [type], 'NULLABLE' as [mode]
									 UNION ALL
									 SELECT 'ProjectName' as [name], 'STRING' as [type], 'NULLABLE' as [mode]
									 UNION ALL
									 SELECT  AliasName as [name] , DataType as [type], 'NULLABLE' as [mode]
									 FROM @SysColumns WHERE TableName='Companies'
								) Users FOR Json PATH , INCLUDE_NULL_VALUES
						)

   SET  @DDL=JSON_QUERY(@DDL)

   SELECT  @Trades= ( SELECT 'Name' as [name], 'STRING' as [type], 'NULLABLE' as [mode]  FOR Json PATH , INCLUDE_NULL_VALUES)

   SELECT @Trades= (SELECT 'Trades' as [name], 'RECORD' as [type], 'REPEATED' as [mode] ,JSON_QUERY(@Trades) as fields FOR Json PATH , INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER )

   SET @DDL=JSON_MODIFY(@DDL,'append $',JSON_QUERY(@Trades))

	INSERT INTO @Companies (Objectid)
	SELECT c.ObjectId
	FROM IQFrame_Company c WITH (NOLOCK)


	SELECT @DML=CASE WHEN @DML IS NULL THEN N'' ELSE  @DML+N', 'END
	             +ColumnName+N' AS ['+AliasName+N']'
	FROM @SysColumns WHERE TableName='Companies'



	SET @DML=N'  ; WITH tr as (SELECT @Objectid as Objectid,
									 (SELECT  t. NAME as ''Name''
											FROM IQ_ProjectTradeCompanyMapping ct WITH (NOLOCK)
											JOIN IQ_ProjectTrades t WITH (NOLOCK) ON ct.TradeId=t.id
											WHERE ct.CompanyId= @ObjectId
											FOR JSON PATH , INCLUDE_NULL_VALUES
										  )  as Trades
										)
	SELECT @Data=(SELECT  '+@DML+N', tr.Trades as Trades, @ProjectName as ProjectName, GETUTCDATE() as TimeStamp, @AppZoneId as AppZoneId FROM IQFrame_Company c WITH (NOLOCK)
				LEFT JOIN  tr ON tr.Objectid=c.Objectid
				WHERE c.Objectid=@Objectid FOR JSON PATH , INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
				) '


	SET @Objectid=NULL

	SELECT TOP 1 @Objectid=Objectid FROM @Companies ORDER BY Objectid

	WHILE @Objectid IS NOT NULL
	BEGIN

		EXECUTE  SP_EXECUTESQL @DML, N'@Objectid BIGINT, @ProjectName VARCHAR(512), @AppZoneId VARCHAR(128) , @Data varchar(max) OUTPUT', @Objectid, @ProjectName, @AppZoneId ,  @Data OUTPUT

		UPDATE @Companies SET CompanyData=@Data WHERE Objectid=@Objectid

		IF EXISTS (SELECT 1 FROM @Companies WHERE Objectid>@Objectid )
		BEGIN
			SELECT TOP 1 @Objectid=Objectid FROM @Companies WHERE Objectid>@Objectid ORDER BY Objectid
		END
		ELSE
		BEGIN
			SET @Objectid=NULL
		END
	END

	---- COMPANIES DDL HARD CODED IN Python
	-- SELECT @DDL as DDL -- DEBUG
	SELECT CompanyData FROM @Companies ORDER BY  Objectid

END

