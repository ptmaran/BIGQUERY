 BEGIN
     SET NOCOUNT ON ;
    DECLARE @SmartAppId UNIQUEIDENTIFIER
	DECLARE @Objectid BIGINT
	DECLARE @DSql nvarchar(max)
	DECLARE @ItemJson nvarchar(max)
	DECLARE @AppZoneId VARCHAR(128)
	SET @SmartAppId=?
	SET @DSQL=?
	SET @AppZoneId=?

	 DECLARE  @Objects TABLE (Objectid BIGINT, ItemJson nVarchar(max) )
	 INSERT INTO @Objects (Objectid)
	 SELEcT  doc.objectid FROM IqFrame_DocumentTypeinstance doc WITH (NOLOCK)
	 JOIN IQFrame_DefinedSmartFolder df WITH (NOLOCK) ON doc.SmartApp=df.ObjectId
	  WHERE df.ID=@SmartappId  and doc.status in (0,2,3)
	  AND (@Objectid Is NULL OR  doc.objectid=@Objectid)

	  SELECT TOP 1 @Objectid=Objectid FROM @Objects ORDER BY Objectid

	  WHILE @Objectid IS NOT NULL
	  BEGIN

	  EXECUTE SP_EXECUTESQL @DSQL, N'@Objectid BIGINT,@AppZoneId VARCHAR(128), @ItemJson nvarchar(max) OUTPUT'
	                          , @Objectid,@AppZoneId, @ItemJson OUTPUT

      UPDATE @Objects SET ItemJson=@ItemJson WHERE Objectid=@Objectid

	   IF EXISTS (SELECT 1 FROM @Objects WHERE Objectid>@Objectid)
		 BEGIN
			 SELECT TOP 1 @Objectid=Objectid FROM @Objects WHERE Objectid>@Objectid ORDER BY Objectid
		 END
		 ELSE
		 BEGIN
			SET @Objectid = NULL
		 END
	  END

	 SELECT ItemJson From @Objects WHERE ItemJson IS NOT NULL

END
