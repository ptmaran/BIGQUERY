
BEGIN
			SET NOCOUNT ON ;

			DECLARE @SmartAppId UNIQUEIDENTIFIER    --='E1F627E1-5FD9-4FF6-ACEE-7871A4C71E77' --- DEBUG
			DECLARE @ProfileMdColumns VARCHAR(max)  -- = 'fld33fb81d7, fld03e9c62b, fldead45b1c, fld8ef1c984, fld05de2b55, fld5d44dbd7, fldb2aca826, fld43abe575, fld22c71f14, fldac9c4bec'

            SET @SmartAppId = ?     --- DEBUG
            SET @ProfileMdColumns = ?
			CREATE TABLE #FilteredColumns (ColumnName VARCHAR(128))

			INSERT INTO #FilteredColumns(ColumnName)
			SELECT Item FROM IQFrame_Split(@ProfileMdColumns, ',')


			DECLARE @SysColumns nVarchar(max)
			DECLARE @MdcColumns nvarchar(max)
			DECLARE @TableAlias nvarchar(max)
			DECLARE @Joins nvarchar(max)
			DECLARE @DSQL nvarchar(max)
			DECLARE @Contacts nVARCHAR(max)
			DECLARE @ContactFieldId VARCHAR(512)
			DECLARE @ContactColumnName VARCHAR(512)
			DECLARE @ContactAlias VARCHAR(512)
			DECLARE @ContactJson nvarchar(1024)
			DECLARE @Files nVARCHAR(max)
			DECLARE @FileColumnName VARCHAR(512)
			DECLARE @FileAlias VARCHAR(512)
			DECLARE @FileJson nvarchar(1024)
			DECLARE @Grids nVARCHAR(max)
			DECLARE @GridColumnName VARCHAR(512)
			DECLARE @GridTableName VARCHAR(512)
			DECLARE @GridAlias VARCHAR(512)
			DECLARE @GridJson nvarchar(1024)
			DECLARE @GJson nvarchar(max)
			DECLARE @HasScheduleMd BIT = 0
			DECLARE @ScheduleMDResource nVARCHAR(max)
			DECLARE @ScheduleMDSchema nVARCHAR(1024)
			DECLARE @IsCheckListApp BIT =0
			DECLARE @CheckListSchema nVARCHAR(max)
			DECLARE @CheckListResponse nVARCHAR(max)
			DECLARE @MetaDataColumns nvarchar(max)
			DECLARE @MissingMdColumns nvarchar(max)
			DECLARE @NewMDColumns nvarchar(max)


			DECLARE @SmartAppName VARCHAR(512)
			DECLARE @DDL nvARchar(MAX)
			DECLARE @Jsons TABLE(Jsql VARCHAR(max))
			DECLARE @JSON NVARCHAR(MAX)
			DECLARE @SysDDL VARCHAR(max)

			CREATE TABLE #Contacts (ID BIGINT IDENTITY (1,1) PRIMARY KEY,ColumnName VARCHAR(512), FieldId VARCHAR(512), TableAlias VARCHAR(32))
			CREATE TABLE #Files (ID BIGINT IDENTITY (1,1) PRIMARY KEY, ColumnName VARCHAR(512), TableAlias VARCHAR(32))
			CREATE TABLE #Grids (ID BIGINT IDENTITY (1,1) PRIMARY KEY,TableName VARCHAR(512), ColumnName VARCHAR(512), TableAlias VARCHAR(32), DDL VARCHAR(max))


			CREATE TABLE #SmartApps (SmartAppId UNIQUEIDENTIFIER, SmartAppName VARCHAR(512), DML VARCHAR(max), DDL VARCHAR(MAX) , SysDDL VARCHAR(max)
					, MetaDataColumns VARCHAR(max), MissingMdColumns nvarchar(max), NewMDColumns nvarchar(max))
			CREATE TABLE #MetaDataInfo (SmartAppId UNIQUEIDENTIFIER,SmartAppName varchar(256), TableName Varchar(256), ColumnName Varchar(256) , FieldID Varchar(64)
					, FieldLabel Varchar(1024),FieldType Varchar(1024), CollectionName Varchar(1024), ReturnType Varchar(1024),SQLType Varchar(1024), TableAlias VARCHAR(128)
					)
			CREATE TABLE #BQ_SysColumns (Id  Bigint IDENTITY (1,1) PRIMARY KEY, TableName Varchar(512)
										, ColumnName Varchar(512), AliasName varchar(512), DataType varchar(128)
										)

			CREATE TABLE #ScheduleMDColumns (Id BIGINT IDENTITY(1,1) PRIMARY KEY, ColumnName VARCHAR(512), AliasName VARCHAR(512), DataType VARCHAR(128))
			CREATE TABLE #CheckListResponseColumns (Id BIGINT IDENTITY(1,1) PRIMARY KEY,  AliasName VARCHAR(512), DataType VARCHAR(128))


 			INSERT INTO #BQ_SysColumns (TableName, ColumnName, AliasName, DataType)
			VALUES  ('SmartApp','doc.Objectid','ItemId', 'Bigint' )
					,('SmartApp','doc.Id','UniqueId', 'Varchar(45)')
					,('SmartApp','doc.Name','Name', 'Varchar(256)')
					,('SmartApp','doc.Description','Description', 'Varchar(4000)')
					,('SmartApp','doc.createdby','CreatedById', 'BIGINT')
					,('SmartApp','cusr.DisplayName','CreatedBy', 'Varchar(200)')
					,('SmartApp','doc.CreatedDate','CreatedDate', 'DateTime')
					,('SmartApp','doc.ModifiedBy','ModifiedById', 'BIGINT')
					,('SmartApp','musr.DisplayName','ModifiedBy', 'Varchar(200)')
					,('SmartApp','doc.ModifiedDate','ModifiedDate', 'DateTime')
					,('SmartApp','doc.CenterLatitude','CenterLatitude', 'Varchar(256)')
					,('SmartApp','doc.CenterLongitude','CenterLongitude', 'Varchar(256)')
					,('SmartApp','doc.Street','Street', 'Varchar(256)')
					,('SmartApp','doc.Zip','Zip', 'Varchar(256)')
					,('SmartApp','doc.County','County', 'Varchar(256)')
					,('SmartApp','doc.State','State', 'Varchar(256)')
					,('SmartApp','doc.City','City', 'Varchar(256)')
					,('SmartApp','doc.Country','Country', 'Varchar(256)')
					,('SmartApp','doc.Stage','Stage', 'Bigint')
					,('SmartApp','doc.StageName','StageName', 'Varchar(200)')
					,('SmartApp','doc.Status','Status', 'int')
					,('SmartApp','doc.path','Path', 'VARCHAR(4000)')
					,('SmartApp','doc.ProjectBase','ProjectId', 'Bigint')
					,('SmartApp','doc.DocumentType','DocumentType', 'Bigint')
					,('SmartApp','doc.FileName','FileName', 'Varchar(128)')
					,('SmartApp','doc.ExtendedGPSData','ExtendedGPSData', 'Varchar(4000)')
					,('SmartApp','doc.LocationAccuracy','LocationAccuracy', 'Decimal(18,10)' )
					,('SmartApp','doc.SmartApp','SmartAppId', 'BIGINT')
					,('SmartApp','dsf.Name','SmartAppName','Varchar(128)')
					,('SmartApp','doc.SmartAppInstance','SmartAppInstanceId', 'BIGINT')
					,('SmartApp','doc.Issubitem','Issubitem', 'BIT')
					,('SmartApp','doc.ParentId','ParentId', 'BIGINT')
					,('SmartApp','doc.version','Version', 'INT')
					,('SmartApp','doc.Revision','Revision','INT')


			INSERT INTO #ScheduleMDColumns (ColumnName,AliasName, DataType)
			VALUES ('tr.StartDate','ScheduleMD_StartDate','DATETIME')
				   , ('tr.EndDate','ScheduleMD_EndDate','DATETIME')
				   , ('tr.EstimatedWorkTime','ScheduleMD_EstimatedWorkTime','NUMERIC')
				   , ('CAST(tr.PercentComplete as DECIMAL(10,2))','ScheduleMD_PercentComplete','NUMERIC')
				   , ('tr.OrganizationId','ScheduleMD_OrganizationId','INTEGER')
				   , ('tr.OrganizationName','ScheduleMD_OrganizationName','STRING')
				   , ('tr.ActualStartDate','ScheduleMD_ActualStartDate','DATETIME')
				   , ('tr.ActualEndDate','ScheduleMD_ActualEndDate','DATETIME')
				   , ('tr.ActualWorkTime','ScheduleMD_ActualWorkTime','NUMERIC')


			INSERT INTO #CheckListResponseColumns (AliasName, DataType)
			VALUES ('TemplateId','INT64')
				   , ('TemplateName','STRING')
				   , ('SectionId','STRING')
				   , ('SectionName','STRING')
				   , ('SectionTitle','STRING')
				   , ('QuestionId','STRING')
				   , ('Question','STRING')
				   , ('Type','STRING')
				   , ('Response','STRING')
				   , ('Comment','STRING')
				   , ('QuestionIndex','INT64')
				   , ('StreamId','STRING')





			SET @CheckListResponse=N' CheckList_Responses= ( SELECT  * FROM
										(	 SELECT cv.Objectid as  TemplateId, ISNULL(cv.Name, ct.name) as TemplateName
																, cq.QuestionUId as  QuestionId, cq.Question , cq.Type,  ISNULL(r.Response,qo.OptionValue)  as Response
																, r.Comment, cq.QuestionCounter as QuestionIndex
																, qimg.StreamId as streamid
																,cq.SectionId, cq.SectionName, cq.SectionTitle
								FROM IQFrame_CheckListItemsDetails cd WITH (NOLOCK)
								LEFT JOIN IQFrame_ChecklistTemplateversions cv WITH (NOLOCK)   on cd.TemplateVersionId=cv.Objectid
								LEFT JOIN IQFrame_ChecklistTemplates ct WITH (NOLOCK)   ON ct.Objectid=cv.parentid
								LEFT JOIN IQFrame_CheckListQuestions cQ WITH (NOLOCK) ON cd.TemplateVersionId=cq.TemplateVersionId
								LEFT JOIN IQFrame_CheckListItemResponses r WITH (NOLOCK) ON r.ItemID=cd.ItemId AND r.QuestionId=cQ.ID
								LEFT JOIN IQFrame_CheckListItemOptionResponseMapping rm WITH (NOLOCK) ON rm.ResponseId=r.Id
								LEFT JOIN IQFrame_CheckListQuestionOptions qo WITH (NOLOCK) ON qo.Questionid=cQ.ID AND rm.OptionId=qo.id
								LEFT JOIN IQFrame_CheckListQuestionImages qimg on qimg.OptionId=qo.Id and qimg.QuestionId=qo.QuestionId

								WHERE cd.ItemId=@Objectid

								) x FOR JSON PATH , INCLUDE_NULL_VALUES  ) '




			INSERT INTO #SmartApps (SmartAppId, SmartAppName)
			SELECT  dsf.ID,dsf.Name
			FROM IQFrame_DefinedSmartFolder dsf WITH (NOLOCK)
			WHERE dsf.StatusType  in (0,3)
			AND dsf.name not like 'sample%'
			AND dsf.Name NOT LIKE '%PMO'
			AND (@SmartAppId IS null OR dsf.ID=@SmartAppId)
			AND dsf.Name NOT in ('Cabinet SmartFolder')
--			AND dsf.ID  IN ('E1F627E1-5FD9-4FF6-ACEE-7871A4C71E77')  -- DEBUG


           SELECT TOP 1 @SmartAppId=SmartAppId, @SmartAppName=SmartAppName FROM #SmartApps  ORDER BY SmartAppId

			WHILE @SmartAppId IS NOT NULL
			BEGIN

					SET @HasScheduleMd=0
					SET @IsCheckListApp=0
					DELETE FROM #MetaDataInfo
					DELETE FROM #Contacts
					DELETE FROM #Files
					DELETE FROM #Grids

					IF EXISTS (SELECT 1
									FROM IQFrame_DefinedSmartFolder dsf WITH (NOLOCK)
									JOIN IQStudio_SmartApps sa WITH (NOLOCK) ON sa.AppGuid=dsf.ID
									WHERE dsf.Id=@Smartappid AND sa.Type='CheckList'
								)
					BEGIN
						SET @IsCheckListApp=1


					END

					INSERT INTO #MetaDataInfo(SmartAppId,SmartAppName, TableName, ColumnName,FieldID,  FieldType, FieldLabel, CollectionName,ReturnType)
					SELECT sf.ID, sf.Name
						,Def.Name as TableName,Info.PropName ColumnName
						,Info.UniqueId
						,Info.FieldType, Info.FieldLabel, ver.Label AS CollectionName
						,Info.ReturnType
					FROM IQFrame_Definition Def WITH (NOLOCK)
					JOIN [IQFrame_MetadataDefinitions] MDef on Def.ObjectId=Mdef.ReferenceId
					JOIN IQFrame_DefinitionVersion Ver WITH (NOLOCK) on Def.ObjectId = Ver.ParentId and Def.ActiveVersion=Ver.ObjectId
					JOIN IQFrame_MetaDataInfo  Info WITH (NOLOCK) on Info.TableName=Def.Name
					join IQFrame_DocumentTypeDefinitionVersion dtav ON  dtav.objectid=Mdef.objectid
					join IQFrame_DocumentTypeDefinition dtd on dtd.activeversion=dtav.objectid
					join IQFrame_DefinedSmartItem dsi ON dsi.documenttype=dtd.objectid
					JOIN [IQFrame_DefinedSmartFolder] sf ON sf.DefinedSmartItem=dsi.objectid
					WHERE info.PropName not in ('ObjectId','ParentId')
					and Info.PropName not like '%pkey'
					and Info.PropName not like '%@id'
					and Info.PropName not like '%@val'
					and Info.PropName not like '%@stat'
					and Info.PropName not like '%@Json'
					and Info.PropName not like '%IQKEY'
					AND sf.StatusType  in (0,3)
					AND  ( sf.ID=@SmartAppId)

	--SELECT * FROM #MetaDataInfo
	--select * from #FilteredColumns

					SET @MissingMdColumns=NULL

					SELECT @MissingMdColumns = CASE WHEN @MissingMdColumns IS NULL THEN N'' ELSE @MissingMdColumns+N', ' END + f.ColumnName
					FROM #FilteredColumns f
					LEFT JOIN  #MetaDataInfo m ON m.ColumnName=f.ColumnName
					WHERE m.ColumnName IS NULL

					SET @NewMDColumns=NULL

					SELECT @NewMDColumns = CASE WHEN @NewMDColumns IS NULL THEN N'' ELSE @NewMDColumns+N', ' END + m.ColumnName
					FROM #MetaDataInfo m
					LEFT JOIN  #FilteredColumns f ON m.ColumnName=f.ColumnName
					WHERE f.ColumnName IS NULL

				  DELETE m
				  FROM #MetaDataInfo m
				  LEFT JOIN #FilteredColumns f ON f.ColumnName=m.ColumnName
				  WHERE  (@ProfileMdColumns IS NOT NULL AND f.ColumnName IS NULL)


				  UPDATE #MetaDataInfo SET SQLType='STRING'  WHERE ReturnType IN ('VariableCharacter','Text','VariableCharacterMAX')
				  UPDATE #MetaDataInfo SET SQLType='INTEGER'  WHERE ReturnType IN ('BigInteger','TinyInteger')
				  UPDATE #MetaDataInfo SET SQLType='BOOLEAN'  WHERE ReturnType='Boolean'
				  UPDATE #MetaDataInfo SET SQLType='DATETIME'  WHERE ReturnType='DateTime'
				  UPDATE #MetaDataInfo SET SQLType='Numeric'  WHERE ReturnType='Numeric'
				  UPDATE #MetaDataInfo SET SQLType='Numeric'    WHERE ReturnType like 'Decimal%'

				  IF EXISTS (SELECT 1 FROM #MetaDataInfo WHERE FieldType='ScheduleData')
				  BEGIN
					SET @HasScheduleMd=1

					SET  @ScheduleMDResource = N'ScheduleMD_Resources= (	SELECT  * FROM  (
										 SELECT  r.ResourceId, CASE WHEN r.WorkTeamId IS NOT NULL THEN ISNULL(wt.Name,'''')+'' : '' +u.DisplayName ELSE u.DisplayName END as Name,  u.id as UniqueId
										 FROM IQ_ProjectScheduleResources r WITH (NOLOCK)
										 JOIN IQFrame_user u WITH (NOLOCK) ON r.ResourceId=u.objectid  AND ISNULL(u.IsContact,0)=0
										 LEFT JOIN IQFrame_Company c WITH (NOLOCK) ON c.Objectid =u.Company
										 LEFT JOIN IQ_WorkPlannerTeams wt WITH (NOLOCK) ON wt.id=r.WorkTeamId
										  WHERE r.Taskid=@Objectid
										  UNION ALL
										  SELECT  r.ResourceId, CASE WHEN r.WorkTeamId IS NOT NULL THEN ISNULL(wt.Name,'''')+'' : '' +c.Name ELSE c.Name END as Name,  c.id as uniqueId
										  FROM IQ_ProjectScheduleResources r WITH (NOLOCK)
										  JOIN IQFrame_Company c WITH (NOLOCK) ON r.ResourceId=c.objectid
										  LEFT JOIN IQ_WorkPlannerTeams wt WITH (NOLOCK) ON wt.id=r.WorkTeamId
										  WHERE r.Taskid=@Objectid
										  UNION ALL
										  SELECT  r.ResourceId, '''' as Name,  u.id as uniqueId
										  FROM IQ_ProjectScheduleResources r WITH (NOLOCK)
										  JOIN IQFrame_ProjectGroup u WITH (NOLOCK) ON r.ResourceId=u.objectid
										  WHERE r.Taskid=@Objectid
										  UNION ALL
										  SELECT   r.ResourceId,  ISNULL(t.Name,'''')+'' : '' + ISNULL(c.Name,'''') as [value],  u.id as uniqueId
										  FROM IQ_ProjectScheduleResources r WITH (NOLOCK)
										  JOIN IQFrame_UserGroup u WITH (NOLOCK) ON r.ResourceId=u.objectid
										  JOIN IQ_WorkPlannerTeams t WITH (NOLOCK) ON u.ObjectId=t.AssociatedUserGroup
										  LEFT JOIN IQFrame_Company c WITH (NOLOCK) ON t.CompanyId=c.Objectid
										  WHERE r.Taskid=@Objectid
										 )  x FOR JSON PATH , INCLUDE_NULL_VALUES

										)  '
				  END

				  ; WITH info as (SELECT DENSE_RANK() OVER(ORDER BY Tablename) as rowno, TableName FROM #MetaDataInfo )

				  UPDATE i SET i.TableAlias='MD'+CAST(t.rowno as VARCHAR(128))
				  FROM #MetaDataInfo  i
				  JOIN info t ON i.TableName=t.TableName



				  DELETE FROM #Contacts
				  INSERT INTO #Contacts (FieldId, ColumnName)
				  SELECT DISTINCT FieldID, ColumnName 	FROM #MetaDataInfo WITH (NOLOCK) WHERE FieldType='contact'

				  UPDATE #Contacts SET TableAlias='MDC'+CAST(ID as VARCHAR(10))


				  DELETE FROM #Files
				  INSERT INTO #Files (ColumnName)
				  SELECT DISTINCT ColumnName 	FROM #MetaDataInfo WITH (NOLOCK) WHERE FieldType='file'

				  UPDATE #Files SET TableAlias='MDF'+CAST(ID as VARCHAR(10))


				  INSERT INTO #Grids (TableName, ColumnName)
				  SELECT DISTINCT TableName, ColumnName 	FROM #MetaDataInfo WITH (NOLOCK) WHERE FieldType='grid'

				  UPDATE #Grids SET TableAlias='IQGD'+CAST(ID as VARCHAR(10))



				  SET @ContactFieldId = NULL

					SELECT TOP 1 @ContactFieldId=FieldId, @ContactColumnName=ColumnName, @ContactAlias=TableAlias FROM #Contacts ORDER BY FieldId
					WHILE @ContactFieldId IS NOT NULL
					BEGIN

						IF @Contacts IS NULL
						BEGIN
							SET @Contacts=N'  '
						END
						ELSE
						BEGIN
							SET @Contacts=@Contacts+N', '
						END

						SET @Contacts=@Contacts+@ContactAlias+N' as ( SELECT @Objectid as Objectid, (SELECT COALESCE( u.Objectid, c.Objectid) as Id, COALESCE(u.DisplayName, c.Name) as Name
													FROM IQFrame_MetadataContacts mdc WITH (NOLOCK)
													LEFT JOIN IQFrame_User u WITH (NOLOCK) ON u.objectid=mdc.contactid
													LEFT JOIN IqFrame_Company c WITH (NOLOCK) ON mdc.Contactid=c.Objectid
													WHERE mdc.fieldid='''+@ContactFieldId+N'@IQKEY'''+N' AND mdc.itemid=@Objectid  FOR JSON PATH , INCLUDE_NULL_VALUES ) as '''+@ContactColumnName+N''' ) '

						IF EXISTS (SELECT 1 FROM #Contacts WHERE FieldId>@ContactFieldId)
						BEGIN
							SELECT TOP 1 @ContactFieldId=FieldId, @ContactColumnName=ColumnName, @ContactAlias=TableAlias FROM #Contacts WHERE FieldId>@ContactFieldId ORDER BY FieldId
						END
						ELSE
						BEGIN

							SET @ContactFieldId=NULL
						END
					END


					SET @FileColumnName = NULL

					SELECT TOP 1 @FileColumnName=ColumnName, @FileAlias=TableAlias FROM #Files ORDER BY ColumnName
					WHILE @FileColumnName IS NOT NULL
					BEGIN

						IF @Files IS NULL
						BEGIN
							SET @Files=N' '
						END
						ELSE
						BEGIN
							SET @Files=@Files+N', '
						END

						SET @Files=@Files+@FileAlias+N' as ( SELECT @Objectid as Objectid, (SELECT  c.Objectid as Id, c.Name , c.MD5, c.StreamId,sr.ImageURL
																FROM IQFrame_FileMetaDataCollection mdc WITH (NOLOCK)
																JOIN IQFrame_Content c WITH (NOLOCK) ON c.objectid=mdc.ContentId
																LEFT JOIN IQFrame_StreamImageRepo sr WITH (NOLOCK) ON sr.StreamID=c.StreamID AND sr.Size=5
													WHERE mdc.fieldid='''+@FileColumnName+N''''+N' AND mdc.Parentid=@Objectid  FOR JSON PATH , INCLUDE_NULL_VALUES ) as '''+@FileColumnName+N''' ) '
						IF EXISTS (SELECT 1 FROM #Files WHERE ColumnName>@FileColumnName)
						BEGIN
							SELECT TOP 1 @FileColumnName=ColumnName, @FileAlias=TableAlias FROM #Files WHERE ColumnName>@FileColumnName ORDER BY ColumnName
						END
						ELSE
						BEGIN

							SET @FileColumnName=NULL
						END
					END


					SET @GridColumnName = NULL

					SELECT TOP 1 @GridColumnName=ColumnName, @GridAlias=TableAlias, @GridTableName=TableName FROM #Grids ORDER BY ColumnName
					WHILE @GridColumnName IS NOT NULL
					BEGIN

						IF @Grids IS NULL
						BEGIN
							SET @Grids=N' '
						END
						ELSE
						BEGIN
							SET @Grids=@Grids+N', '
						END



						SET @GJson=NULL

						SELECT @GJson=CASE WHEN @GJson IS NULL THEN N'' ELSE @GJson+N', ' END +N' IQGD.'+i.COLUMN_NAME
						FROM INFORMATION_SCHEMA.COLUMNS i
						WHERE i.TABLE_NAME='IQGD_'+@GridTableName+'_'+@GridColumnName
						AND i.COLUMN_NAME NOT in ('Objectid','Parentid')

						SET @GJson= N' as ( SELECT @Objectid as Objectid, (SELECT '+@GJson+N' FROM IQGD_'+@GridTableName+'_'+@GridColumnName
								+ N' as IQGD  WHERE IQGD.Objectid=@Objectid FOR JSON PATH , INCLUDE_NULL_VALUES ) as '
								+ @GridColumnName +N')'


						SET @Grids=@Grids+@GridAlias+@GJson

						SET @GridJson = NULL
						SELECT @GridJson=CASE WHEN @GridJson IS NULL THEN N'' ELSE @GridJson+N' UNION ALL ' END
												+ N' SELECT '''+COLUMN_NAME +N''' as [name], '
												+ N''''+CASE WHEN DATA_TYPE IN ('datetime') THEN 'DATETIME'
														WHEN DATA_TYPE IN ('numeric') THEN 'NUMERIC'
														WHEN DATA_TYPE IN ('bit') THEN 'BOOLEAN'
												       WHEN DATA_TYPE IN ('int','bigint','tinyint') THEN 'INT64'
													   ELSE 'STRING' END + N''' as [type], ''NULLABLE'' as [mode] '
						FROM INFORMATION_SCHEMA.COLUMNS I
						 WHERE i.TABLE_NAME='IQGD_'+@GridTableName+'_'+@GridColumnName
						AND i.COLUMN_NAME NOT in ('Objectid','Parentid')


						SET @GridJson=N' SELECT @GJson =( SELECT * FROM ( '+ @GridJson + N' ) x FOR Json Path )'


						EXECUTE SP_EXECUTESQL @GridJson, N'@GJson varchar(max) OUTPUT', @GJson OUTPUT


						UPDATE #Grids SET DDL=@GJson

						IF EXISTS (SELECT 1 FROM #Grids WHERE ColumnName>@GridColumnName)
						BEGIN
							SELECT TOP 1 @GridColumnName=ColumnName, @GridAlias=TableAlias, @GridTableName=TableName FROM #Grids WHERE ColumnName>@GridColumnName ORDER BY ColumnName
						END
						ELSE
						BEGIN

							SET @GridColumnName=NULL
						END
					END




					SET @Joins=NULL
					SET @DSQL=NULL

						SET @MdcColumns=NULL
						SET @Joins = N' FROM IQFrame_DocumentTypeinstance Doc WITH (NOLOCK)  '
									+N' LEFT JOIN IQFrame_DefinedSmartFolder dsf WITH (NOLOCK) ON doc.SmartApp=dsf.objectid '
									+N' LEFT JOIN IQFrame_ProjectSmartFolderBase psf WITH (NOLOCK) ON doc.projectbase=psf.objectid '
									+N' LEFT JOIN IQFrame_user cusr WITH (NOLOCK) ON doc.createdby=cusr.objectid'
									+N' LEFT JOIN IQFrame_user musr WITH (NOLOCK) ON doc.modifiedby=musr.objectid'
									+N' LEFT JOIN IQ_ProjectScheduleTasks tr WITH (NOLOCK) ON tr.Id=doc.Objectid '


						; WITH mdcs as (SELECT DISTINCT TableName, TableAlias
										FROM #MetaDataInfo WITH (NOLOCK)
										WHERE FieldType NOT in ('contact','file','grid')
										)

						SELECT @Joins=@Joins +N' LEFT JOIN ['+TableName+N'] as '+TableAlias + N' ON doc.Objectid = '+TableAlias+N'.Objectid'
						FROM mdcs


						SELECT @Joins=@Joins+N' LEFT JOIN '+TableAlias+N' ON '+TableAlias+N'.Objectid=doc.Objectid '
						FROM #Contacts c

						SELECT @Joins=@Joins+N' LEFT JOIN '+TableAlias+N' ON '+TableAlias+N'.Objectid=doc.Objectid '
						FROM #Files c

						SELECT @Joins=@Joins+N' LEFT JOIN '+TableAlias+N' ON '+TableAlias+N'.Objectid=doc.Objectid '
						FROM #Grids c


						SELECT @ContactJson=( SELECT * FROM (
												SELECT 'Id' as name, 'INT64' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'Name' as name, 'STRING' as type, 'NULLABLE' as mode

												) x
												FOR Json Path
											)

						SELECT @FileJson=( SELECT * FROM (
												SELECT 'Id' as name, 'INT64' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'Name' as name, 'STRING' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'StreamId' as name, 'STRING' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'ImageURL' as name, 'STRING' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'MD5' as name, 'STRING' as type, 'NULLABLE' as mode
												) x
												FOR Json Path
											)
						SELECT @ScheduleMDSchema=( SELECT * FROM (
												SELECT 'ResourceId' as name, 'INT64' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'Name' as name, 'STRING' as type, 'NULLABLE' as mode
												UNION ALL
												SELECT 'UniqueId' as name, 'STRING' as type, 'NULLABLE' as mode
												) x
												FOR Json Path
											)

						--SET @SysDDL = NULL
						SELECT @SysDDL=( SELECT * FROM (
									SELECT 'TimeStamp' as name, 'TIMESTAMP' as type, 'NULLABLE' as mode
									UNION ALL
									SELECT 'AppZoneId' as name, 'STRING' as type, 'NULLABLE' as mode
									UNION ALL
									SELECT 'ProjectName' as name, 'STRING' as type, 'NULLABLE' as mode
									UNION ALL
									SELECT  AliasName as [name] , CASE WHEN DataType like 'VARCHAR%' THEN 'STRING'
																							WHEN DataType in ('BIGINT','INT') THEN 'INTEGER'
																							WHEN DataType like 'Decimal%' THEN 'NUMERIC'
																							WHEN DataType='BIT' THEN 'BOOLEAN'
																							ELSE DataType END  as [type]
												, 'NULLABLE' as [mode]
									FROM #BQ_SysColumns WHERE TableName='SmartApp'

										) x  FOR Json Path

								)


						INSERT INTO @Jsons(Jsql)
						SELECT (
						SELECT * FROM (
						--SELECT 'ProfileId' as name, 'INTEGER' as type, 'NULLABLE' as mode
						--UNION ALL
						SELECT 'TimeStamp' as name, 'TIMESTAMP' as type, 'NULLABLE' as mode
						UNION ALL
						SELECT 'AppZoneId' as name, 'STRING' as type, 'NULLABLE' as mode
						UNION ALL
						SELECT 'ProjectName' as name, 'STRING' as type, 'NULLABLE' as mode
						UNION ALL
						 SELECT  AliasName as [name] , CASE WHEN DataType like 'VARCHAR%' THEN 'STRING'
																				WHEN DataType in ('BIGINT','INT') THEN 'INTEGER'
																				WHEN DataType like 'Decimal%' THEN 'NUMERIC'
																				WHEN DataType='BIT' THEN 'BOOLEAN'
																				ELSE DataType END  as [type]
									, 'NULLABLE' as [mode]
						FROM #BQ_SysColumns WHERE TableName='SmartApp'
						UNION ALL
						SELECT s.AliasName as [name], s.DataType  as [type], 'NULLABLE'  as [mode]
						FROM #ScheduleMDColumns s
						WHERE @HasScheduleMd=1
						UNION ALL
						SELECT REPLACE(ColumnName,'@','_') as [name], SQLType  as [type], 'NULLABLE'  as [mode]
						FROM #MetaDataInfo i  WHERE FieldType NOT IN  ('Contact', 'File','grid','ScheduleData')
						 ) sysdata
						 FOR Json PATH , INCLUDE_NULL_VALUES
						 )



						SET @FileColumnName = NULL

						SELECT TOP 1 @FileColumnName=ColumnName FROM #MetaDataInfo  WHERE  FieldType IN  ('Contact', 'File','grid','ScheduleData' ) ORDER BY ColumnName

						WHILE @FileColumnName IS NOT NULL
						BEGIN

						        SELECT @GridJson=DDL FROM #Grids WHERE ColumnName=@FileColumnName

								SELECT @Json=( SELECT  CASE WHEN m.FieldType='ScheduleData' THEN N'ScheduleMD_Resources' ELSE m.ColumnName  END as  name
													    , 'RECORD' as [type] ,'REPEATED'  as [mode]
														, CASE WHEN m.FieldType='Contact' THEN JSON_QUERY(@ContactJson)
															   WHEN m.FieldType='File' THEN JSON_QUERY(@FileJson)
															  WHEN m.FieldType='grid' THEN JSON_QUERY(@GridJson)
															  WHEN m.FieldType='ScheduleData' THEN JSON_QUERY(@ScheduleMDSchema)
																								 END as fields
													FROM #MetaDataInfo m WITH (NOLOCK)
													WHERE m.ColumnName=@FileColumnName
													FOR JSON PATH, INCLUDE_NULL_VALUES , WITHOUT_ARRAY_WRAPPER
											)

								UPDATE @Jsons SET JSQL=JSON_MODIFY(JSQL,'append $',JSON_QUERY(@Json))

								IF EXISTS (SELECT 1 FROM #MetaDataInfo  WHERE  FieldType IN  ('Contact', 'File','grid', 'ScheduleData')  AND ColumnName>@FileColumnName )
								BEGIN
									SELECT TOP 1 @FileColumnName=ColumnName FROM #MetaDataInfo  WHERE  FieldType IN  ('Contact', 'File','grid', 'ScheduleData')  AND ColumnName>@FileColumnName ORDER BY ColumnName
								END
								ELSE
								BEGIN

									SET @FileColumnName=NULL
								END

						END

						SET @SysColumns=N' GETUTCDATE() as TimeStamp , @AppZoneID as AppZoneId, psf.Name as ProjectName'
						 SELECT  @SysColumns= CASE WHEN @SysColumns IS NULL THEN N'' ELSE @SysColumns + N', ' END
								   +ColumnName+ N' As  ['+AliasName+N']'
						 FROM #BQ_SysColumns WHERE TableName='SmartApp'

						 IF @HasScheduleMd=1
						 BEGIN
								  SELECT  @SysColumns= CASE WHEN @SysColumns IS NULL THEN N'' ELSE @SysColumns + N', ' END
										   +ColumnName+ N' As  ['+AliasName+N']'
								 FROM #ScheduleMDColumns
						 END



				IF @Contacts IS NOT NULL
				BEGIN
					SET @DSQL = N' ; WITH '+@Contacts
				END

				IF @Files IS NOT NULL
				BEGIN
					IF @DSQL IS NOT NULL
					BEGIN
						SET @DSQL=@DSQL+N' , '+@Files
					END
					ELSE
					BEGIN

						SET @DSQL=N' ; WITH '+@Files
					END
				END
				IF @Grids IS NOT NULL
				BEGIN
					IF @DSQL IS NOT NULL
					BEGIN
						SET @DSQL=@DSQL+N' , '+@Grids
					END
					ELSE
					BEGIN

						SET @DSQL=N' ; WITH '+@Grids
					END
				END


				 SET @MdcColumns= @SysColumns


				SELECT @MdcColumns=@MdcColumns
									+CASE WHEN m.FieldType='contact' THEN N', ' +c.TableAlias+N'.['+c.ColumnName+N']  AS '''+m.ColumnName+''''
										  WHEN m.FieldType='file' THEN N', ' +f.TableAlias+N'.['+f.ColumnName+N']  AS '''+f.ColumnName+''''
										  WHEN m.FieldType='grid' THEN N', ' +g.TableAlias+N'.['+g.ColumnName+N']  AS '''+g.ColumnName+''''
										  WHEN m.FieldType='ScheduleData'THEN N', '+ @ScheduleMDResource
										ELSE N', ' +m.TableAlias+N'.['+m.ColumnName+N']  AS '''+REPLACE(m.ColumnName,'@','_')+'''' END

				FROM #MetaDataInfo m WITH (NOLOCK)
				LEFT JOIN #Contacts c ON m.FieldID=c.FieldId
				LEFT JOIN #Files f ON f.ColumnName=m.ColumnName
				LEFT JOIN #Grids g ON g.ColumnName=m.ColumnName
				WHERE m.SmartAppId=@smartAppId
				ORDER BY m.CollectionName, m.ColumnName

				IF @IsCheckListApp=1
				BEGIN
					SET @MdcColumns=@MdcColumns+N', '+@CheckListResponse

					SELECT @CheckListSchema=( SELECT  AliasName as [name] , DataType   as [type]
									, 'NULLABLE' as [mode]
						FROM #CheckListResponseColumns
						FOR Json PATH , INCLUDE_NULL_VALUES
						)
					 SET @CheckListSchema = (SELECT  'CheckList_Responses' as  name
													    , 'RECORD' as [type] ,'REPEATED'  as [mode]
														,JSON_QUERY(@CheckListSchema) as fields
													FOR JSON PATH, INCLUDE_NULL_VALUES , WITHOUT_ARRAY_WRAPPER
											)

					UPDATE @Jsons SET JSQL=JSON_MODIFY(JSQL,'append $',JSON_QUERY(@CheckListSchema))
				END







				SET @DSQL= CASE WHEN @DSQL IS NULL THEN N'' ELSE @DSQL END
								+ N' SELECT @ItemJson=( SELECT '
							   +@MdcColumns +@Joins
							   +N' WHERE doc.Objectid=@Objectid '
							   +N'  FOR JSON PATH , WITHOUT_ARRAY_WRAPPER )  '

    			SELECT @DDL=Jsql FROM @Jsons

				SET @MetaDataColumns=NULL

				SELECT @MetaDataColumns = CASE WHEN @MetaDataColumns IS NULL THEN N'' ELSE @MetaDataColumns+N', ' END + ColumnName
				FROM #MetaDataInfo




				UPDATE #SmartApps SET DML=@DSQL, DDL=@DDL, SysDDL=@SysDDL, MetaDataColumns=@MetaDataColumns, MissingMdColumns=@MissingMdColumns, NewMDColumns=@NewMDColumns WHERE SmartAppId=@SmartAppId



				DELETE FROM @Jsons;

				SET @Contacts=NULL
				SET @Files=NULL
				SET @Grids=NULL
				DELETE FROM #Contacts
				DELETE FROM #Files
				DELETE FROM #Grids
				-- select @SmartAppId
				IF EXISTS (SELECT 1 FROM #SmartApps WHERE SmartAppId>@SmartAppId )
				BEGIN
					SELECT TOP 1 @SmartAppId=SmartAppId, @SmartAppName=SmartAppName FROM #SmartApps WHERE SmartAppId>@SmartAppId  ORDER BY SmartAppId

				END
				ELSE
				BEGIN
					SET @SmartAppId = NULL
				END

			END

			SELECT SmartAppId,SmartAppName, DML
			, JSON_QUERY(DDL) DDL
			, JSON_QUERY(SysDDL) as SysDDL
			, MetaDataColumns
			, MissingMdColumns, NewMDColumns
			FROM #SmartApps ;





			DROP TABLE #Files
			DROP TABLE #Contacts
			DROP TABLE #MetaDataInfo
			DROP TABLE #BQ_SysColumns
			DROP TABLE #SmartApps
			DROP TABLE #Grids
			DROP TABLE #ScheduleMDColumns
			DROP TABLE #CheckListResponseColumns
			DROP TABLE #FilteredColumns


END

