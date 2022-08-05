#   BQ Schema is super set of all the Org Projects schema.

import pyodbc
import json
import re
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest

 # gilabane smartitem data sync for all the projects in all the profiles -- 102 mints
 # gilbane users and companies sync for all the projects in all the profiles --
# nextwave smartitem data sync for all the projects in all the profiles --
# nextwave users and companies sync for all the projects in all the profiles --

#   Global Variables
__SYNCSMARTAPPSSCHEMA = 0
__SYNCRESOURCESSCHEMA = 0
__SYNCSMARTAPPSDATA = 0
__SYNCRESOURCESDATA = 1
__ORGID = 14   #  NextWave_Extract - 14  Gilbane_Extract - 1
__BQDATASET= 'DBAdmin'  # 'NextWave_Extract'
__BQPROJECTID = 'smartappbeta-mta'
__TEMPFILE = 'Temp.jsonl'
__CLUSTERFIELD ="ProjectName"
__PARTITIONFIELD ="CreatedDate"
__APPZONESCRIPTFILE = 'AppzoneScripts.sql'
__SMARTITEMSSCRIPTFILE = 'SmartItemsDML.sql'
__CREDENTIALJSON =  'C:/Users/tparamanandam/.google/smartappbeta-mta.json'

__TIMEPARTITION = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field=__PARTITIONFIELD
)
__CREDENTIALS = service_account.Credentials.from_service_account_file(__CREDENTIALJSON)
__BQCLIENT = bigquery.Client(credentials=__CREDENTIALS, project=__BQPROJECTID)
with open(__APPZONESCRIPTFILE, 'r') as _FILE:
    # __SQLSCRIPTS = (_FILE.read()).split('GO')
    __APPZONESQL = _FILE.read()

with open(__SMARTITEMSSCRIPTFILE, 'r') as _FILE:
    __EXTRACTIONSQL = _FILE.read()

__USERSDDL ='[{"name":"TimeStamp","type":"TIMESTAMP","mode":"NULLABLE"},{"name":"AppZoneId","type":"STRING","mode":"NULLABLE"},{"name":"ProjectName","type":"STRING","mode":"NULLABLE"},{"name":"UserId","type":"INTEGER","mode":"NULLABLE"},{"name":"Name","type":"STRING","mode":"NULLABLE"},{"name":"Email","type":"STRING","mode":"NULLABLE"},{"name":"Phone","type":"STRING","mode":"NULLABLE"},{"name":"Mobile","type":"STRING","mode":"NULLABLE"},{"name":"ProfilePicture","type":"STRING","mode":"NULLABLE"},{"name":"Barcode","type":"STRING","mode":"NULLABLE"},{"name":"CompanyId","type":"STRING","mode":"NULLABLE"},{"name":"CreatedDate","type":"DATETIME","mode":"NULLABLE"}]'
__COMPANIESDDL = '[{"name":"TimeStamp","type":"TIMESTAMP","mode":"NULLABLE"},{"name":"AppZoneId","type":"STRING","mode":"NULLABLE"},{"name":"ProjectName","type":"STRING","mode":"NULLABLE"},{"name":"CompanyId","type":"INTEGER","mode":"NULLABLE"},{"name":"Name","type":"STRING","mode":"NULLABLE"},{"name":"Color","type":"STRING","mode":"NULLABLE"},{"name":"Thumbnailurl","type":"STRING","mode":"NULLABLE"},{"name":"WebPage","type":"STRING","mode":"NULLABLE"},{"name":"VendorID","type":"STRING","mode":"NULLABLE"},{"name":"CompanyType","type":"INTEGER","mode":"NULLABLE"},{"name":"PrimeContractor","type":"INTEGER","mode":"NULLABLE"},{"name":"IsImportedFromOrg","type":"BOOLEAN","mode":"NULLABLE"},{"name":"CreatedDate","type":"DATETIME","mode":"NULLABLE"},{"name":"Trades","type":"RECORD","mode":"REPEATED","fields":[{"name":"Name","type":"STRING","mode":"NULLABLE"}]}]'
__USERSCRIPTS = 'UsersDML.sql'
__COMPANIESSCRIPT='CompaniesDML.sql'

with open(__USERSCRIPTS, 'r') as _FILE:
    __USERSDML = _FILE.read()

with open(__COMPANIESSCRIPT, 'r') as _FILE:
    __COMPANIESDML = _FILE.read()



def mergejson (_jsonA, _jsonB):

    if _jsonA is None:
        _jsonfinal = _jsonB
    else :
        _jsonfinal=_jsonA
        for x in _jsonB:
            if not any([obj.get('name') == x["name"] for obj in _jsonA]):
                _jsonfinal.append(x)

    return _jsonfinal

def replacesmartappname (_smartappname):
    _tablename = re.sub('[^a-zA-Z0-9]', '_', _smartappname)
    _tablename = re.sub('_+', '_', _tablename)
    return _tablename

def createbqtable(_schema_json, _dataset, _tablename, _timepartition):
    _bqdataset = __BQCLIENT.create_dataset(dataset=_dataset, exists_ok=True)
    _table_ref = _bqdataset.table(_tablename)
    _schema_json=json.loads(_schema_json)
    # print(type(_schema_json))
    # print(_tablename,_schema_json)
    _bqtable = bigquery.Table(_table_ref, schema=_schema_json)
    _bqtable.clustering_fields = [__CLUSTERFIELD]

    if _timepartition is not None:
        _bqtable.time_partitioning = _timepartition

    _bqtable = __BQCLIENT.create_table(_bqtable, exists_ok=True)
    print(__BQPROJECTID, '.', _dataset, '.', _tablename, ' Table Created')
    return

def alterbqtable(_schema_json, _dataset, _tablename):
    # print(_schema_json)
    try:
        _bqdataset = __BQCLIENT.create_dataset(dataset=_dataset, exists_ok=True)
        _table_ref = _bqdataset.table(_tablename)
        _schema_json = json.loads(_schema_json)
        _bqtable = bigquery.Table(_table_ref, schema=_schema_json)
        _bqtable = __BQCLIENT.update_table(_bqtable, ["schema"])
        print(__BQPROJECTID, '.', _dataset, '.', _tablename, ' Table Altered')

    except BadRequest as e:
        print('Error :{}'.format(e['message']))
        print(_tablename, ' Not Altered in ', _dataset)
        print(_schema_json)
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        print(_tablename, ' Not Altered in ', _dataset)
        print(_schema_json)
        raise

    return


def populatebqtable (_dataset, _tablename, _jsonlfile, _appzonedb, _profileid ) :

    dataset = __BQCLIENT.dataset(_dataset)
    _table_ref = dataset.table(_tablename)
    _table = bigquery.Table(_table_ref)
    job_config= bigquery.LoadJobConfig(
        autodetect=False, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    print("Data is loading for ", _tablename, ' for ', _appzonedb, ' for profileid-', _profileid)


    try:
        with open(_jsonlfile, mode='rb') as _file:
            _load_job = __BQCLIENT.load_table_from_file(_file, _table, job_config=job_config)
            _load_job.result()

        print('Data loaded to BigQuery in ', _dataset, '.', _tablename, ' Table  ')
    except BadRequest as e:
        for e in _load_job.errors:
            print('Error :{}'.format(e['message']))
        print("Data not loaded for ", _tablename, ' for ', _appzonedb , ' for profileid-' , _profileid)

    finally :
        print('Data loading Completed ', _dataset, '.', _tablename, ' Table  ')

    return


def getsaxconnection(_saxserver, _saxdb, _saxusername, _saxpwd):
    _saxconnection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + _saxserver
                              + ';DATABASE=' + _saxdb
                              + ' ;UID=' + _saxusername
                              + ';PWD=' + _saxpwd
                              )

    return _saxconnection

def getappzoneconnection(_appzoneserver, _appzonedb, _appzoneusername, _appzonepwd):
    _appzoneconnection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + _appzoneserver
                              + ';DATABASE=' + _appzonedb
                              + ' ;UID=' + _appzoneusername
                              + ';PWD=' + _appzonepwd
                              )

    return _appzoneconnection

def getorgprofiles(_pid):
    _sqlqry = """ 
		SELECT  'pc'+proj.AppZoneID as dbname , c.MsSqlServer, c.MsSqlUserName
                        , c.MsSqlPassword, proj.AppZoneId	,oprof.id ProfileID
		FROM  JSV_OrgConsoleProfiles oprof WITH(NOLOCK) 
		JOIN JSV_OrgConsoles org WITH(NOLOCK) ON org.id=oprof.OrgConsoleID
		JOIN JSV_Projects proj WITH(NOLOCK) ON oprof.ProjectID = proj.ID  
		JOIN JSV_Clusters c WITH (NOLOCK) ON c.id=proj.ClusterID
		WHERE proj.IsSample = 0 AND proj.[Status] = 1 AND proj.SetupStatus = 17
		AND org.id=?
		 ; """
    _saxcon = getsaxconnection('','', '','')
    _cursor = _saxcon.cursor()
    _cursor.execute(_sqlqry,_pid)
    _profilelist = _cursor.fetchall()
    _cursor.close()

    if (_saxcon):
        _saxcon.close()

    return _profilelist


def getprofileprojects(_pid):
    _sqlqry = """ 
		SELECT 'pc'+proj.AppZoneID as dbname , c.MsSqlServer, c.MsSqlUserName
                        , c.MsSqlPassword, proj.AppZoneId	,oproj.ProfileID
		FROM  JSV_OrgConsoleProjects oproj WITH(NOLOCK)   
		JOIN JSV_Projects proj WITH(NOLOCK) ON oproj.ProjectID = proj.ID  
		JOIN JSV_OrgConsoleProfiles oprof WITH(NOLOCK) ON oprof.ID = oproj.ProfileID
		JOIN JSV_OrgConsoles org WITH(NOLOCK) ON org.id=oprof.OrgConsoleID
		JOIN JSV_Clusters c WITH (NOLOCK) ON c.id=proj.ClusterID
		WHERE proj.IsSample = 0 AND proj.[Status] = 1 AND proj.SetupStatus = 17
		AND proj.[Type] != 7 AND proj.RedirectUrl IS NOT NULL AND  proj.AppZoneID IS NOT NULL
        AND oprof.Id=?
        -- AND proj.AppZoneid='8e9b146deb0a4780bbcb1a4fe7c4329b'  -- DEBUG
        
 		 ; """
    _saxcon = getsaxconnection('10.55.100.209', 'smartapp.com', 'zonemanager', 'x11ek25q32269Bw')
    _cursor = _saxcon.cursor()
    _cursor.execute(_sqlqry, _pid)
    _dblist = _cursor.fetchall()
    _cursor.close()

    if (_saxcon):
        _saxcon.close()

    return _dblist


def getbiqqueryschema(_server, _dbname, _userid, _pwd, _smartappid, _mdcolumns  ):
    _sasqlqry = __APPZONESQL
    _schemaconn = getappzoneconnection(_server,_dbname,_userid,_pwd)
    _cursor = _schemaconn.cursor()
    _cursor.execute(_sasqlqry, [_smartappid, _mdcolumns])
    _profilelist = _cursor.fetchall()
    _cursor.close()
    if (_schemaconn):
        _schemaconn.close()

    return  _profilelist

def getprojectschema(_appzoneconn, _smartappid, _mdcolumns  ):
    _sasqlqry = __APPZONESQL
    _cursor = _appzoneconn.cursor()
    _cursor.execute(_sasqlqry, [_smartappid, _mdcolumns])
    _salist = _cursor.fetchall()
    _cursor.close()
    return  _salist


def syncbqschema (_smartapplist):
    if __SYNCRESOURCESSCHEMA ==1 :
        createbqtable(__USERSDDL, __BQDATASET, 'Users' , __TIMEPARTITION)
        createbqtable(__COMPANIESDDL, __BQDATASET, 'Companies', __TIMEPARTITION)
        alterbqtable(__USERSDDL, __BQDATASET, 'Users')
        alterbqtable(__COMPANIESDDL, __BQDATASET, 'Companies')

    if __SYNCSMARTAPPSSCHEMA == 1 :
        for name, group in _smartapplist.groupby('SmartAppName'):
            _smartappname = name
            _tablename = replacesmartappname(_smartappname)
            _ddl = group["DDL"].tolist()[0]
            _sysddl = group["SysDDL"].tolist()[0]
            # print("Name ",_smartappname,"group : ",_ddl,"sysddl :", _sysddl )
            createbqtable(_sysddl, __BQDATASET, _tablename, __TIMEPARTITION)
            alterbqtable(_ddl, __BQDATASET, _tablename)
    return


def syncresourcesdata(_projectslist, _profileid) :
    for _project in _projectslist:
        _appzonedb = _project[0]
        _appzoneserver = _project[1]
        _appzoneuserid = _project[2]
        _appzonepwd = _project[3]
        _appzoneid = _project[4]
        _appzoneconn = getappzoneconnection(_appzoneserver, _appzonedb, _appzoneuserid, _appzonepwd)
        _userscount = getresourcesjson(_appzoneconn, 'Users', _appzonedb, __USERSDML, _appzoneid)
        if _userscount > 0:
            populatebqtable(__BQDATASET, 'Users', __TEMPFILE, _appzonedb, _profileid)

        _companiescount = getresourcesjson(_appzoneconn, 'Companies', _appzonedb, __COMPANIESDML, _appzoneid)
        if _companiescount > 0:
            populatebqtable(__BQDATASET, 'Companies', __TEMPFILE, _appzonedb, _profileid)

        if (_appzoneconn):
            _appzoneconn.close()

    return


def syncsmartappsdata(_projectslist, _smartappslist, _profileid) :
    for _project in _projectslist:
        _appzonedb = _project[0]
        print("Project DB Name ", _appzonedb)
        _appzoneserver = _project[1]
        _appzoneuserid = _project[2]
        _appzonepwd = _project[3]
        _appzoneid= _project[4]
        _appzoneconn = getappzoneconnection(_appzoneserver, _appzonedb, _appzoneuserid, _appzonepwd)
        for _index, _row in _smartappslist.iterrows():
            _smartappid = _row['SmartAppId']
            _smartappname = _row['SmartAppName']
            _dml = _row['DML']
            _mdcolumns = _row['MetaDataColumns']
            _smartitemscount = getsmartitemsjson(_appzoneconn, _smartappid, _dml, _smartappname, _appzonedb, _appzoneid, _mdcolumns)
            try:
                # checkifappexists(_appzoneserver, _appzonedb, _appzoneuserid, _appzonepwd, _smartappid, _smartappname)
                if _smartitemscount > 0:
                    populatebqtable(__BQDATASET, replacesmartappname(_smartappname), __TEMPFILE, _appzonedb, _profileid)
                else :
                    print("Data not available  Smartappid : ", _smartappid, _smartappname, ' in', _appzonedb)
            except Exception as e:
                print("Data not Populated for Smartappid : ", _smartappid, _smartappname, ' in', _appzonedb,
                      ' project due to schema mismatch')
                print(e)


                pass

        if (_appzoneconn):
            _appzoneconn.close()
    return


# def getsmartitemslist(_zoneconn, _smartappid):
#     _cursor = _zoneconn.cursor()
#     _sql ="""SELECT  doc.objectid FROM IqFrame_DocumentTypeinstance doc WITH (NOLOCK)
# 	 JOIN IQFrame_DefinedSmartFolder df WITH (NOLOCK) ON doc.SmartApp=df.ObjectId
# 	  WHERE df.ID=?  and doc.status in (0,2,3)
# 	  """
#
#     _cursor.execute(_sql, _smartappid)
#     _itemslist = _cursor.fetchall()
#     _cursor.close()
#
#     # print('No of smartitems in smartappid :', len(_itemslist))
#     return _itemslist



def getsmartitemsjson(_zoneconn, _smartappid, _dsql, _smartappname, _appzonedb, _appzoneid, _mdcolumns, _recursioncount=0):
    _params =[_smartappid, _dsql, _appzoneid]
    _cursor = _zoneconn.cursor()
    _itemscount = 0

    try :
        _cursor.execute(__EXTRACTIONSQL, _params)

        with open(__TEMPFILE, 'a+', encoding='utf-8') as _file:
            _file.truncate(0)
            while True:
                _row = _cursor.fetchone()
                if _row is None:
                    break
                _file.write(_row[0] + '\n')
                _itemscount += 1
            _file.close()
    except Exception as e:
        print("Data not Populated for Smartappid : ", _smartappid, _smartappname, ' in', _appzonedb,
              ' project due to schema mismatch')
        print(e)
        if _recursioncount == 0:
            _projectschema=getprojectschema(_zoneconn, _smartappid, _mdcolumns)
            _newDsql =_projectschema[0][2]
            _missingmdcolumns = _projectschema[0][6]
            _newmdcolumns = _projectschema[0][7]
            _newitemscount= getsmartitemsjson(_zoneconn , _smartappid, _newDsql, _smartappname, _appzonedb, _appzoneid, _mdcolumns, 1)
            _itemscount=_newitemscount
            print(_appzonedb, ' project is having only ( ', _projectschema[0][5], ' fields in ', _smartappname, ' app')
            print(_appzonedb, ' project is missing (', _missingmdcolumns, ') fields in ', _smartappname, ' app')
            if _newmdcolumns is not None:
                print(_appzonedb, ' project is having additional (', _newmdcolumns, ') fields in ', _smartappname, ' app')
            if _itemscount>0:
                print("Data Loaded for Smartappid : ", _smartappid, _smartappname, ' in', _appzonedb,
                      ' after excluding missing and additional fields in the project')
        return _itemscount
    finally :
        _cursor.close()

    print('No of smartitems in smartappid (', _smartappid, _smartappname , ') :', _itemscount)
    return _itemscount


def getresourcesjson(_zoneconn, _resourcename, _appzonedb, _dsql, _appzoneid):
    _cursor = _zoneconn.cursor()
    _itemscount = 0

    try :
        _cursor.execute(_dsql, _appzoneid)

        with open(__TEMPFILE, 'a+', encoding='utf-8') as _file:
            _file.truncate(0)
            while True:
                _row = _cursor.fetchone()
                if _row is None:
                    break
                _file.write(_row[0] + '\n')
                _itemscount += 1
            _file.close()
    except Exception as e:
        print("Data not Populated for ", _resourcename, ' from', _appzonedb)
        print(e)
        return _itemscount
    finally :
        _cursor.close()
    print('No of Objects in ', _resourcename, _itemscount)
    return _itemscount


def getprofileschema(_profiles):
    _proflieschema = pd.DataFrame()
    for _pf in _profiles:
        # print("Profile :",_pf)
        _smartapplist=getbiqqueryschema(_pf[1], _pf[0], _pf[2], _pf[3],  None ,  None )
        for _sa in _smartapplist:
            _sarecord = pd.DataFrame.from_records([{"SmartAppId":_sa[0], "SmartAppName":_sa[1], "DML":_sa[2], "DDL":_sa[3]
                                                     , "SysDDL":_sa[4], "ProfileId":_pf[5], "MetaDataColumns":_sa[5]}])
            _proflieschema=pd.concat([_proflieschema, _sarecord],ignore_index=True)
    _uniquesmartappids=_proflieschema.SmartAppId.unique()
    for _sid in _uniquesmartappids:
        # print("Unique SmartAppId :",_sid)
        _ddl = None
        _filteredapps = _proflieschema.loc[_proflieschema['SmartAppId'] == _sid]
        for _index in _filteredapps.index:
            _ddl=mergejson(_ddl, json.loads(_filteredapps["DDL"][_index]))
        _proflieschema.loc[_proflieschema.SmartAppId == _sid, 'DDL'] = json.dumps( _ddl)
    return _proflieschema

def checkifappexists(_server,_dbname,_userid,_pwd, _smartappid, _smartappname):

    _sasqlqry = " SELECT Name FROM IqFrame_DefinedSmartFolder WITH (NOLOCK) WHERE Id=?"
    _schemaconn = getappzoneconnection(_server,_dbname,_userid,_pwd)
    _cursor = _schemaconn.cursor()
    _cursor.execute(_sasqlqry, _smartappid)
    _smartapplist = _cursor.fetchall()
    _cursor.close()

    if len(_smartapplist) == 0:
        print(_smartappname, "Missing in ", _dbname)

    if (_schemaconn):
        _schemaconn.close()

    return


def extractprojectdata():
    _smartappschemas=[]
    if __SYNCRESOURCESDATA ==1 or __SYNCSMARTAPPSDATA==1 or __SYNCSMARTAPPSSCHEMA==1 :
        #  Getting  all the Profile Projects
        _orgprofiles = getorgprofiles(__ORGID)
        # Getting Merged schema from all the Profiles
        _smartappschemas=getprofileschema(_orgprofiles)
        # sync BQ schema

    if __SYNCSMARTAPPSSCHEMA== 1 or __SYNCRESOURCESSCHEMA==1 :
        syncbqschema(_smartappschemas)

    if __SYNCRESOURCESDATA ==1 or __SYNCSMARTAPPSDATA==1 :
        for _pf in _orgprofiles:
            _profileid=_pf[5]
            print('Profile Id :', _profileid, "Profile DB :", _pf[0])

            _profileprojects =getprofileprojects(_profileid)
            if __SYNCRESOURCESDATA ==1 :
                syncresourcesdata(_profileprojects,_profileid)
            if __SYNCSMARTAPPSDATA== 1 :
                # Fetch SmartApps list by ProfileId
                _smartapplist = _smartappschemas.loc[_smartappschemas['ProfileId'] == _profileid]
                # print(_smartappschemas.iloc[0]['DML'])  # DEBUG
                syncsmartappsdata(_profileprojects, _smartapplist, _profileid)

    return



if __name__ == '__main__':
    try:
        print(datetime.utcnow())
        _exectiontime = datetime.utcnow()
        extractprojectdata()

    finally:

        _datediff = datetime.utcnow() - _exectiontime
        print(datetime.utcnow())
        print("GetProfileProjects.py Execution completed in ", divmod(_datediff.total_seconds(), 60))
