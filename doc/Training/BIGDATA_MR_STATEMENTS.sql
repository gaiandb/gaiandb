------------------------------------------------------------------------
------ ALL  SQL statements useful for  distributed processing slides/demo
------ Patrick Dantressangle  dantress@uk.ibm.com 
------------------------------------------------------------------------
 
 SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE Q1_parts','with_provenance') Q_drop

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE T1_PART','with_provenance') Q_drop

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE T1_out','with_provenance') Q_drop 

----------- Set LT MRBIKES....   

SELECT * FROM NEW com.ibm.db2j.GaianQuery('call setlt(''MRBIKES'',''id INTEGER, stationName VARCHAR(40), areaName VARCHAR(30), lat DOUBLE,lon DOUBLE, nbBikes INTEGER,nbEmptyDocks INTEGER, installed BOOLEAN,locked BOOLEAN, temp BOOLEAN''
                                          ,'''')', 'with_provenance') GQ

call listlts()

-----------------SET local GAIAN  6414 to federate the whole file. and verify ...

call setdsfile('MRBIKES','DS0','./testdata/cyclehire.csv', 'MAP_COLUMNS_BY_POSITION' ,'')

--ID,SUBSTR(STATIONNAME,1,50) STATIONNAME, SUBSTR(AREANAME,1,50) AREANAME,lat ,lon , nbBikes ,nbEmptyDocks , installed ,locked , temp 

SELECT * FROM MRBIKES_P   


SELECT * FROM NEW com.ibm.db2j.GaianQuery('SELECT count(*) FROM MRBIKES_0','with_provenance') GQ

------------------ Set the tables for distributing the data....

SELECT * FROM NEW com.ibm.db2j.GaianQuery('
	CREATE TABLE Q1_parts(gdbnode varchar(50), lowkey varchar(10),  highkey varchar(10))
	', 'with_provenance') GQ

------verify that all tables were created on all nodes.

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) from Q1_parts', 'with_provenance') GQ

call listlts()

call listnodes()
 
SELECT * FROM NEW com.ibm.db2j.GaianQuery('INSERT INTO  Q1_parts
                                             VALUES( ''IBMFMO4FVV37LS'', ''A'',''F''),
                                                   (''IBMFMO4FVV37LS:6415'',''F'',''N''),
                                                   (''IBMFMO4FVV37LS:6416'',''N'',''Z'')
                 ','') Q_insert_part_info

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) from Q1_parts', 'with_provenance') GQ

-----------------distribute the data to every nodes relevant to map/reduce
SELECT * FROM NEW com.ibm.db2j.GaianQuery('drop table T1_PART','with_provenance') Q_1

SELECT * FROM NEW com.ibm.db2j.GaianQuery('CREATE TABLE T1_PART(id INTEGER,stationName VARCHAR(255), areaName VARCHAR(255), lat DOUBLE,lon DOUBLE, nbBikes INTEGER,nbEmptyDocks INTEGER, installed BOOLEAN,locked BOOLEAN, temp BOOLEAN)
                                          ','with_provenance') Q_create
                                          
-------verify that they are all created.

call listderbytables()

--make sure all are empty ....

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) from T1_part', 'with_provenance') GQ

select gdb_node()  from  sysibm.sysdummy1

SELECT * FROM NEW com.ibm.db2j.GaianQuery('INSERT INTO T1_PART
                                            SELECT id,stationName,areaName,lat,lon,nbBikes,nbEmptyDocks,installed,locked,T1.temp
                                              FROM MRBIKES T1,Q1_parts Q1
                                             WHERE gdb_node()=Q1.gdbnode
                                               AND T1.areaName between lowkey AND highkey 
                                          ','with_provenance') Q_distribute
                                          
SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) cl,Min(areaname) mn,max(areaname) mx from T1_part', 'with_provenance') GQ

---results                          
---159	IBMFMO4FVV37LS	jdbc:derby:gaiandb;create=true
---181	IBMFMO4FVV37LS:6415	jdbc:derby:gaiandb6415;create=true
---225	IBMFMO4FVV37LS:6416	jdbc:derby:gaiandb6416;create=true

--count that all data has been distributeed to all nodes...

SELECT sum(c) All_data_across_all_nodes FROM NEW com.ibm.db2j.GaianQuery('
                                            SELECT count(*) c
                                              FROM T1_PART 
                                          ','with_provenance') Q_distribute
                                          
                                          
                                          
---Now do the distributed processing ..... 
--create the intermediate result sets..just in case w want to reuse it ...
 SELECT * FROM NEW com.ibm.db2j.GaianQuery('CREATE TABLE T1_out (col1   VARCHAR(50),
                                                                 wSUM   INTEGER,
                                                                 wCOUNT INTEGER,
                                                                 wMAX_per_AREA INTEGER )
                                 ','with_provenance') Q_create

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) from T1_out', 'with_provenance') GQ
                                        
SELECT * FROM NEW com.ibm.db2j.GaianQuery('INSERT INTO T1_out
                             SELECT T1.areaname,
                                SUM(T1.nbBikes) wsum, 
                                COUNT(T1.nbBikes) wcount,
                                MAX(T1.nbBIKES) wMAX_per_AREA
                             FROM T1_PART T1
                            GROUP BY T1.areaname','with_provenance') Q_reduce_step1                                    

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select count(*) from T1_out', 'with_provenance') GQ

---Results: 
---24	IBMFMO4FVV37LS	jdbc:derby:gaiandb;create=true
---26	IBMFMO4FVV37LS:6415	jdbc:derby:gaiandb6415;create=true
---42	IBMFMO4FVV37LS:6416	jdbc:derby:gaiandb6416;create=true

SELECT * FROM NEW com.ibm.db2j.GaianQuery('select * from T1_out', 'with_provenance') GQ

---Final step : calculating average of bikes globally

SELECT  SUM( wSUM)/(SUM(wcount)) GLOBAL_AVERAGE_BIKES 
 FROM NEW com.ibm.db2j.GaianQuery('SELECT  col1, wsum,   wcount   
                                     FROM T1_OUT'
                                  , 'with_provenance') Q_final_reduce
                                  
---Final step : calculating averge of bikes per areaname 

SELECT col1 arename, SUM( wSUM)/(SUM(wcount)) GLOBAL_AVERAGE_BIKES_PER_AREA, MAX(wMAX_per_AREA ) wMAX_per_AREA, 
       CASE WHEN MAX(wMAX_per_AREA )=SUM( wSUM)/(SUM(wcount)) THEN 'FULL' ELSE ' ' END "FULL?"
  FROM NEW com.ibm.db2j.GaianQuery('SELECT  col1, wsum,   wcount , wMAX_per_AREA
                                     FROM T1_OUT'
                                  , 'with_provenance') Q_final_reduce
  GROUP BY col1 
                                    
--clean all the Partition tables ...

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DELETE FROM T1_PART','with_provenance') Q_clean

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DELETE FROM Q1_parts','with_provenance') Q_clean

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DELETE FROM T1_out','with_provenance') Q_clean

--drop all the Partition tables ...

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE Q1_parts','with_provenance') Q_drop

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE T1_PART','with_provenance') Q_drop

SELECT * FROM NEW com.ibm.db2j.GaianQuery('DROP TABLE T1_out','with_provenance') Q_drop 




----------------------------------------------------------------------
---------- Analysis for errors in Gaian  logs.  ----------------------
----------------------------------------------------------------------

SELECT 
  Error_type,
   SUM(Case When  GDB_NODE = 'IBMFMO4FVV37LS'  then cnt else 0 end ) IBMFMO4FVV37LS,
   SUM(Case When  GDB_NODE = 'IBMFMO4FVV37LS:6415'  then cnt else 0 end) "IBMFMO4FVV37LS:6415",
   SUM(Case When  GDB_NODE = 'IBMFMO4FVV37LS:6416'  then cnt else 0 end) "IBMFMO4FVV37LS:6415"
FROM ( 
SELECT GDB_NODe,substr(error_type,1, locate(':',error_type)) Error_type, count(*) cnt
 FROM NEW com.ibm.db2j.GaianQuery('
	select  substr(column1,locate(''** '', column1)+15) error_type 
     from GDB_LTLOG_0 where (Column1 like ''%GDB_WARNING%''  OR Column1 like ''%GDB_ERROR%'') and Substr(column1,locate(''** '', column1)) is not null
', 'with_provenance') GQ
Group by GDB_NODe,substr(error_type,1, locate(':',error_type))
) T
GROUP BY ERROR_TYPE

