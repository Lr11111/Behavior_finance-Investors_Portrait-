filename DataBaseName 'E:\NJZQ\交付普通\ZJ2015\ZJ1503.DBF'; 
proc dbf db3=DataBaseName out=DataBaseName; 
run; 

filename DataBaseName 'E:\njzq2\ASHAREMANAGEMENTHOLDREWARD.dbf'; 
proc dbf db3=DataBaseName out=DataBaseName; 
run; 

FILENAME exam 'E:\NJZQ\交付普通\ZJ2015\dbf3.DBF';
PROC DBF DB5=exam OUT=b;
run;

FILENAME exam 'E:\NJZQ\dbase4\b1.dbf';
PROC DBF DB5=exam OUT=b;
run;

filename  encoding='utf-8';
PROC IMPORT OUT= WORK.Dbf 
            DATAFILE= "E:\njzq2\ASHAREMANAGEMENTHOLDREWARD.dbf" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;

filename export "E:\njzq2\ASHAREMANAGEMENTHOLDREWARD.dbf" encoding='GBK';
PROC IMPORT out= work.approval
             datafile = export
            DBMS=DBF REPLACE;
     GETDELETED=NO;run;

PROC IMPORT OUT= WORK.Dbf 
            DATAFILE= "E:\NJZQ\交付普通\ZJ2015\ZJ1502.DBF" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;

PROC IMPORT OUT= WORK.Dbf 
            DATAFILE= "E:\NJZQ\data2016\WT2015_06_010.dbf" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;

PROC IMPORT OUT= WORK.Dbf1 
            DATAFILE= "E:\NJZQ\交付信用\ywkm.DBF" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;

PROC IMPORT OUT= WORK.Dbf1 
            DATAFILE= "E:\NJZQ\交付信用\khxxa.DBF" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;

PROC SETINIT;RUN;


proc import datafile="E:\NJZQ\data2016\WT2015_06_010.dbf"            
out=mydata           
dbms=dbf          
replace; 
run;
