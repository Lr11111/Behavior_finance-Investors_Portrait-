FILENAME exam 'f:\dbf\������ͨ\WT0701.dbf';
PROC DBF DB5=exam OUT=b;
run;
FILENAME exam 'E:\NJZQ\201501.dbf';
PROC DBF DB5=exam OUT=b201501;
run;

data t;set b201501;if  index(zqdm,'1502') then output;run;
proc freq data=t;table zqdm;run;

proc sort data=b201501 NODUPKEY;by khh;run;
data t;set b201501;where zqlb='E0';run;

proc freq data=b201501 noprint;tables khh;output out=z;run;

data riqi;set b(keep=wtrq jgsm);
date=input(put(wtrq,best8.),yymmdd10.);
format date yymmdd10.;
run;

ods html file="e:\t.xlsx";
proc tabulate data=riqi out=r;
class date jgsm;
table date ,jgsm all;
run;quit;

data t;set r;where _type_='10';run;
proc gplot data=t;plot n*date;symbol i=j;run;


FILENAME exam 'E:\NJZQ\������ͨ\ZJ2015\dbf4.dbf';
PROC DBF DB5=exam OUT=z;
run;
data riqi;set z(keep=rq ywkm);
date=input(put(rq,best8.),yymmdd10.);
format date yymmdd10.;
run;

ODS HTML CLOSE;
ODS HTML;
ods html file="e:\t1.xls";
proc tabulate data=riqi out=r;
class date ywkm;
table ywkm all,date;
run;quit;

data t;set r;where _type_='10';run;
proc gplot data=t;plot n*date;symbol i=j;where n>100;run;


FILENAME exam 'E:\NJZQ\kehu.dbf';
PROC DBF DB5=exam OUT=kh;
run;

data riqi;set kh(keep=khrq);
date=input(put(khrq,best8.),yymmdd10.);
format date yymmdd10.;
run;

proc univariate data=riqi noprint;
histogram date;where khrq>19910101;run;quit;


data riqi;set kh(keep=csrq);
date=input(strip(csrq),yymmdd10.);
format date yymmdd10.;
run;

proc univariate data=riqi noprint;
histogram date;where "01jan1911"d<date<"01jan2014"d;;quit;
/****************************/
/*ҵ���Ŀ*/
data ywkm;
set biao(keep=f1 f2) biao(keep=f3 f4 rename=(f3=f1 f4=f2)) biao(keep=f5 f6 rename=(f5=f1 f6=f2));run;

/*ί�����*/
PROC IMPORT OUT= WORK.wtlb 
            DATAFILE= "C:\Users\chenzq\Desktop\��ṹ.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet2$"; 
     GETNAMES=NO;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data wtlb;set wtlb;array char _character_;array num _numeric_;
do i=1 to 6;
a=num(i);b=char(i);output;end;keep a b;run;

/*ί�б� ��ǩ*/
PROC IMPORT OUT= WORK.label 
            DATAFILE= "C:\Users\chenzq\Desktop\��ṹ.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet3$"; 
     GETNAMES=NO;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

/*��ȡ������*/
proc contents data=b noprint out=z;run;quit;
/*�������еı����� �� ���еı�ǩ���ж�ӳ*/
proc sql;create table label1 as select z.name,label.f2 from z left join label on strip(z.name)=strip(label.f1);quit;
/*����2����������ֱ�洢����������ǩ��*/
/*proc sql noprint ;select name,f2 into:name separated by " ",:label separated by " "*/
/*from label1 ;quit;*/
/*���ݱ�������ǩ�ı�񣬹��� var1='����1' �ĸ�ʽ */
proc sql noprint;
select distinct strip(name)||"='"||strip(F2)||"'"
   into : rename separated by " "
           from label1;
quit;
/*label�����б�ǩ*/
data b1;set b(obs=1);label &rename;run;

