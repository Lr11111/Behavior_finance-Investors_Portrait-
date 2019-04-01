/*构建逻辑库：njzq*/
libname njzq 'e:\njzq';

/*打开客户信息表格108w条*/
PROC IMPORT OUT= WORK.khxx    
            DATAFILE= "I:\njzq_bdase4_bdf\khxx.DBF" 
            DBMS=DBF REPLACE;
     GETDELETED=NO;
RUN;
/*********************************************************************/
/*导入表头的label*/
%macro label_(name,num);
PROC IMPORT OUT= WORK.&name  
            DATAFILE= "C:\Users\chenzq\Desktop\表结构.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet&num$"; 
     GETNAMES=NO;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
%mend;
/*资金表的label*/
%label_(zj_label,5);
proc sql noprint;
select distinct strip(f1)||"='"||strip(F2)||"'"
   into : zj_label separated by " "
           from zj_label;
quit;

/*客户信息的label*/
%label_(khxx_label,4);
proc sql noprint;
select distinct strip(f1)||"='"||strip(F2)||"'"
   into : khxx_label separated by " "
           from khxx_label;
quit;
/*委托表的label*/
%label_(wt_label,3);
proc sql noprint;
select distinct strip(f1)||"='"||strip(F2)||"'"
   into : wt_label separated by " "
           from wt_label;
quit;

/*****************************************************************/
/*客户信息引入label*/
proc datasets library=work noprint;
modify khxx;
   label &khxx_label;
run;quit;
/*将客户信息表保存到njzq文件夹*/
data njzq.khxx;set khxx;run;
proc freq data=khxx;table zjlb;run;


/***********************打开委托表数据***********************/
%let name1=WT0702;
FILENAME exam "E:\NJZQ\data\&name1..DBF";
PROC DBF DB5=exam OUT=&name1;run;

proc datasets library=work noprint;
modify wt1501;
   label &wt_label;
run;quit;
/*找出分级基金的委托*/
data fj;
set wt0701;
where substr(zqdm,1,2) in ('50','15','16');
run;


data fj_150;
set wt1501;where zqdm like '150%';run;

data zy;set zj1501(keep=zy firstobs=396011 obs=400000);a1=scan(zy,2,'()');
if substr(a1,1,2) in ('50','15','16');;run;



/*导入分级基金的名单，及其相关信息*/
option notes;
PROC IMPORT OUT= WORK.menu  
            DATAFILE= "C:\Users\chenzq\Desktop\分级基金2.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet3$"; 
     GETNAMES=yes;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

/***********对分级基金目录进行提取**************/
data menu1;set njzq.menu;if find(_col17,'分级');run;
data menu1;set menu1;keep _col15 _col16 _col1 _col0 _col2;
           _col0=substr(_col0,1,6);run;
/*将名单里取出重复值*/
proc sort data=menu1  nodupkey;by _col0 _col15;run;
/*计算包含重要指标的menu_*/
data menu_;set njzq.menu;keep _col0-_col5 _col9-_col17 ;_col0=substr(_col0,1,6);
_col3=substr(_col3,1,6);_col4=substr(_col4,1,6);_col5=substr(_col5,1,6);run;
proc sort data=menu_  nodupkey;by _col0 _col15;run;


libname data "I:\njzq_bdase4_bdf\交付普通zj";
/**************************************************************************/
/**************打开资金表数据****************/
%let n=15;
%let m=4;
/*%macro zj;*/
%do m=1 %to &m;
FILENAME exam "I:\njzq_bdase4_bdf\交付普通zj\zj&n.0&m..DBF";
PROC DBF DB5=exam OUT=ZJ1501;run;

/*data data.zj&n.0&m;set zj1501;run;*/
proc datasets library=work noprint;
modify zj1501;
   label &zj_label;
run;quit;

/*找出资金表中的买卖单,对摘要进行提取*/
data zj;set zj1501(keep=rq khh zy);informat code $6.;code=scan(zy,2,'()');
date=input(put(strip(rq),$8.),yymmdd8.);format date yymmdd10.;drop rq;run;
data zj1;set zj;t1=scan(zy,1,'(');t2=substr(compress(t1,,'n'),1,2);
t3=find(t1,strip(t2));t4=substr(t1,t3,80-t3);
where substr(code,1,2) in ('00','12','15','16','50','51','55');run;
/*data zj2;set zj1;t1=scan(zy,1,'(');t2=substr(compress(t1,,'n'),1,2);*/
/*t3=find(t1,strip(t2));t4=substr(t1,t3,80-t3);run;*/
data zj3;set zj1;informat bs $2.;bs=compress(t4,'买卖','k');informat stock $20.;
if bs~='' then stock=substr(t4,3,20);drop t1-t4;run;
data zj4;set zj3;t1=scan(zy,2,')');t2=scan(t1,1,'*');t3=scan(t1,2,'*');
if t3~='' then do;price=input(strip(t3),8.);vol=input(strip(compress(t2,'股')),8.);end;
drop t1-t3;run;

/*将menu1目录 和 zj4资金 进行匹配*/
proc sql;create table fenji as select * from menu1 left join zj4 on
zj4.code=menu1._col0 ;quit;
data fenji;set fenji;where date~=.;run;
/*删除code一样但name不一样的、删除交易日期和到期日不符的*/
data fenji_zj&n.0&m;set fenji;if find(_col1,substr(stock,1,2));where date<_col16 or _col16=.;run;
%end;
%mend;
%zj;

/*整合*/
data fenji_20&n;set fenji_zj&n.01 fenji_zj&n.02 fenji_zj&n.03 fenji_zj&n.04 ;run;
data fenji;set  fenji_zj; if bs='' then bs='申';where stock~='恒立实业' ;run;


/*将客户信息数据进行合并*/
proc sql;create table fenji1 as select * from fenji left join khxx on
fenji.khh=khxx.khh ;quit;
/*将分级基金信息进行合并*/
proc sql;create table fenji2 as select * from fenji1 left join menu_ on
fenji1._col0=menu_._col0 and fenji1._col15=menu_._col15 ;quit;


/*分类统计*/
/*月份*/
data month;set fenji2(keep=date _col1 _col2 code price vol _col11 xb csrq bs);
month=month(date);year=%eval(20&n);
csrq=input(put(strip(csrq),$8.),yymmdd8.);age=year-year(csrq);
if price~=. then ln_cje=log(price*vol);label ln_cje='成交额的对数';
run;
proc freq data=month;table month _col2 _col1 _col11 bs  xb;run;
proc univariate data=month noprint;
histogram date age;
histogram ln_cje/normal ;
where age>0;
run;quit;



proc export data=fenji_1
                outfile='e:\f1.xlsx'
                dbms=xlsx
                replace;label;
quit;
/*/*将4个2015年资金分级基金表合并*/*/
/*data fenji_2015;set fenji_zj1501 fenji_zj1502 fenji_zj1503 fenji_zj1504;;run;*/
/*data fenji_2015;set fenji_2015;if bs='' then bs='申';where stock~='恒立实业' and price~=.;run;*/
option;

data fenji1;set fenji;if find(_col1,substr(stock,1,4));run;

data t;set fenji_2015;if find(_col1,substr(stock,1,2)) and ~find(_col1,substr(stock,1,4));run;


data zy_;set zj1501(keep=zy rq);a1=scan(zy,2,'()');
date=input(put(strip(rq),$8.),yymmdd8.);;run;
proc sql;create table fenji as select * from menu1 left join zy_ on
zy_.a1=menu1._col0 ;quit;
data fenji;set fenji;where date~=.;run;
data fenji1;set fenji;stock=scan(zy,1,'(');stock1=compress(stock,,'d');
stock2=kcompress(stock1,'买卖');run;
data fenji1;set fenji1;if find(_col1,substr(stock2,1,2));run;



data t;set zj2;t=substr(t4,1,2);run;
proc freq data=fenji1;table _col2;run;

data t;set zj2;where t3=14;run;


data zy1;set zy1;where _col0~='';run;

proc freq data=wt1501;table jys;run;

quit;



proc sort data=menu1 out=z1 nodupkey;by _col0 _col15;run;

proc sort data=menu1 nouniquekeys out=z
;by _col0 ;run;
proc sort data=z nodup ;by _col0 _col15;run;

data t;set menu1;where _col0='150101';run;

proc sort data=t nodup;by  _col15;run;

data a; a='12345678'; b=reverse(substr(reverse(a),1,2)); put a=b=; run;
