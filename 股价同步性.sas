
/*导入3420股票2001.01-2017.10的月度数据：收盘价（前复）、流通股数、所有权益、市值*/
PROC IMPORT OUT= WORK.data 
            DATAFILE= "E:\njzq\data.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;
data data;set data;var3=input(var3,best32.);run;
/*导入期间HS300指数收益*/
PROC IMPORT OUT= WORK.HS300 
            DATAFILE= "e:\njzq\hs300.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="HS300$"; 
     GETNAMES=no;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
data hs300;set hs300;rename f1=month f2=index f3=index_rate;
format f1 yymmd7.;attrib _all_ label="";run;
/*导入不复权价格数据*/
PROC IMPORT OUT= WORK.price 
            DATAFILE= "E:\njzq\price.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;
/*导入股票index目录数据*/
PROC IMPORT OUT= WORK.index 
            DATAFILE= "E:\njzq\index.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;
data index;set index;rename var1=code var2=name;i+1;run;
/*导入期间treasure收益，数据是日度的，转化为月度的*/
PROC IMPORT OUT= WORK.treasure
            DATAFILE= "e:\njzq\hs300.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="treasure$"; 
     GETNAMES=yes;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
data treasure;set treasure;rename _col0=month _col1=treasure;attrib _all_ label="";format _col0 yymmd7.;run;
proc means data=treasure noprint;by month;var treasure;output out=treasure_(drop=_type_ _freq_) mean=;run;

/*合并hs300、treasure，计算市场超额收益*/
data temp;merge hs300(where=(year(month)>2001)) treasure_(where=(year(month)>2001));run;
data MKT;set temp;retain i 12;i+1;treasure1m=treasure/12;keep month index_rate treasure1m i;run;

/************************************************************/
/*构建3420股票202个月的时间表*/
data month;do j=1 to 3420;do i=1 to 202;month=intnx('month','31jan2001'd,i-1);output;end;end;format month yymmd7.;run;
data data1;merge month(keep=i month) data;rename var1=price var2=float var3=equity var4=cap;run;
data data1;set data1;if _n_=1 then equity=.;run;
/*对data1数据进行处理*/
/*计算收益率，标记股票，填补equity*/
data data2;set data1;retain e;retain n 1;
if month<lag(month) then do;e=equity;n+1;end;else rate=(price-lag(price))/lag(price);
if equity~=. then e=equity;run;
/*删除未上市的记录*/ 
/*data t;set data2;if price=. and lag(price)~=.;run;*/
data data2;set data2;drop equity;rename e=equity;where price~=.;run;
/*构建账面市值比因子：BM,，2001年的记录*/
data data;set data2;BM=equity/cap;where  year(month)~=2001;run;
/*根据每个月来分组，市值、账面市值比*/
proc sort data=data ;by month n;run;
proc rank data=data groups=2 out=f;ranks cap_;var cap;by month;run;
proc rank data=f groups=3 out=f1;ranks BM_;var BM;by month;run;

/*分组后组间收益差*/
/*HML*/
proc means data=f1 noprint nway;class month BM_;var rate;output out=f2 mean=;run;
data f3;do i=1 to 3;array r(3);set f2;r(i)=rate;;end;run;
data f3;set f3;HML=r1-r3;keep month hml;run;
/*SMB*/
proc means data=f1 noprint nway;class month cap_;var rate;output out=f4 mean=;run;
data f5;do i=1 to 2;array r(2);set f4;r(i)=rate;;end;run;
data f5;set f5;SMB=r2-r1;keep month SMB;run;
proc univariate data=f5;var SMB;where month>'01jan2008'd;run;

/*将HML SMB的数据合并*/
data hebing;merge data f3 f5;by month;run;
data hebing1;set hebing;;keep i month rate n HML SMB;run;

proc datasets lib=work  nolist;
   delete f: /memtype=data;
quit;
/*将市场收益、五风险收益引入*/
proc sql noprint;create table hebing2 as select * from hebing1 left join mkt
on hebing1.i =mkt.i;quit;

proc sort data=hebing2;by n i;run;

/************************将动量因子MOM引入**********************/
data mom;set data2(keep=i rate n);run;
/*计算各个股票前12个月的平均收益率*/
proc expand data =mom out =mom1  method = none;
by n; id i;
convert rate =rate_12/transformout= (movavg 12 );  ***  convert value=value1 / transform=(movmax 5 trimleft 4);
run;
data mom2;set mom1;by n;rate12=lag(rate_12);if first.n then call missing(rate12);run;
/*根据前12月平均收益率对各个月份分为3组*/
proc sort data=mom2;by i n;run;
proc rank data=mom2 groups=3 out=f;ranks rate12_;var rate12;by i;run;
/*分组求和*/
proc means data=f noprint nway ;class i rate12_;var rate;output out=f2 mean=;run;
data f3;do j=1 to 3;array r(3);set f2;r(j)=rate;;end;run;
data f3;set f3;MOM=r1-r3;keep i mom;run;
/*合并*/
proc sql noprint;create table hebing3 as select * from hebing2 left join f3
on hebing2.i =f3.i;quit;

proc sort data=hebing3;by n i;run;
data hebing;set hebing3;MKT=(index_rate-treasure1m)/100;drop index_rate treasure1m;run;
/***************************************************/
/********************滚动回归***********************/
PROC IMPORT OUT= WORK.hebing 
            DATAFILE= "E:\stock\siyinzi\wuyinzi.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.hebing 
            DATAFILE= "E:\stock\siyinzi\sanyinzi.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;
data hebing1;set hebing;rename var1=date var2=ticker var3=rate var4=MKT var5=SMB var6=HML var7=RMW var8=CMA;run;
data hebing1;informat code $6.;set hebing1;date1=input(ticker+1000000,$12.);code=substr(date1,2,7);drop ticker date1;run;
data hebing2;set hebing1;retain i;if lag(date)~=date then i+1;run;
proc sort data=hebing2;by code date;run;

/**/
proc means data=hebing2 nway noprint;var mkt smb hml rmw cma;class date;output out=z mean=;run;

proc reg data=hebing2 noprint SSE  outest=z;model rate=SMB  mkt HML rmw cma;by code ;run;

proc univariate data=z;histogram _rsq_;where _rsq_~=1;title '五因子回归R2分布';run;

proc univariate data=z;histogram _sse_;where _sse_<8;title '五因子回归R2分布';run;

proc tabulate data=z ;
var Intercept smb mkt hml rmw cma;
table (Intercept smb mkt hml rmw cma),
      (n mean max min std);
run;

/*根据回归的残差平方和、残差自由度来计算标准差*/

%let m=60;
%macro prg;
%do m=60 %to 123;
data t;set hebing2;where i between %eval(&m-59) and &m;run;
proc reg data=t noprint SSE  outest=z;model rate=SMB  mkt HML;by code ;run;
proc append base=total data=z;run;
%end;
%mend;
%prg;


data total1;set total;retain i 60;if code='000001' then i+1;
keep code _rmse_ intercept smb mom mkt hml _edf_ _sse_ _rsq_ i;run;
/*剔除自由度低的观测，比如股票n刚上市9个月，不足以回归出有效结论*/
data RSQ;set total1;keep code _RSQ_ i;where _edf_>=7;run;


data final;set final2;keep industry1 year i date n attr state ln_MV holdPct holdpct_1st 
		turnoverRate avgTurnoverRate code name;where 2012<year<2018;run;

 proc means data=final nway noprint;id industry1 i date n attr state name;class code year;
	var ln_MV holdPct holdpct_1st avgTurnoverRate;output out=final1 mean=;run;

/*201301对应145 61*/
data t;do i=61 to 128;month=intnx('month','01jan2013'd,i-61);output;end;;format month yymmd7.;run;

data t1;merge rsq(in=a) t;by i;if a;run;
data t1;set t1;year=year(month);run;

proc means data=t1 nway noprint;class code year;var _RSQ_;output out=t2(drop=_type_ _freq_) mean=;run;
data final1;merge final1(in=a) t2;by code year;if a ;run;

/******************** 导入 速动比率 资产负债率等指标************************/
PROC IMPORT OUT= WORK.zhibiao 
            DATAFILE= "E:\stock\siyinzi\zhibiao.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=yes;
     DATAROW=2; 
RUN;

data zhibiao;informat code $6.;set zhibiao;date1=input(ticker+1000000,$12.);code=substr(date1,2,7);drop _ ticker date1;run;

data final2;merge final1(in=a) zhibiao;if a;run;

PROC EXPORT DATA= WORK.final2 
            OUTFILE= "E:\stock\siyinzi\final2.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;

/*********************************************************/
/*********************************************************/
/*********************************************************/
libname paper 'e:\stock';
data final2;set paper.final2;run;

/*计算股价同步性的回归得到 R2 */
PROC IMPORT OUT= WORK.hebing 
            DATAFILE= "E:\stock\siyinzi\sanyinzi.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=NO;
     DATAROW=2; 
RUN;
data hebing1;set hebing;rename var1=date var2=ticker var3=rate var4=MKT var5=SMB var6=HML;run;
data hebing1;informat code $6.;set hebing1;date1=input(ticker+1000000,$12.);code=substr(date1,2,7);drop ticker date1;run;
data hebing2;set hebing1;retain i;if lag(date)~=date then i+1;run;
proc sort data=hebing2;by code date;run;

/*各个月各个行业的平均收益率*/
data ind;set final2;keep code date industry1;run;

data hebing2;set hebing2;keep code date i rate MKT;date=intnx('month',date,0,'B');run;
data hebing3;merge hebing2(in=a) ind(in=b);by code date;if a and b;run;
data hebing3;retain industry;set hebing3;;if industry1~='' then industry=industry1;drop industry1;label industry='行业';run;
proc means data=hebing3 nway noprint;var rate;class date industry;output out=z(drop=_type_)
	mean=;run;

data z;set z;where _freq_>5;rename rate=rateM;run;

proc sort data=hebing3;by date industry;run;

data hebing4;merge hebing3(in=a) z;by date industry;if a;run;

proc sort data=hebing4;by code date;run;


/*根据回归的残差平方和、残差自由度来计算标准差*/
%let m=60;
%macro prg;
%do m=60 %to 123;
data t;set hebing4;where i between %eval(&m-59) and &m;run;
proc reg data=t noprint SSE  outest=z;model rate=mkt rateM;by code ;run;
proc append base=total data=z;run;
%end;
%mend;
%prg;


data total1;set total;retain i 60;if code='000001' then i+1;
keep code _rmse_ intercept   mkt hml _edf_ _sse_ _rsq_ i;run;
/*剔除自由度低的观测，比如股票n刚上市9个月，不足以回归出有效结论*/
data RSQ;set total1;keep code _RSQ_ i;where _edf_>=7;run;


/*201301对应145 61*/
data t;do i=61 to 128;month=intnx('month','01jan2013'd,i-61);output;end;;format month yymmd7.;run;

data t1;merge rsq(in=a) t;by i;if a;run;
data t1;set t1;year=year(month);run;
data t1;set t1;synch=log(_rsq_/(1-_rsq_));run;

proc means data=t1 nway noprint;class month;var _RSQ_ synch;output out=t2(drop=_type_ _freq_) mean=;run;

/*行业*/


data final;set final2;keep industry1 year i date n attr state ln_MV holdPct holdpct_1st 
		turnoverRate avgTurnoverRate code name;;run;

proc sort data=t1;by code month;run;
data final1;merge t1(in=a rename=(month=date)) final ;by code date;if a ;run;
proc sort data=final1;by code year;run;
proc sort data=zhibiao;by code year;run;

data final2;merge final1(in=a) zhibiao;by code year;if a;run;

data paper.tongbuxing;set final2;run;
PROC EXPORT DATA= paper.tongbuxing 
            OUTFILE= "E:\stock\siyinzi\tongbuxing.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;

/*****************************************************/
data z;set final2;where date='01dec2016'd;run;
ods html close;ods html ;
proc tabulate data=z ;
var _rsq_ synch holdpct holdpct_1st ln_mv avgTurnoverRate pb taturnover currenTRatio asseTLiabRatio;
table ( _rsq_ synch holdpct holdpct_1st ln_mv avgTurnoverRate pb taturnover currenTRatio asseTLiabRatio),
      (n mean max min std);
run;


proc freq data=hebing;table var1;run;

/************************************************************************************/
/************************************************************************************/
libname paper 'e:\stock';
data final2;set paper.tongbuxing;run;

/*不同资本系r2走势*/
proc means data=final2 nway noprint;var _rsq_ synch;class date attr;output out=a1 
	mean=;run;
data a1;set a1;where attr in ('地方国有企业','无资本系','中央国有企业','民营企业');run;
proc tabulate data=a1 format=8.4;;
var _rsq_;class date attr;
table date,attr*_rsq_;
run;
proc tabulate data=a1 format=8.4;;
var synch;class date attr;
table date,attr*synch;
run;

/*简单回归*/
proc glm data=final2;
model synch=state/solution ss1 ss2 ss3 ss4 ;;quit;

proc glm data=final2;class industry1 year;
model synch=state holdpct holdpct_1st industry1 year/solution ss1 ss2 ss3 ss4 ;;quit;

proc glm data=final2;class industry1 year;
model synch=state holdpct holdpct_1st  ln_mv avgTurnoverRate 
 pb taturnover currenTRatio asseTLiabRatio industry1 year/solution ss1 ss2 ss3 ss4 ;;quit;



 proc sort data=final2;by code year;run;
PROC IMPORT OUT= jixiao/*导入数据*/
            DATAFILE= "E:\stock\风险承担\第五章\绩效数据.xlsx"
            DBMS=excel REPLACE;
     GETNAMES=YES; 
RUN;

data jixiao1;
set jixiao;
keep code year wn dz soe zl lq;
run;
data jixiao1;informat code $6.;set jixiao1;code=substr(code,1,6);run;

data final3;merge final2(in=a) jixiao1;by code year;if a;run; 
data final3;set final3;where lq~=.;run;

data roa;set paper.final2;keep code date roa;run;
data final3;merge final3(in=a) roa ;by code date;if a;run;

/**************************************************************/
proc glm data=final3;class industry1 year;
model synch=state holdpct holdpct_1st  ln_mv avgTurnoverRate 
 pb taturnover currenTRatio asseTLiabRatio industry1 year/solution  ;;quit;


proc sort data=final4(keep=code) out=z nodupkey;by code;run;

proc tabulate data=final3 format=8.4;;
var synch _rsq_;class year attr;
table (_rsq_ synch),attr*(mean n);
run;

proc glm data=final3;class attr(ref='无资本系');
model synch=attr/solution  ;;quit;

proc glm data=final3;class industry1 year;
model synch=state roa 
 pb taturnover currenTRatio asseTLiabRatio industry1 year/solution  ;;quit;

proc glm data=final3;class industry1 year;
model state=roa 
 pb taturnover currenTRatio asseTLiabRatio /solution  ;;quit;


proc means data=final3 nway noprint;var holdPct holdpct_1st;class date attr;output out=a1 
	mean=;run;

data a1;set a1;where attr in ('地方国有企业','无资本系','中央国有企业','民营企业');run;
proc tabulate data=a1 format=8.4;;
var holdPct;class date attr;
table date,attr*holdPct*mean;
run;
proc tabulate data=a1 format=8.4;;
var holdpct_1st;class date attr;
table date,attr*holdpct_1st*mean;
run;

proc tabulate data=final3 format=8.4;;
var holdPct holdpct_1st;class  attr;
table (holdPct holdpct_1st),attr*(mean n);
run;

proc freq data=final3;table zl*dz;run;

proc sql;
	create table z as select *,sum(dz) as sum,calculated sum/count(*) as proportion 
	from final3 group by attr;
	quit;
data z1;set z;keep attr proportion;run;
proc sort data=z1 nodupkey;by attr;run;

proc sql;
	create table z as select *,sum(zl) as sum,calculated sum/count(*) as proportion 
	from final3 group by attr;
	quit;
data z1;set z;keep attr proportion;run;
proc sort data=z1 nodupkey;by attr;run;

data t;set final3(keep=attr year lq);if lq>0 then di_f=1;else di_f=0;run;
proc sql;
	create table z as select *,sum(di_f) as sum,calculated sum/count(*) as proportion 
	from t group by attr,year;
	quit;
data z1;set z;keep attr year proportion;run;
proc sort data=z1 nodupkey;by attr year;run;


data final4;set final3;where state=1;run;

proc glm data=final4;class industry1 year;
model synch=holdpct_1st holdpct industry1 year/solution  ;;quit;


proc glm data=final4;class industry1 year;
model synch=holdpct_1st holdpct zl dz lq industry1 year/solution  ;;quit;

/*****************************************/
/*****************************************/
data final3;set final3;rename pb=PB ln_mv=SIZE taTurnover=TurnoverRate asseTLiabRatio=Leverage;
	drop turnoverRate avgturnoverRate;run;

/*控制变量回归*/
proc glm data=final3;class industry1 year;
model synch=holdpct_1st*state state holdpct_1st
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;
proc glm data=final3;class industry1 year;
model _rsq_ =holdpct_1st*state state holdpct_1st
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;

/***********  x的取值：lq zl dz holdpct  **************/
%let x=lq;
proc glm data=final3;class industry1 year;
model synch=&x*state state &x
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;
proc glm data=final3;class industry1 year;
model _rsq_ =&x*state state &x
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;

/* proc glm data=final4;class industry1 year;*/
/*model synch=holdpct_1st holdpct zl dz lq ln_mv avgTurnoverRate */
/* pb industry1 year/solution  ;;quit;*/


proc glm data=final3;class industry1 year;
model synch=state 
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;
proc glm data=final3;class industry1 year;
model _rsq_ = state
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;

data t;set final3;if attr in ('地方国有企业','中央国有企业') then flag_=1;
	if attr in ('民营企业','外资企业') then flag_=0;if flag_~=. then output;run;
proc glm data=t;class industry1 year;
model synch=flag_ 
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;
proc glm data=t;class industry1 year;
model _rsq_ = flag_
size TurnoverRate Leverage pb industry1 year/solution  ;;quit;

ods html close;ods html ;


data f6;merge f6(in=a) jixiao1;by code year;if a;run; 
data f6;set f6;where lq~=.;run;


/*导入信息透明度指标*/
PROC IMPORT OUT= WORK.a 
            DATAFILE= "e:\stock\2017年深交所信息披露考评.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="主板企业$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

PROC IMPORT OUT= WORK.a1 
            DATAFILE= "e:\stock\深交所信息披露考评2001-2016.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

proc append data=a(drop=_col1) base=a1(drop=_col1);run;

data a2;set a1;drop _col1;
if _col2='A' then TRA=5;
if _col2='B' then TRA=4;
if _col2='C' then TRA=3;
if _col2='D' then TRA=2;
run;
/*计算a2信息透明度分类统计*/
proc freq data = a2;table _col3*_col2/NOCOL NOPERCENT ;where _col3>2007;run;
proc sort data = a2;by _col0 _col3;run;

data final;merge  final3(in=a) a2(rename=(_col0=code _col3=year));by code year;if a ;run;

/**********************************************************/
/*winstor缩尾调整*/
proc univariate data=final noprint;  *//查找1％、99％;
   var synch _rsq_ holdpct_1st holdpct lq size TurnoverRate Leverage pb ;
   output out=temp p1=synch_1 _rsq__1 holdpct_1st_1 holdpct_1 lq_1 size_1 TurnoverRate_1 Leverage_1 pb_1 
	p99=synch_2 _rsq__2 holdpct_1st_2 holdpct_2 lq_2 size_2 TurnoverRate_2 Leverage_2 pb_2;
run;

proc sql;
   create table final_ as select final.state,final.zl,final.dz,final.tra,final.year,final.industry1,
						final.date,final.attr,final.code,
   case
   when(synch < synch_1) then synch_1
   when(synch > synch_2)  then synch_2
   else synch
   end as synch,

   case
   when(_rsq_<_rsq__1) then _rsq__1
   when(_rsq_>_rsq__2)  then _rsq__2
   else _rsq_
   end as _rsq_,

   case
   when(holdpct_1st<holdpct_1st_1) then holdpct_1st_1
   when(holdpct_1st>holdpct_1st_2)  then holdpct_1st_2
   else holdpct_1st
   end as holdpct_1st,
      case
   when(lq<lq_1) then lq_1
   when(lq>lq_2)  then lq_2
   else lq
   end as lq,

      case
   when(holdpct<holdpct_1) then holdpct_1
   when(holdpct>holdpct_2)  then holdpct_2
   else holdpct
   end as holdpct,
   case
   when(size<size_1) then size_1
   when(size>size_2)  then size_2
   else size
   end as size,
   case
   when(pb<pb_1) then pb_1
   when(pb>pb_2)  then pb_2
   else pb
   end as pb,
   case
   when(turnoverrate<turnoverrate_1) then turnoverrate_1
   when(turnoverrate>turnoverrate_2)  then turnoverrate_2
   else turnoverrate
   end as turnoverrate,
   case
   when(Leverage<Leverage_1) then Leverage_1
   when(Leverage>Leverage_2)  then Leverage_2
   else Leverage
   end as Leverage
   from final,temp;
quit;

/******************* 引入行业数据 *********************/
data WORK.INDUSTRY    ;
infile 'E:\bank\industry.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
   informat _ best32. ;
   informat ticker $6. ;
   informat intoDate yymmdd10. ;
   informat industryName1 $8. ;
   informat isNew best32. ;
   format _ best12. ;
   format ticker $6.;
   format intoDate yymmdd10. ;
   format industryName1 $8. ;
   format isNew best12. ;
input
            _
            ticker
            intoDate
            industryName1 $
            isNew
;run;
data t;set industry;where isnew=1;run;

proc sort data=final;by code;run;
data final;merge final(in=a) t(keep=ticker industryName1 rename=(ticker=code));by code;if a;run;
data final;set final;
	rename industryName1=industry1 holdpct_1st=Holdpct holdpct=Herfindhal
		attr = attribution soe=attr;drop industry1;
label soe='attr';
run;

/*数据样本极端值的调整*/
data t;set final;h_s = herfindhal * state;run;
proc rank data=t groups=40 out=t;ranks f1;var h_s;run;
proc rank data=t groups=40 out=t;ranks f2;var synch;run;
data t;set t;where not ((f1=0 and f2=39) or (f1=39 and f2=0));run;
data final;set t;dz = 1-dz;run;

data paper.tongbuxing_1;set final;run;
/*导出同步性数据成csv*/
PROC EXPORT DATA= final
            OUTFILE= "E:\stock\tongbuxing_psm.csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;
/************************描述性统计*************************/
proc tabulate data=final_ ;
var synch _rsq_ state holdpct_1st holdpct lq zl dz  size TurnoverRate Leverage pb;
table (synch _rsq_ state holdpct_1st holdpct lq zl dz  size TurnoverRate Leverage pb),
      (n mean max min std);
where date = '01dec2016'd;
run;

/************************设置哑变量*************************/
proc glmmod data=final prefix=year outdesign=year noprint;
    class year;
    model year = year/noint;run;
data year;set year;drop year year4;run;
proc glmmod data=final prefix=industry1 outdesign=industry noprint;
    class industry1;
    model synch = industry1/noint;run;
data industry;set industry;drop synch industry27;run;

data final_;set final;keep state zl dz tra date attr code 
synch _rsq_ state holdpct_1st holdpct lq zl dz  size TurnoverRate Leverage pb;run;
data final_;merge final_ year industry;run;

/***************保存数据*************/
libname paper 'e:\stock';
data paper.tongbuxing_;set final_;run;
data final_;set  paper.tongbuxing_;run;

data final;set  paper.tongbuxing_1;run;

/****************相关性****************/
data t;set final;temp=attr*state;run;
proc freq data=t;table state*attr;run;

proc corr data=final;var herfindhal state;run;

%let x=lq;

data t;set final_;&x._state = &x*state;run;
proc reg data=t  plots=none  noprint ADJRSQ RSQUARE outest=est tableout;
synch:model synch=&x._state state &x  TurnoverRate Leverage pb ;
_rsq_:model synch=&x._state state &x  TurnoverRate Leverage pb ;
quit;


ods html file="E:\stock\风险承担\1.xls";
proc glm data=t outstat=z ;
model synch=&x._state state &x size TurnoverRate Leverage pb /solution;;quit;
proc glm data=t outstat=z ;
model _rsq_=&x._state state &x size TurnoverRate Leverage pb /solution;;quit;



ods html file="E:\stock\风险承担\同步性回归参数.xls";
/*对变量进行state的初步回归*/
proc glm data=final;model synch=state/solution;quit;
proc glm data=final;model synch=state  TurnoverRate Leverage pb roa/solution;quit;
proc glm data=final;class industry1 year;model synch=state   TurnoverRate Leverage pb roa industry1 year/solution;quit;

proc glm data=final;model _rsq_=state  /solution;quit;
proc glm data=final;model _rsq_=state  TurnoverRate Leverage pb roa/solution;quit;
proc glm data=final;class industry1 year;model _rsq_=state  TurnoverRate Leverage pb roa industry1 year/solution;quit;

/*对股权变量进行回归*/
%let n=8;
%macro prg;
%let var=tra lq zl wn dz holdpct herfindhal;
%do n=1 %to 7;
%let x=%scan(&var,&n," "); 
proc glm data=final;
model synch=&x*state state &x
/solution;quit;
proc glm data=final;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa
/solution;quit;
proc glm data=final;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa industry1 year
/solution;quit;

proc glm data=final;
model _rsq_=&x*state state &x
/solution;quit;
proc glm data=final;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb roa
/solution;quit;
proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb roa industry1 year
/solution;quit;
%end;
%mend;
%prg;

%let x=attr;
proc glm data=final;model synch=&x*state state/solution;quit;
proc glm data=final;model synch=&x*state state TurnoverRate Leverage pb roa/solution;quit;
proc glm data=final;class industry1 year;model synch=&x*state state TurnoverRate Leverage pb roa industry1 year/solution;quit;

proc glm data=final;model _rsq_=&x*state state /solution;quit;
proc glm data=final;model _rsq_=&x*state state TurnoverRate Leverage pb roa/solution;quit;
proc glm data=final;class industry1 year;model _rsq_=&x*state state TurnoverRate Leverage pb roa industry1 year/solution;quit;

/************************************************/
proc freq data=final;table state*dz;run;
proc means data=final;class dz;var synch;run;
proc corr data=final NOSIMPLE noprob;var state tra lq zl wn dz holdpct herfindhal attr ;run;

ods html close;

*';*";*); */;%mend;run;

/***********************************************************/
/************************** psm ****************************/
PROC IMPORT OUT= WORK.psm 
            DATAFILE= "E:\stock\tbx_psm.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=yes;
     DATAROW=2; 
RUN;

data final;set final(drop=n);n+1;run;
data psm;set psm;n=var1+0;run;
data psm;merge psm(in=a) final(keep=n industry1 year synch _rsq_ );by n;if a;run;


/*psm样本的统计性描述*/
proc tabulate data=psm ;
var synch _rsq_ state attr tra lq zl wn dz holdpct herfindhal TurnoverRate Leverage pb   roa;
table (synch _rsq_ state attr tra lq zl wn dz holdpct herfindhal TurnoverRate Leverage pb  roa),
      (n mean max min std);
run;

ods html file="E:\stock\风险承担\同步性回归参数psm.xls";
/*对变量进行state的初步回归*/
proc glm data=psm;model synch=state/solution;quit;
proc glm data=psm;model synch=state TurnoverRate Leverage pb roa/solution;quit;
proc glm data=psm;class industry1 year;model synch=state  TurnoverRate Leverage pb roa industry1 year/solution;quit;


/*对股权变量进行回归*/
%macro prg;
%let var=tra lq zl wn dz holdpct herfindhal;
%do n=1 %to 7;
%let x=%scan(&var,&n," "); 
proc glm data=psm;
model synch=&x*state state &x
/solution;quit;
proc glm data=psm;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa
/solution;quit;
proc glm data=psm;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa industry1 year
/solution;quit;
%end;
%mend;
%prg;

%let x=attr;
proc glm data=psm;model synch=&x*state state/solution;quit;
proc glm data=psm;model synch=&x*state state TurnoverRate Leverage pb roa/solution;quit;
proc glm data=psm;class industry1 year;model synch=&x*state state TurnoverRate Leverage pb roa industry1 year/solution;quit;


ods html file="E:\stock\风险承担\同步性回归参数1.xls";
%let n=7;%let var=tra lq zl dz wn holdpct herfindhal attr;%let x=%scan(&var,&n," "); 
proc glm data=final;class industry1 year;
model synch=&x*state state &x
/solution;quit;
 proc glm data=final;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa  /solution;quit;
proc glm data=final;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb roa industry1 year /solution;quit;
proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 /solution;quit;
 proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb roa  /solution;quit;
proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb roa industry1 year /solution;quit;

 %let x=roa;
proc glm data=final;class industry1 year;
model synch=&x*state state &x
/solution;quit;
 proc glm data=final;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb  /solution;quit;
proc glm data=final;class industry1 year;
model synch=&x*state state &x
 TurnoverRate Leverage pb  industry1 year /solution;quit;
proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 /solution;quit;
 proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb  /solution;quit;
proc glm data=final;class industry1 year;
model _rsq_=&x*state state &x
 TurnoverRate Leverage pb  industry1 year /solution;quit;


ods html ;
ods html close;



%let x=zl;
proc glm data=final;class industry1 year;model synch=&x*state state &x TurnoverRate Leverage pb roa industry1 year/solution;quit;
