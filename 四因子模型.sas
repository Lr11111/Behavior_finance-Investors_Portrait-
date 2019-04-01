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
/********************滚动回归***********************/
/*根据回归的残差平方和、残差自由度来计算标准差*/
%let m=72;
%macro prg;
%do m=72 %to 200;
data t;set hebing;where i between %eval(&m-59) and &m;run;
proc reg data=t noprint SSE  outest=z;model rate=SMB mom mkt HML;by n ;run;
proc append base=total data=z;run;
%end;
%mend;
%prg;

data total1;set total;retain i 73;if n<lag(n) then i+1;
keep n _rmse_ intercept smb mom mkt hml _edf_ _sse_ _rsq_ i;run;
/*剔除自由度低的观测，比如股票n刚上市9个月，不足以回归出有效结论*/
data rmse;set total1;keep n _rmse_ i;where _edf_>=7;run;

/*计算mean return、skewness*/
%let m=72;
%macro prg1;
%do m=72 %to 200;
data t;set hebing;where i between %eval(&m-59) and &m;run;
proc means data=t noprint ;by n;var rate;output out=z mean=mean skewness=skwness;run;
proc append base=total_ data=z;run;
%end;
%mend;
%prg1;
data ret_ske;set total_;retain i 73;if n<lag(n) then i+1;where _freq_>=12;drop _type_ _freq_;run;

/*计算价格因素*/
data price;merge month(keep=i month) price;rename var1=price;run;
data price_;set price;retain n 1;if i<lag(i) then n+1;;run;
data price_;set price_;where price~=.;run;
proc sort data=price_;by i n;run;
/*将4个指标合并*/
data final;merge price_ rmse ret_ske;by i n;run;
data final;set final;where i>72;run;
data final1;set njzq.final;if cmiss(of _all_)=0;run;
/*和流通股数进行合并权重*/
proc sort data=njzq.data out=data;by i n;run;
data final2;merge final1(in=a) data(keep=i n float);by i n;if a;floatV=price*float;run;
/*16分类*/
proc rank data=final2 groups=2 out=fenlei;ranks meanf _rmse_f skwnessf pricef;
var mean _rmse_ skwness price;by month;run;
data fenlei;set fenlei;catecory=1000*(meanf+1)+100*(_rmse_f+1)+10*(skwnessf+1)+pricef+1;
drop meanf _rmse_f skwnessf pricef float ;label price='不复权价格' _rmse_='特质波动率' mean='平均收益' 
skwness='偏度' floatV='流通市值' catecory='16分类';run;
proc sort data=fenlei;by n month;run;
data fenlei1;merge fenlei njzq.index(rename=(i=n));by n;;run;

proc sort data=njzq.total1;by  n i;run;
data fenlei2;merge fenlei1 njzq.total1(drop=_SSE_);by n i;run;
PROC EXPORT DATA= WORK.fenlei2 
            OUTFILE= "E:\njzq\fenlei.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;


/*相关统计*/
proc means data=fenlei1 nway noprint;class month;var skwness;output out=z mean=;run;
proc gplot data=z; 
plot skwness*month;symbol i=j;
run;
quit;

data t;set fenlei;keep month catecory n floatV;rename n=stock;run;
/*和index合并*/
proc sql ;create table catecory as select * from t left join index on t.stock=index.i;quit;
data catecory;set catecory;drop i;run;

proc sort data=catecory;by stock month;run; 

data t;set hebing;where i between %eval(&m-59) and &m  and n=1;run;


data n1;set final1;where num=1;run;


data s;set total1;where _edf_<=9;run;
proc freq data=final1;table num;run;

proc means data=f3;var mom;run;
