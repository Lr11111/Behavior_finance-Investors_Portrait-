libname njzq 'e:\njzq';

/* 以1501数据集为例，2015年1、2月，提取出证券交易 */
data stock;set njzq.zj1501;where ywkm in('13001','13101');run;
data stock1;set stock;
informat code $6.;code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;
price=scan(zy,-1,'*');
date=input(put(strip(rq),$8.),yymmdd8.);format date yymmdd10.;drop rq;
run;

data code;set stock(keep=khh zy rq);code=scan(zy,2,'()');if missing(input(code,best32.)) then delete;run;
data code1;informat code $6.;set code(keep=khh code rq);run;
data code1;set code1;month=month(input(put(strip(rq),$8.),yymmdd8.));run;

/*将16分类数据进行合并*/
/*选出15年1、2月的*/
data cate;informat code $6.;set catecory;if year(month)=2015 and month(month) in (1,2);yue=month(month);;run;
proc sql;create table code2 as select * from code1 left join cate 
	on code1.code=cate.code and code1.month=cate.yue;quit;
data code2;set code2;keep code rq khh month catecory name;where catecory~=.;run;
proc sort data=code2;by month code;run;
data code2;set code2;
mean=floor(catecory/1000);
volatility=mod(floor(catecory/100),10);
skwness=mod(floor(catecory/10),10);
price=mod(catecory,10);run;
proc means data=code2 order=freq nway;class mean volatility skwness price;var month;where month=1;output out=t n=;run;
proc freq data=code2 order=freq;table mean volatility skwness price/list;; by month;run;

/*根据流通市值计算预期占比*/
proc means data=cate nway noprint;class yue catecory;var floatV;output out=floatV sum=sum ;run;
proc sql;
create table floatV1 as select *,sum(sum) as sum_mv,sum/calculated sum_mv as proportion label="预期权重" from floatv group by yue;
quit;

proc means data=code2 order=freq nway;class month catecory;var month;;output out=shiji n=n;run;
proc sql;
create table shiji1 as select *,sum(n) as sum_mv,n/calculated sum_mv as proportion1 label="实际权重" from shiji group by month;
quit;
/*合并实际权重、预期权重*/
proc sql;create table all as select * from floatV1,shiji1 
where floatv1.yue=shiji1.month and floatv1.catecory=shiji1.catecory;quit;
data all;retain month;set all(keep=month catecory proportion proportion1);rate=(proportion1-proportion)/proportion1;run;

proc sort data=all;by month descending rate;run;quit;
ODS HTML CLOSE;
ODS HTML;


data t1;set stock(keep=zy);code=scan(zy,2,'()');if missing(input(code,best32.)) then delete;run;
proc sort data=t1;by code;run;



/*读取出分级基金交易*/
proc datasets library=njzq noprint;
modify zj1501;
   label &zj_label;
run;quit;

/*找出资金表中的买卖单,对摘要进行提取出分级基金*/
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
data fenji_zj;set fenji;if find(_col1,substr(stock,1,2));where date<_col16 or _col16=.;run;









proc contents data=njzq.menu out=o1(keep=name label);
run;
proc means data=njzq.total1 noprint nway;by i;var _RMSE_;output out=z mean=;run;
data z1;merge z(in=a) month(where=(j=1));by i;if a;run;
proc gplot data=z1;plot _RMSE_*month;symbol i=j;run;quit;


proc means data=njzq.final noprint nway;by i;var skwness;output out=z mean=;run;
data z1;merge z(in=a) month(where=(j=1));by i;if a;run;
proc gplot data=z1;plot skwness*month;symbol i=j;run;quit;
%put 0.2487020572/55;


proc delete data= njzq.class;run;
