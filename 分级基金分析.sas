libname njzq 'e:\njzq';

/*取出第一个分级基金交易*/
proc sort data=a3(keep=khh date stock price bs vol) out=s;by khh date;run;
data s;set s;je=vol*price;run;



proc sort data=s nodupkey out=s1;by khh;run;


proc means data=z noprint ;class khh bs;var je;output out=z1 n=n sum=s;run;

/*取出各个客户交易的次数、总金额*/
proc means data=s noprint nway ;class khh;var je;output out=s2  sum=sum;run;
data s3;merge total1(in=a) s2(keep=khh sum in=b);by khh;if a and b;
label sum='交易总金额';run;

data flag;set s3(keep=khh);flag=1;run;
proc sort data=khxx;by khh;run;
data flag1;merge khxx(in=a) flag(in=b);by khh;if a ;run;
data flag1;set flag1;if flag=. then flag=0;run;
data flag2;set flag1;
format xb BEST12.;
csrq=input(put(strip(csrq),$8.),yymmdd8.);
if csrq~=. then age=2017-year(csrq);
if 19<age<106;
run;

proc means data=flag2 nway noprint;class flag;;output out=q ;run;

proc sort data=z nodupkey out=z_;by khh;run;
proc sql noprint;create table z2 as select * from khxx left join 

/**********************所有股票、基金的购买************************/
%let n=07;
%let m=1;
/*%macro zj;*/
/*%do m=1 %to &m;*/
FILENAME exam "I:\njzq_bdase4_bdf\交付普通zj\zj&n.0&m..DBF";
PROC DBF DB5=exam OUT=ZJ1501;run;

/*data data.zj&n.0&m;set zj1501;run;*/
proc datasets library=work noprint;
modify zj1501;
   label &zj_label;
run;quit;

/*找出资金表中的买卖单,对摘要进行提取*/
data zj;set zj1501(keep=rq ywkm khh zy srje fcje bczjye);informat code $6.;code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;

price=scan(zy,-1,'*');
date=input(put(strip(rq),$8.),yymmdd8.);format date yymmdd10.;drop rq;
run;
data zj;set zj;p=input(put(strip(price),$8.),8.);run;


data t;set zj;where missing(input(price,best32.));run;

data t1;set zj1501;where khh='025025014252';run;



proc sort data=t1;by ywkm;run;

data zj1;set zj;t1=scan(zy,1,'(');t2=substr(compress(t1,,'n'),1,2);
t3=find(t1,strip(t2));t4=substr(t1,t3,80-t3);
where code~='';run;
/*data zj2;set zj1;t1=scan(zy,1,'(');t2=substr(compress(t1,,'n'),1,2);*/
/*t3=find(t1,strip(t2));t4=substr(t1,t3,80-t3);run;*/
data zj3;set zj1;informat bs $2.;bs=compress(t4,'买卖','k');informat stock $20.;
if bs~='' then stock=substr(t4,3,20);drop t1-t4;run;


/*删除code一样但name不一样的、删除交易日期和到期日不符的*/
data fenji_zj&n.0&m;set fenji;if find(_col1,substr(stock,1,2));where date<_col16 or _col16=.;run;
%end;
%mend;
%zj;



if missing(input(x, best32.))




;
proc sort data=flag1 out=t nouniquekeys ;by khh;run;

proc freq data=flag2 noprint;table age/out=t;run;



/******业务科目分析label*/
%label_(ywkm,6);
proc freq data=njzq.zj1501(keep=ywkm) order=freq noprint;table ywkm/out=ywkm_;run;
data ywkm_;set ywkm_;ywkm1=input(ywkm,best32.);run;
proc sql noprint;create table ywkm2 as select * from ywkm_ left join ywkm on ywkm_.ywkm1=ywkm.f1;quit;

proc sort data=ywkm2;by descending count;run;

proc sort data=njzq.zj1501(keep=ywkm zy) nodupkey out=t;by ywkm;run;
proc sql noprint;create table ywkm3 as select * from ywkm2 left join t 
on ywkm2.ywkm=t.ywkm;quit;
data t;set ywkm3;where f1=.;run;




data t1;set zj2;where khh='000001012353';run;

proc sort data=zj1501(keep=rq fssj khh srje fcje bczjye) out=zj;by  khh;run;
data zj1;set zj;time=input(strip(fssj),time8.);format time time8.;run;
proc sort data=zj1;by khh rq fssj;run;

data zj1;set zj1;by khh;retain zj;if first.khh then zj=bczjye;else zj+srje-fcje;run;
data zj2;set zj1;ye=input(bczjye,best12.);run;

data t;set zj2;if abs(ye-zj)>10;;run;



data aa;
a=dhms(input(scan('2011-09-01 14:20:31',1,' '), yymmdd10.),0,0,
input(scan('2011-09-01 14:20:31',2,' '), time8.));
b=input('2011-09-01 14:20:31', b8601dt.);

format a b b8601dt.;
run;

/*freq统计:客户群组*/
proc freq data=zj1501 order=freq noprint;table khqz/out=khqz;run;

proc means data=zj1501 noprint ;class khqz;var bczjye;output out=yue mean=;run;

proc means data=zj1501 noprint ;id khqz;class khh;var bczjye;output out=yue max=;run;
proc means data=yue order=freq
 noprint;class khqz;var bczjye;output out=yue1 mean=;run;

 data yue;set yue;ye=log10(bczjye);if ye>1;run;
proc univariate  data=yue noprint;histogram ye/NOTABCONTENTS 
;class khqz;where khqz like '0%';quit;

option notes;
data t;set zj2;where khh='021000013272';run;



/*营业部*/
proc freq data=zj1501 order=freq noprint;table yyb/out=yyb;run;
