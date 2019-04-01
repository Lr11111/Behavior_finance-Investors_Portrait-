
data khxx;set njzq.khxx;
khrq=input(put(strip(khrq),$8.),yymmdd8.);
xhrq=input(put(strip(xhrq),$8.),yymmdd8.);
birth=input(csrq,yymmdd8.);
format khrq yymmdd10. xhrq yymmdd10. birth yymmdd10.;
drop csrq WTFSFW;label birth='出生日期';
run;

proc freq data=khxx noprint;table xhrq/out=z;run;
proc freq data=khxx;table khzt;run;


/*导入证件类别科目*/
%label_(zjlb,7);
data zjlb;set zjlb;t= put(f1,$2.);run;
proc sql noprint;
select  distinct strip(t)||"='"||strip(F2)||"'"
   into : zjlb separated by " "
           from zjlb;
quit;
proc format;value label &zjlb;run;
data z;modify z;format zjlb label.;run;

/*是否销户，khzt为3的时候，销户日期有误*/
data t;set khxx;if xhrq>'01jan1990'd then xh=1;else xh=0;
drop khzt;run;


/******************************************************/
/* 以1501数据集为例，2015年1、2月，提取出证券交易 */
/*经验证，zy都包含： 股 * 的符号*/
data stock;set zj1501;where ywkm in('13001','13101');run;
data stock1;set stock();
informat code $6.;code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;
price=scan(zy,-1,'*');p=input(price,8.);
vol=scan(zy,-2,'股)');v=input(vol,8.);
je=-1*fcje+srje;
date=input(put(strip(rq),$8.),yymmdd8.);
time=input(strip(fssj),time8.);
format date yymmdd10.  time time8.;
drop rq price vol srje fcje zy;
run;

%let mon=4 3 4 4 4 4 3 5 4;
%let m=2;      /*定义月*/
%let n=15;     /*定义年*/
%let m=%scan(&mon,%eval(&n-6)," ");   /*该年月个数*/


/*%macro zj;*/
%do m=1 %to &m;
FILENAME exam "I:\njzq_bdase4_bdf\交付普通zj\zj&n.0&m..DBF";
PROC DBF DB5=exam OUT=data;run;

data stock&n.0&m;set data(obs=10000);
informat code $6.;
zy=Tranwrd(zy,"(手)","");
code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;
price=input(scan(zy,-1,'*'),8.);
vol=input(scan(zy,-2,'股)'),8.);
je=-1*fcje+srje;
date=input(put(strip(rq),$8.),yymmdd8.);
time=input(strip(fssj),time8.);
format date yymmdd10.  time time8.;
drop rq zy srje fcje  fssj ywkm;
where ywkm in('13001','13101');
run;

proc datasets library=work noprint;
modify data;
   label &zj_label;
run;quit;

/*检验客户群组是否一致:不同时期会分到不同的客户群组*/
proc sort data=data(keep=khh khqz yyb) nodupkey out=z;by khh yyb;run;
data z1;set z;if lag(khh)=khh ;run;
/*营业部*/
/*检验*/

data stock&n.0&m;set data(obs=100000);
;code=scan(zy,2,'()');
if missing(input(code,best32.)) then output;
where ywkm in('13001','13101');
run;

data t;set stock1;if missing(input(code,best32.));run;

data t;set stock1;where srje=0 or fcje=0;run;

data t;set stock1;where p is null;
where same and v is null ;run;

data t;set stock1;cha=abs(abs(je)-p*v);;run;

/*对应起分级基金的交易*/
data fenji;set njzq.fenji_2015(keep=khh code date);flag=1;run;

proc sql;create table stock2 as select * from stock1 left join fenji
on stock1.khh=fenji.khh and stock1.code=fenji.code;quit;


proc univariate data=stock1() noprint;

;histogram time;run;quit;

/*发生时间点集中在19-22点 未有介绍*/
proc freq data=stock1;table time;run;
