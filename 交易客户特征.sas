
data khxx;set njzq.khxx;
khrq=input(put(strip(khrq),$8.),yymmdd8.);
xhrq=input(put(strip(xhrq),$8.),yymmdd8.);
birth=input(csrq,yymmdd8.);
format khrq yymmdd10. xhrq yymmdd10. birth yymmdd10.;
drop csrq WTFSFW;label birth='��������';
run;

proc freq data=khxx noprint;table xhrq/out=z;run;
proc freq data=khxx;table khzt;run;


/*����֤������Ŀ*/
%label_(zjlb,7);
data zjlb;set zjlb;t= put(f1,$2.);run;
proc sql noprint;
select  distinct strip(t)||"='"||strip(F2)||"'"
   into : zjlb separated by " "
           from zjlb;
quit;
proc format;value label &zjlb;run;
data z;modify z;format zjlb label.;run;

/*�Ƿ�������khztΪ3��ʱ��������������*/
data t;set khxx;if xhrq>'01jan1990'd then xh=1;else xh=0;
drop khzt;run;


/******************************************************/
/* ��1501���ݼ�Ϊ����2015��1��2�£���ȡ��֤ȯ���� */
/*����֤��zy�������� �� * �ķ���*/
data stock;set zj1501;where ywkm in('13001','13101');run;
data stock1;set stock();
informat code $6.;code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;
price=scan(zy,-1,'*');p=input(price,8.);
vol=scan(zy,-2,'��)');v=input(vol,8.);
je=-1*fcje+srje;
date=input(put(strip(rq),$8.),yymmdd8.);
time=input(strip(fssj),time8.);
format date yymmdd10.  time time8.;
drop rq price vol srje fcje zy;
run;

%let mon=4 3 4 4 4 4 3 5 4;
%let m=2;      /*������*/
%let n=15;     /*������*/
%let m=%scan(&mon,%eval(&n-6)," ");   /*�����¸���*/


/*%macro zj;*/
%do m=1 %to &m;
FILENAME exam "I:\njzq_bdase4_bdf\������ͨzj\zj&n.0&m..DBF";
PROC DBF DB5=exam OUT=data;run;

data stock&n.0&m;set data(obs=10000);
informat code $6.;
zy=Tranwrd(zy,"(��)","");
code=scan(zy,2,'()');
if missing(input(code,best32.)) then delete;
price=input(scan(zy,-1,'*'),8.);
vol=input(scan(zy,-2,'��)'),8.);
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

/*����ͻ�Ⱥ���Ƿ�һ��:��ͬʱ�ڻ�ֵ���ͬ�Ŀͻ�Ⱥ��*/
proc sort data=data(keep=khh khqz yyb) nodupkey out=z;by khh yyb;run;
data z1;set z;if lag(khh)=khh ;run;
/*Ӫҵ��*/
/*����*/

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

/*��Ӧ��ּ�����Ľ���*/
data fenji;set njzq.fenji_2015(keep=khh code date);flag=1;run;

proc sql;create table stock2 as select * from stock1 left join fenji
on stock1.khh=fenji.khh and stock1.code=fenji.code;quit;


proc univariate data=stock1() noprint;

;histogram time;run;quit;

/*����ʱ��㼯����19-22�� δ�н���*/
proc freq data=stock1;table time;run;
