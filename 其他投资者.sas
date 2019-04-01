libname njzq 'e:\njzq';libname njzq2 'e:\njzq2';
libname save 'I:\njzq_bdase4_bdf\save';


/*ͳ�Ʋ��뽻�׵�khh����*/
%let year=07 08 09 10 11 12 13 14 15;
%let t=7;
%macro prg2;
%do t=7 %to 15;
%let m=%scan(&year,%eval(&t-6)," "); 
proc means data=njzq.stock&m(keep=khh) nway noprint;class khh;output out=t;run;
proc append base=khh data=t;run;
%end;
%mend;
%prg2;

proc means data=njzq2.khh(rename=(_freq_=count)) nway noprint;class khh;var count;
	output out=khh1 sum=;run;

proc freq data=khh1 noprint;table count/out=t OUTCUM ;run;
data t;set t;log_count=log10(count);run;
proc gplot data=t;plot cum_pct*log_count;symbol i=j;run;

proc univariate data=khh2;var count;run;quit;
data khh1;set khh1;where count>5;run;

/*��ȥ�ּ������׵ģ�ȡ�*/
proc sort data=njzq.fenji(keep=khh) nodupkey out=khh_fj;by khh;run;
data khh_fj;set khh_fj;flag=1;run;
proc sql;create table khh2 as  select * from khh1 left join khh_fj on khh1.khh=khh_fj.khh;quit;
data khh2;set khh2;where flag=.;drop _type_ flag;run;


/*������������������Ͷ����51w����¼1.7�ڣ����Ϊ10�ݣ����μ���*/
data khh_1;set khh2(obs=50000);run;
data khh_2;set khh2(firstobs=50001 obs=100000);run;
data khh_3;set khh2(firstobs=100001 obs=150000);run;
data khh_e;set khh2(firstobs=150001);run;

%let n=11;
%macro prg2;
%do n=4 %to 4;
data khh_&n;set khh2(firstobs=%eval(50000*(&n-1)+1) obs=%eval(50000*&n));run;
%do t=7 %to 15;
%let m=%scan(&year,%eval(&t-6)," "); 
proc sort data=njzq.stock&m out=z;by khh;run;
data z1;merge z(in=a) khh_&n(in=b);by khh;if a and b;run;
proc append base=save.kh_&n data=z1;run;
%end;
%end;
%mend;
%prg2;

%macro prg3;
%do t=7 %to 15;
%let m=%scan(&year,%eval(&t-6)," "); 
proc sort data=njzq.stock&m out=z;by khh;run;
%do n=5 %to 11;

data khh_&n;set khh2(firstobs=%eval(50000*(&n-1)+1) obs=%eval(50000*&n));run;
data z1;merge z(in=a) khh_&n(in=b);by khh;if a and b;run;
proc append base=save.kh_&n data=z1;run;
%end;
%end;
%mend;
%prg3;

%let i=4;

%macro prg4;
%do i=11 %to 11;
proc sort data=save.kh_&i(drop=_freq_ count) out=data;by khh code date time;run;

data kh;retain n;set data;
if lag(khh)~=khh or lag(code)~=code then n+1;
/*if  n<10001 then output;*/
run;
data kh_;set kh;retain priceM cost;
by n;if first.n then do;value=0;volum=0;priceM=price;cost=0;end;
if je<=0 then do;
	value+(-vol*price);cost+vol*price;
	volum+vol;priceM=cost/volum;bs='b';end;
else do;
	volum+(-vol);value+(vol*price);cost=volum*priceM;bs='s';end;
balance=volum*price;
if last.n then return=value+balance;
label value='����Ʒ���ʽ�����' volum='����Ʒ�ֳ������䶯' balance='����Ʒ����ֵ�䶯'
      return='����Ʒ�ָ���ӯ��';
order+1;
drop bczjye khqz je;
run;

proc sql;create table kh_ as
select *,min(volum) as minV
from kh_ group by n 
having minV>=0;quit;
proc sort data=kh_;by order;run;

/*����ʱ״������ʵ��ӯ������ʵ�ֿ���*/
data kh_1;set kh_;if bs='s' then do;
	if price>priceM then PL='P';else PL='L';
	realize=vol*abs(price-priceM); end;run;

/***************************����ЧӦDE**************************/
data de;set kh_1;dateM=intnx('month',date,0,'b');drop date;;format dateM yymmd7.;run;run;
/*ÿ����ʵ��ӯ����ʵ�ֿ���*/
proc means data=de nway noprint;class khh dateM PL code;var realize;output out=de1 sum=;where PL~=''; ;run;
proc means data=de1 nway noprint;class khh dateM PL ;var realize;output out=de1 sum=;
	label realize='�¶���ʵ��ӯ��' _freq_='�¶���ʵ��ӯ����Ʊ��';run;
/*ÿ��������ӯ�����������*/
proc sort data=de;by khh code dateM;run;
/*ȡ������Ʊ��ĩ״̬*/
data de2;retain dateM;set de;by khh code dateM;if last.dateM then output;
	drop n price vol time bs PL minV realize order return;run;
data de2;set de2;by khh code;output;if last.code and dateM~='01may2015'd and volum>0
	then do;dateM='01may2015'd ;;output;end;run;

proc timeseries data=de2 out=de3;
   by khh code;  id dateM  interval=month accumulate=last;
   var priceM cost value volum balance;run;
data t;set de3(keep=priceM cost value volum balance);flag+1;run;
/*proc timeseries data=t out=t1;*/
/*id flag interval=day setmissing=PREV;*/
/*var priceM cost value volum balance;run;*/
/*data t1;set t(obs=100);if priceM~=. then do;array n(6) _NUMERIC_;;end;*/
/*	else do;do i=1 to 2;*/
/*	priceM=n(1);cost=n(2);end;end;;run;*/

data t1;set t;retain a b c d e;
	if priceM~=. then do;a=priceM; b=cost; c=value ;d=volum ;e=balance;end;
	rename a=priceM b=cost c=value d=volum e=balance;
	drop priceM cost value volum balance;run;

data de3;merge de3 t1; drop flag;;run;

/*�����Ʊ�¶ȼ۸�*/
proc sql;create table de4 as select * from de3 left join price1
on de3.code=price1.code and de3.dateM=price1.month;quit;
proc sort data=de4;by khh month dateM;run;
 
data de5;set de4;drop dateM;where flag~=.;if price>=priceM then float='P';else float='L';
	floatV=volum*abs(price-priceM);run;
proc means data=de5 noprint nway;class khh month float;var floatV;
	output out=de6 sum=;run; 
data de6;set de6;rename _freq_=floatN;label _freq_='�¶ȸ���ӯ����Ʊ��' float='�¶ȸ���ӯ��';
	drop _type_;run;
/*��ʵ��ӯ��������ӯ���ϲ�*/
data de7;merge de1(in=a rename=(dateM=month)) de6(rename=(float=PL));by khh month PL;run;
data de_;set de7;totalV=sum(floatV,realize);totalN=sum(_freq_,floatN);
	if PL='P' then do;pgrV=realize/totalV;pgrN=_freq_/totalN;end;
	if PL='L' then do;plrV=realize/totalV;plrN=_freq_/totalN;end;
	where realize>0;
	run;
data de_1;merge de_(where=(PL='P') drop=plrV plrN) de_(where=(PL='L') drop=pgrV pgrN);
	by khh month;run;
data de_1;
        set de_1;
        array xx _numeric_;
        do over xx;
                if xx=. then xx=0;
        end;
run;
data de_2;set de_1;deV=pgrV-plrV;deN=pgrN-plrN;run;



/********************************��Ʊ�͹�ƱЧӦ******************************/
/*����3ָ��ѡ����Ʊ�͹�Ʊ*/
data w;set njzq.final;drop i;run;
proc rank data=w(drop=mean) groups=2 out=w1;ranks price skwness _rmse_;var price skwness _rmse_;
	by month;run;
proc sort data=w1;by n month;run;
data float;set njzq.data;floatV=float*price;month=intnx('month',month,0,'b');;keep month n floatV;run;
proc sort data=float;by n month;run;
data w2;merge w1(in=a) float;by n month;if a;run;
/*��ǳ�����lottery*/
data w2;set w2;if price=0 and skwness=1 and _rmse_=1 then lottery=1;else lottery=0;run;
/*��������²�Ʊ�͹�Ʊ��ռ�ȣ�����ͨ��ֵΪȨ��*/
proc means data=w2 noprint nway;class month lottery;var floatV;output out=w3 sum=;run;
proc sql;
	create table w3 as select *,sum(floatV) as sum,floatV/calculated sum as proportion label="Ԥ��Ȩ��", 
	sum(_freq_) as sum_,_freq_/calculated sum_ as proportionN label="Ԥ��Ȩ��(��Ʊ��)"
	from w3 group by month;
	quit;
/*ʵ��ռ��*/
data w_;set kh_;keep khh code month cje;cje=price*vol;
	month=intnx('month',date,0,'b');where bs='b';format month yymmd7.;run;
/*��ǹ����Ʊ�Ƿ��Ʊ��*/
data w_1;merge w2(keep=month n lottery in=a) index(rename=(i=n));by n;if a;drop n flag;run;
proc sql;create table w_2 as select * from w_ left join w_1 on w_.code=w_1.code and w_.month=w_1.month;quit;
proc means data=w_2 noprint nway;class khh month lottery;var cje;
	output out=w_3 sum=;run;
proc sql;
	create table w_3 as select *,sum(cje) as sum,cje/calculated sum as proportionR label="ʵ��Ȩ��" ,
	sum(_freq_) as sum_,_freq_/calculated sum_ as proportionRN label="ʵ��Ȩ��(������)"
	from w_3 group by khh,month;
	quit;
proc sql;create table w_4 as select * from w_3 left join w3 
	on w_3.lottery=w3.lottery and w_3.month=w3.month;quit;
proc sort data=w_4(drop=_type_ _freq_  sum sum_ floatV);by khh month lottery;;run;
/*����ָ�꣺ewls*/
data lotteryW;set w_4;ewlsV=proportionR/proportion-1;ewlsN=proportionRN/proportionN-1;run;

/*********************************��������ЧӦ**********************************/
/*������Ʊ����ĩ�۸�ǰ��ĩ�۸�*/
data kh_m;set kh_1;keep n khh code vol date bs;;run;
data kh_m1;set kh_m;
dateM=intnx('month',date,0,'b');format dateM yymmd7.;run;
proc means data=kh_m1 nway noprint;class bs khh dateM code;var vol;output out=kh_m2 sum=;run;


proc sql;create table kh_m3 as select * from kh_m2 left join price1
on kh_m2.code=price1.code and kh_m2.dateM=price1.month;quit;
proc sort data=kh_m3;by bs khh dateM;run;
/*�����µס�ǰ�µ׼۸��µĽ��׶�*/
data kh_m3;set kh_m3;where flag~=.;drop dateM _type_ price pricePre flag;
monthV=vol*price;monthPreV=vol*pricePre;run;
proc means data=kh_m3 noprint nway;class bs khh month;var vol monthV monthPreV _freq_;
	output out=kh_m4(drop=_type_) sum=vol monthV monthPreV tradeN;
	label vol='�¶Ƚ�����' monthV='�¶Ƚ��׶�µ׼Ƽۣ�' monthPreV='�¶Ƚ��׶�³��Ƽۣ�'
	tradeN='�¶Ƚ��״���' ;run;
/*�������� and �ֲ���ֵ �ϲ�*/
/*����ĩû�гֲ��� ������*/
proc sort data=kh_;by khh code date;run;
proc timeseries data=kh_ out=kh3;
   by khh code;  id date  interval=month accumulate=last;
   var volum;run;
data kh4;retain vol;set kh3;if volum~=. then vol=volum;drop volum;run;
proc sort data=kh4;by khh date;run;

proc sql;create table kh5 as select * from kh4 left join price1
on kh4.code=price1.code and kh4.date=price1.month;quit;

proc sort data=kh5(drop= n date);by khh month;run;
/*ÿ��ĩ�ֲ���ֵ���ֲֹ�Ʊ��*/
data kh5;set kh5;value=vol*price;run;
proc means data=kh5 nway noprint;class khh month;var value;output out=kh6(drop=_type_) sum= ;run;
proc datasets library=work noprint;modify kh6;rename _freq_=positionN value=positionV;
label positionN='�µ׳ֲֹ�Ʊ��' positionV='�µ׳ֲ���ֵ';run;quit;

data kh7;set kh6;by khh;output;if last.khh and month~='01may2015'd 
	then do;month='01may2015'd ;positionN=0;positionV=0;output;end;run;
proc timeseries data=kh7 out=kh7;
by khh;id month interval=month setmissing=0;
var positionN positionV;run;
data kh7;set kh7;by khh;positionPreV=lag(positionV);if first.khh then call missing(positionPreV);run;

proc sql;create table kh_m5 as select * from kh_m4 left join kh7 
	on kh_m4.khh=kh7.khh and kh_m4.month=kh7.month;quit;
data kh_m5;set kh_m5;
	if bs='b' then do;if positionV=0 then turnoverB=1;else turnoverB=monthV/positionV;end;
	if bs='s' then do;if positionPreV=0 then turnoverS=1;else turnoverS=monthPreV/positionPreV;end;
    if turnoverB>1 then turnoverB=1;if turnoverS>1 then turnoverS=1;
run;
data kh_m6;merge kh_m5(where=(bs='b') drop=turnoverS) kh_m5(where=(bs='s') drop=turnoverB);
by khh month;run;
data turnover;set kh_m6;turnover=sum(turnoverB,turnoverS)/2;run;

/*********************************positionͳ��*********************************/
/*position������0����ĳ��Ʊ����յ�0*/
data tradeoff;set kh_;by n;retain tradeoffN;if first.n or lag(volum)=0 then tradeoffN+1;run;

data tradeoff1;set tradeoff(drop=value volum cost balance return);retain priceM cost;
by tradeoffN;if first.tradeoffN then do;value=0;volum=0;priceM=price;cost=0;end;
if bs='b' then do;
	value+(-vol*price);cost+vol*price;
	volum+vol;priceM=cost/volum;bs='b';end;
else do;
	volum+(-vol);value+(vol*price);cost=volum*priceM;bs='s';end;
balance=volum*price;
if last.tradeoffN then return=value+balance;
label value='����Ʒ���ʽ�����' volum='����Ʒ�ֳ������䶯' balance='����Ʒ����ֵ�䶯'
      return='����Ʒ�ָ���ӯ��';
run;

/*ͳ��ÿ��tradeoff�µ�ʱ�������������桢ƽ���ֲֳɱ�*/
proc means data=tradeoff1 noprint nway;id code khh;class tradeoffN;var cost;
	output out=t_cost mean=costM;run;
proc means data=tradeoff1 noprint nway;id code khh;class tradeoffN;var date;
	output out=t_time max=max min=min;run;
data t_return;set tradeoff1;by tradeoffN;if last.tradeoffN then output;run;
data tradeoff2;merge t_return t_cost t_time;by tradeoffN;run;
/*��������2014.6ǰtradeoff�����״��*/
data tradeoff3;set tradeoff2;where date<'01jul2014'd;tradeoffR=return/costM;length=max-min;run;
proc means data=tradeoff3 nway noprint;class khh;var return _freq_ costM tradeoffR length;
	output out=tradeoff4 mean=return positionN costM tradeoffR length;run;
data tradeoff4;set tradeoff4;rename _freq_=positionN1;label return='ƽ������' _freq_='tradeoff������Ŀ' 
	costM='ƽ���ֲֳɱ�' tradeoffR='ƽ��������' length='ƽ������ʱ��' positionN='ƽ�����״���';run;
/*��ѡ�����桢�����ʡ��������׬��*/
proc sort data=tradeoff3;by khh return;run;
data tReturnMin tReturnMax;set tradeoff3(keep=khh return);by khh;if first.khh then output tReturnMin;
	if last.khh then output tReturnMax;run;
proc sort data=tradeoff3;by khh tradeoffR;run;
data tRateMin tRateMax;set tradeoff3(keep=khh tradeoffR);by khh;if first.khh then output tRateMin;
	if last.khh then output tRateMax;run;
data tradeoff_M;merge  tReturnMin(rename=(return=ReturnMin)) tReturnMax(rename=(return=ReturnMax)) 
	tRateMin(rename=(tradeoffR=RateMin)) tRateMax(rename=(tradeoffR=RateMax));
	label ReturnMin='position�������' ReturnMax='position�������' 
	RateMin='position���������' RateMax='position���������';run;
/*�ϲ��������׵�ƽ��״������ߵ�״��*/
data tradeoff;merge tradeoff4 tradeoff_M;run;

/***********************�¶�������************************/
/*��de6�����ϼ��㣺��ĩ�ֲָ�ӯ���*/
data de6;set de6;if float='L' then floatV=-floatV;run;
proc means data=de6 nway noprint;class khh month;var floatV;output out=ret sum=;run;
data ret;set ret;diffloatV=floatV-lag(floatV);run;
/*��de1�����ϼ��㣺�¶���ʵ��ӯ�����*/
data de1;set de1;if PL='L' then realize=-realize;run;
proc means data=de1 nway noprint;class khh dateM;var realize;output out=ret1 sum=;run;
data ret2;merge ret ret1(rename=(dateM=month));by khh month;run;
data ret2;set ret2;return=sum(diffloatV,realize);label floatV='��ĩ����ӯ��' diffloatV='�¶ȸ���ӯ���䶯'
	realize='�¶���ʵ��ӯ��' return='�¶�����';drop _type_ _freq_;run;
/*��kh7�����Ϻϲ����¶����桢�³��ֲ���ֵ*/
data ret3;merge ret2 kh7;by khh month;run;
/*�¶�������=�¶�����/�³��ֲ���ֵ���³��ղ�ʱ��������������*/
data ret3;set ret3;if positionPreV>0 then rate=return/positionPreV;
	else if positionV>0 then rate=return/(positionV-return);run;
proc means data=ret3 nway noprint;class khh;var floatV diffloatV realize return positionN positionV rate;
	output out=ret4 mean= floatV diffloatV realize return positionN positionV rate;label  floatV='ƽ����ĩ����ӯ��' diffloatV='ƽ����ĩ����ӯ���䶯' 
	realize='ƽ���¶���ʵ��ӯ��' return='ƽ���¶�����' positionN='ƽ����ĩ�ֲֹ�Ʊ��' positionV='ƽ����ĩ�ֲ���ֵ' 
	rate='ƽ���¶�������' _freq_='���ڽ��׵�����';;run;

/*�ܵĺϲ���1����״����ret4��2��������״����tradeoff��3�������ţ�turnover��*/
/*	4��Ʊ��ƫ�ã�lotteryW��5����ЧӦ��de_2*/
data t1;set ret4;rename return=returnM _freq_=monthN;drop _type_ ;run;
data t2;set tradeoff;rename return=returnTradeoff positionN=positionNTradeoff;drop _type_;run;
data t3;set turnover;keep khh turnoverB turnoverS turnover;
	if turnoverB=. then turnoverB=0;if turnoverS=. then turnoverS=0;run;
proc means data=t3 nway noprint;class khh;var turnoverB turnoverS turnover;output out=t3_(drop=_type_ _freq_) mean=turnoverB turnoverS turnover;
	label turnoverB='ƽ���¶����뻻����' turnoverS='ƽ���¶�����������' turnover='ƽ���¶Ȼ�����';run;

proc means data=lotteryw nway noprint;class khh;var month;output out=t4 n=;run;
proc means data=lotteryw nway noprint;where lottery=1;class khh;var proportionR proportionRN proportion proportionN ewlsV ewlsN;
	output out=t4_(drop=_type_ _freq_) sum=;run;
data t4_1;merge t4(in=a) t4_;by khh;if a;run;
data t4_1;set t4_1;proportionR=proportionR/month;proportionRN=proportionRN/month;proportion=proportion/month;
	proportionN=proportionN/month;ewlsV=ewlsV/month;ewlsN=ewlsN/month;
	label ewlsV='��Ʊ��ƫ�ã����׽�' ewlsN='��Ʊ��ƫ�ã����״�����';drop _type_ _freq_ month;run;

data t5;set de_2;keep khh pgrV pgrN plrV plrN deV deN;run;
proc means data=t5 nway noprint;class khh;var pgrV pgrN plrV plrN deV deN;output out=t5_(drop=_type_ _freq_) 
	mean=pgrV pgrN plrV plrN deV deN;label pgrV='����ӯ����Ʊ��������' pgrN='����ӯ����Ʊ������������' 
	plrV='���������Ʊ��������' plrN='���������Ʊ������������' deV='����ЧӦ����' deN='����ЧӦ��������';run;

data total;merge t1 t2 t3_ t4_1 t5_;by khh;run;
data total;merge total(in=a) khxx;by khh;if a;run;


data save.total&i;set total;run;
data save.shouyi&i;set ret4;run;
data save.tradeoff&i;set tradeoff;run;
data save.turnover&i;set turnover;run;
data save.lotteryW&i;set lotteryW;run;
data save.de&i;set de_2;run;


/* proc datasets noprint;*/
/*copy in=work out=save;select ret4 tradeoff turnover lotteryW de_2;run;*/

proc datasets lib=work  nolist;
   save price1 khxx index /memtype=data;
quit;

%end;
%mend;
%prg4;



data total;merge total(in=a) khxx;by khh;if a;run;

PROC EXPORT DATA= WORK.Total 
            OUTFILE= "E:\njzq\total.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;

proc datasets lib=work  nolist;
   delete ���ݼ����� /memtype=data;
quit;

data khxx;set njzq.khxx;
khrq=input(put(strip(khrq),$8.),yymmdd8.);
xhrq=input(put(strip(xhrq),$8.),yymmdd8.);
birth=input(csrq,yymmdd8.);
format khrq yymmdd10. xhrq yymmdd10. birth yymmdd10.;
drop csrq WTFSFW;label birth='��������';
run;
data khxx;set khxx;age_kh=year(khrq)-year(birth);age_17=2017-year(birth);
	label age_kh='��������' age_17='���䣨��2017�꣩' 
	;run;
proc sort data=khxx;by khh;run;


/**********************************�Ʊ���****************************/
data bg1;informat code $6.;set njzq.bg;if find(zy,"˰��") then delete;
	code=scan(zy,2,':,');if missing(input(code,best32.)) then delete;run;
data t1 t2;set bg1(keep=code khh);if length(code)=5 then do;flag='�۹�';output t1;end;
	if length(code)=6 then do;flag='B��';output t2;end;run;
proc means data=t1 noprint nway;class khh;output out=t1_;run;
proc means data=t2 noprint nway;class khh;output out=t2_;run;

proc means data=njzq.hg(keep=khh) noprint nway;class khh;output out=t3;run;
proc means data=njzq.rz(keep=khh) noprint nway;class khh;output out=t4;run;
proc means data=njzq.rq(keep=khh) noprint nway;class khh;output out=t5;run;

data all;merge t1_(drop=_type_ rename=(_freq_=flag1)) t2_(drop=_type_ rename=(_freq_=flag2))
	t3(drop=_type_ rename=(_freq_=flag3)) t4(drop=_type_ rename=(_freq_=flag4))
	t5(drop=_type_ rename=(_freq_=flag5));by khh;label flag1='�۹ɽ��״���' flag2='B�ɽ��״���' 
	flag3='�ع����״���' flag4='���ʽ��״���' flag5='��ȯ���״���' ;run;


%macro prg5;
%do x=3 %to 11;
data kh_;set save.kh_&x;keep khh code date;run;
proc sql;create table kh_w as select * from kh_ inner join warrant1 
	on kh_.code=warrant1.code and kh_.date between warrant1.start and warrant1.last;quit;
proc sql;create table kh_e as select * from kh_ inner join etf1 
	on kh_.code=etf1.code and kh_.date> etf1.start ;quit;
data kh_c;set kh_;where code like '3%';run;

/*Ȩ֤��ETF����ҵ��*/
proc means data=kh_w(keep=khh) noprint nway;class khh;output out=t6;run;
proc means data=kh_e(keep=khh) noprint nway;class khh;output out=t7;run;
proc means data=kh_c(keep=khh) noprint nway;class khh;output out=t8;run;

proc append base=t6_ data=t6;run;
proc append base=t7_ data=t7;run;
proc append base=t68_ data=t8;run;
%end;
%mend;
%prg5;





data all1;merge all t6(drop=_type_ rename=(_freq_=flag6)) t7(drop=_type_ rename=(_freq_=flag7))
	t8(drop=_type_ rename=(_freq_=flag8));by khh;label flag6='Ȩ֤���״���'
	flag7='ETF���״���' flag8='��ҵ�彻�״���';run;

proc sql;create table all2 as select * from khh2 left join all1 on
	khh2.khh=all1.khh;quit;


data all2;set all2;  
        array xx _numeric_;
        do over xx;
                if xx=. then xx=0;
        end;
run;
data all2;
	retain khh flag1_
			flag2_
			flag3_
			flag4_
			flag5_
			flag6_
			flag7_
			flag8_;
	set all2;
	if flag1>0 then flag1_=1;else flag1_=0;label flag1_='�Ƿ�۹ɽ���';
	if flag2>0 then flag2_=1;else flag2_=0;label flag2_='�Ƿ�B�ɽ���';
	if flag3>0 then flag3_=1;else flag3_=0;label flag3_='�Ƿ�ع�����';
	if flag4>0 then flag4_=1;else flag4_=0;label flag4_='�Ƿ����ʽ���';
	if flag5>0 then flag5_=1;else flag5_=0;label flag5_='�Ƿ���ȯ����';
	if flag6>0 then flag6_=1;else flag6_=0;label flag6_='�Ƿ�Ȩ֤����';
	if flag7>0 then flag7_=1;else flag7_=0;label flag7_='�Ƿ�ETF����';
	if flag8>0 then flag8_=1;else flag8_=0;label flag8_='�Ƿ�ҵ�彻��';
	rename _freq_=yearN;
	label _freq_='���ڽ��������' count='���״���';
	run;
/*���浽save��51wͶ���ߵ��Ʊ���*/
proc datasets lib=work  nolist;
   save all2 price1 khxx index /memtype=data;
quit;


%macro prg6;
%do y=1 %to 11;
proc append base=total data=save.total&y;run;
%end;
%mend;
%prg6;

data total1;merge total(in=a) all2;by khh;run;

PROC EXPORT DATA= WORK.total1 
            OUTFILE= "E:\njzq\����Ͷ����.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;


/*����ּ��ͻ�����*/
PROC IMPORT OUT= WORK.total_fj
            DATAFILE= "e:\njzq\total.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="total$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data total_fj;set total_fj;drop _col34;rename _col44=count;run;
data t;set total(obs=1);run;

data t1;set t total_fj;run;

proc contents data=total noprint out=o1(keep=name label);run;
proc contents data=total_fj noprint out=o2(keep=name label);run;
proc sort data=o1;by label;run;proc sort data=o2;by label;run;
data o;merge o1 o2(rename=(name=name1));by label;run;
proc sql noprint;
select distinct strip(name1)||"="||strip(name)
   into : label separated by " "
           from o;
quit;
data total_fj;set total_fj;rename &label;run;

data total_fj;retain khh_ yyb_;informat khh_ $12. yyb_ $4.;set total_fj;
	khh=1000000000000+khh;khh_=substr(put(khh,$13.),2,12);
	yyb=10000+yyb;yyb_=substr(put(yyb,$5.),2,4);
	rename khh_=khh yyb_=yyb;label khh_='�ͻ���' yyb_='Ӫҵ��';	;run;
/*�ּ��ͻ��Ʊ���:�Ʊ��� ����*/


data total_fj;set total_fj;judge=1;run;
data zhibiao;set total1 total_fj;;run;
data zhibiao;set zhibiao;if judge=. then judge=0;run;

proc contents data=zhibiao noprint out=o1(keep=name label);run;

proc means data=zhibiao nway noprint;class judge;var xb;output out=z sum=;run;


PROC EXPORT DATA= WORK.simple 
            OUTFILE= "E:\njzq\z.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;
