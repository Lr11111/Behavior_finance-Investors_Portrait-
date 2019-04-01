libname njzq 'e:\njzq';

data t;set njzq.kh_fj(keep= code );run;
/*权证*/
data z;set njzq.zj1501(keep=ywkm zy);where zy ?"ETF";run;
data z;set njzq.zj1501(keep=ywkm zy);where zy ?"权证";run;
data z;set njzq.zj1501(keep=ywkm zy);if find(zy,"回购");run;
data z1;set njzq.zj1501(keep=ywkm zy);where zy ?"1503";run;
data z;set njzq.zj1501(keep=ywkm zy);where zy ?"合约";run;

data z2;set stock1(keep=ywkm code zy);where code like '2%';run;

data z2;set stock1(keep=ywkm code zy);where code like '580%';run;
data z2;set stock1(keep=ywkm code zy);where code like '5%';run;
data z2;set stock1(keep=ywkm code zy);where code like '2%';run;
/*港股*/
data z2;set njzq.kh_fj(keep= code );if length(code)=5;run;
data z2;set njzq.kh_fj(keep= code );where code like '5%';run;


/*580997是权证代码*/
data z2;set t;where code like '580%';run;


/*找出资金表中的B股、港股,对摘要进行提取*/
data zj;set njzq.zj1501(keep=rq ywkm khh zy srje fcje bczjye);code=scan(zy,2,'()');
if missing(input(code,best32.)) then output; 
run;

data zj1;set zj;where ywkm in('13101','13001');#;run;


/*引入股票月末收盘价（不复权）*/
data price;informat code $6.;set njzq.final;keep n month price;run;
data index;informat code $6.;set njzq.index;flag=2;keep code i flag;format code $6.;run;
/*股票月度数据，加上前月度数据*/
proc sort data=price out=price;by n month;run;
data price1;set price;pricePre=lag(price);by n;if first.n then call missing(pricePre);run;
data price1;merge price1(in=a) index(rename=(i=n));by n;if a;run;

/*持仓：*/

proc sort data=kh_;by khh code date;run;
proc timeseries data=kh_ out=kh3;
   by khh code;  id date  interval=month accumulate=last;
   var volum;run;
data kh4;retain vol;set kh3;if volum~=. then vol=volum;drop volum;run;
proc sort data=kh4;by khh date;run;

proc sql;create table kh5 as select * from kh4 left join price1
on kh4.code=price1.code and kh4.date=price1.month;quit;

proc sort data=kh5(drop= n date);by khh month;run;
/*每月末持仓市值、持仓股票数*/
data kh5;set kh5;value=vol*price;run;
proc means data=kh5 nway noprint;class khh month;var value;output out=kh6(drop=_type_) sum= ;run;
proc datasets library=work noprint;modify kh6;rename _freq_=positionN value=positionV;
label positionN='月底持仓股票数' positionV='月底持仓市值';run;quit;

/**************************平均持仓成本**************************/
data kh;retain n;set njzq.kh_fj;
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
label value='单个品种资金流动' volum='单个品种持有量变动' balance='单个品种市值变动'
      return='单个品种浮动盈亏';
order+1;
drop bczjye khqz je;
run;
/*删除初始持仓的股票：无法确定其平均成本*/
proc sql;create table kh_ as
select *,min(volum) as minV
from kh_ group by n 
having minV>=0;quit;
proc sort data=kh_;by order;run;

/*卖出时状况：已实现盈利、已实现亏损*/
data kh_1;set kh_;if bs='s' then do;
	if price>priceM then PL='P';else PL='L';
	realize=vol*abs(price-priceM); end;run;

/***************************处置效应DE**************************/
data de;set kh_1;dateM=intnx('month',date,0,'b');drop date;;format dateM yymmd7.;run;run;
/*每个月实现盈利、实现亏损*/
proc means data=de nway noprint;class khh dateM PL code;var realize;output out=de1 sum=;where PL~=''; ;run;
proc means data=de1 nway noprint;class khh dateM PL ;var realize;output out=de1 sum=;
	label realize='月度已实现盈亏' _freq_='月度已实现盈亏股票数';run;
/*每个月账面盈利、账面亏损*/
proc sort data=de;by khh code dateM;run;
/*取各个股票月末状态*/
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

/*引入股票月度价格*/
proc sql;create table de4 as select * from de3 left join price1
on de3.code=price1.code and de3.dateM=price1.month;quit;
proc sort data=de4;by khh month dateM;run;
 
data de5;set de4;drop dateM;where flag~=.;if price>=priceM then float='P';else float='L';
	floatV=volum*abs(price-priceM);run;
proc means data=de5 noprint nway;class khh month float;var floatV;
	output out=de6 sum=;run; 
data de6;set de6;rename _freq_=floatN;label _freq_='月度浮动盈亏股票数' float='月度浮动盈亏';
	drop _type_;run;
/*已实现盈亏、浮动盈亏合并*/
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

/********************************彩票型股票效应******************************/
/*根据3指标选出彩票型股票*/
data w;set njzq.final;drop i;run;
proc rank data=w(drop=mean) groups=2 out=w1;ranks price skwness _rmse_;var price skwness _rmse_;
	by month;run;
proc sort data=w1;by n month;run;
data float;set njzq.data;floatV=float*price;month=intnx('month',month,0,'b');;keep month n floatV;run;
proc sort data=float;by n month;run;
data w2;merge w1(in=a) float;by n month;if a;run;
/*标记出来：lottery*/
data w2;set w2;if price=0 and skwness=1 and _rmse_=1 then lottery=1;else lottery=0;run;
/*计算各个月彩票型股票的占比：以流通市值为权重*/
proc means data=w2 noprint nway;class month lottery;var floatV;output out=w3 sum=;run;
proc sql;
	create table w3 as select *,sum(floatV) as sum,floatV/calculated sum as proportion label="预期权重", 
	sum(_freq_) as sum_,_freq_/calculated sum_ as proportionN label="预期权重(股票数)"
	from w3 group by month;
	quit;
/*实际占比*/
data w_;set kh_;keep khh code month cje;cje=price*vol;
	month=intnx('month',date,0,'b');where bs='b';format month yymmd7.;run;
/*标记购买股票是否彩票型*/
data w_1;merge w2(keep=month n lottery in=a) index(rename=(i=n));by n;if a;drop n flag;run;
proc sql;create table w_2 as select * from w_ left join w_1 on w_.code=w_1.code and w_.month=w_1.month;quit;
proc means data=w_2 noprint nway;class khh month lottery;var cje;
	output out=w_3 sum=;run;
proc sql;
	create table w_3 as select *,sum(cje) as sum,cje/calculated sum as proportionR label="实际权重" ,
	sum(_freq_) as sum_,_freq_/calculated sum_ as proportionRN label="实际权重(交易数)"
	from w_3 group by khh,month;
	quit;
proc sql;create table w_4 as select * from w_3 left join w3 
	on w_3.lottery=w3.lottery and w_3.month=w3.month;quit;
proc sort data=w_4(drop=_type_ _freq_  sum sum_ floatV);by khh month lottery;;run;
/*计算指标：ewls*/
data lotteryW;set w_4;ewlsV=proportionR/proportion-1;ewlsN=proportionRN/proportionN-1;run;

/*********************************过度自信效应**********************************/
/*买卖股票的月末价格、前月末价格*/
data kh_m;set kh_1;keep n khh code vol date bs;;run;
data kh_m1;set kh_m;
dateM=intnx('month',date,0,'b');format dateM yymmd7.;run;
proc means data=kh_m1 nway noprint;class bs khh dateM code;var vol;output out=kh_m2 sum=;run;


proc sql;create table kh_m3 as select * from kh_m2 left join price1
on kh_m2.code=price1.code and kh_m2.dateM=price1.month;quit;
proc sort data=kh_m3;by bs khh dateM;run;
/*计算月底、前月底价格下的交易额*/
data kh_m3;set kh_m3;where flag~=.;drop dateM _type_ price pricePre flag;
monthV=vol*price;monthPreV=vol*pricePre;run;
proc means data=kh_m3 noprint nway;class bs khh month;var vol monthV monthPreV _freq_;
	output out=kh_m4(drop=_type_) sum=vol monthV monthPreV tradeN;
	label vol='月度交易量' monthV='月度交易额（月底计价）' monthPreV='月度交易额（月初计价）'
	tradeN='月度交易次数' ;run;
/*买卖交易 and 持仓市值 合并*/
/*对月末没有持仓量 填充进来*/
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

/*********************************position统计*********************************/
/*position数：从0买入某股票到清空到0*/
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
label value='单个品种资金流动' volum='单个品种持有量变动' balance='单个品种市值变动'
      return='单个品种浮动盈亏';
run;

/*统计每个tradeoff下的时长、个数、收益、平均持仓成本*/
proc means data=tradeoff1 noprint nway;id code khh;class tradeoffN;var cost;
	output out=t_cost mean=costM;run;
proc means data=tradeoff1 noprint nway;id code khh;class tradeoffN;var date;
	output out=t_time max=max min=min;run;
data t_return;set tradeoff1;by tradeoffN;if last.tradeoffN then output;run;
data tradeoff2;merge t_return t_cost t_time;by tradeoffN;run;
/*交易者在2014.6前tradeoff的相关状况*/
data tradeoff3;set tradeoff2;where date<'01jul2014'd;tradeoffR=return/costM;length=max-min;run;
proc means data=tradeoff3 nway noprint;class khh;var return _freq_ costM tradeoffR length;
	output out=tradeoff4 mean=return positionN costM tradeoffR length;run;
data tradeoff4;set tradeoff4;rename _freq_=positionN1;label return='平均收益' _freq_='tradeoff交易数目' 
	costM='平均持仓成本' tradeoffR='平均收益率' length='平均持有时长' positionN='平均交易次数';run;
/*挑选出收益、收益率、大亏、大赚的*/
proc sort data=tradeoff3;by khh return;run;
data tReturnMin tReturnMax;set tradeoff3(keep=khh return);by khh;if first.khh then output tReturnMin;
	if last.khh then output tReturnMax;run;
proc sort data=tradeoff3;by khh tradeoffR;run;
data tRateMin tRateMax;set tradeoff3(keep=khh tradeoffR);by khh;if first.khh then output tRateMin;
	if last.khh then output tRateMax;run;
data tradeoff_M;merge  tReturnMin(rename=(return=ReturnMin)) tReturnMax(rename=(return=ReturnMax)) 
	tRateMin(rename=(tradeoffR=RateMin)) tRateMax(rename=(tradeoffR=RateMax));
	label ReturnMin='position最低收益' ReturnMax='position最高收益' 
	RateMin='position最低收益率' RateMax='position最高收益率';run;
/*合并往返交易的平均状况、最高低状况*/
data tradeoff;merge tradeoff4 tradeoff_M;run;


/***********************月度收益率************************/
/*在de6基础上计算：月末持仓浮盈情况*/
data de6;set de6;if float='L' then floatV=-floatV;run;
proc means data=de6 nway noprint;class khh month;var floatV;output out=ret sum=;run;
data ret;set ret;diffloatV=floatV-lag(floatV);run;
/*在de1基础上计算：月度已实现盈亏情况*/
data de1;set de1;if PL='L' then realize=-realize;run;
proc means data=de1 nway noprint;class khh dateM;var realize;output out=ret1 sum=;run;
data ret2;merge ret ret1(rename=(dateM=month));by khh month;run;
data ret2;set ret2;return=sum(diffloatV,realize);label floatV='月末浮动盈亏' diffloatV='月度浮动盈亏变动'
	realize='月度已实现盈亏' return='月度收益';drop _type_ _freq_;run;
/*在kh7基础上合并：月度收益、月初持仓市值*/
data ret3;merge ret2 kh7;by khh month;run;
/*月度收益率=月度收益/月初持仓市值：月初空仓时，不考虑收益率*/
data ret3;set ret3;if positionPreV>0 then rate=return/positionPreV;
	else if positionV>0 then rate=return/(positionV-return);run;
proc means data=ret3 nway noprint;class khh;var floatV diffloatV realize return positionN positionV rate;
	output out=ret4 mean= floatV diffloatV realize return positionN positionV rate;label  floatV='平均月末浮动盈亏' diffloatV='平均月末浮动盈亏变动' 
	realize='平均月度已实现盈亏' return='平均月度收益' positionN='平均月末持仓股票数' positionV='平均月末持仓市值' 
	rate='平均月度收益率' _freq_='存在交易的月数';;run;
/**********************交易次数***********************/
proc means data=kh_ nway noprint;class khh;var n;output out=cishu min=min max=max;run;




/*总的合并：1收益状况：ret4、2往返交易状况：tradeoff、3过度自信：turnover、*/
/*	4彩票型偏好：lotteryW、5处置效应：de_2*/
data t1;set ret4;rename return=returnM _freq_=monthN;drop _type_ ;run;
data t2;set tradeoff;rename return=returnTradeoff positionN=positionNTradeoff;drop _type_;run;
data t3;set turnover;keep khh turnoverB turnoverS turnover;
	if turnoverB=. then turnoverB=0;if turnoverS=. then turnoverS=0;run;
proc means data=t3 nway noprint;class khh;var turnoverB turnoverS turnover;output out=t3_(drop=_type_ _freq_) mean=turnoverB turnoverS turnover;
	label turnoverB='平均月度买入换手率' turnoverS='平均月度卖出换手率' turnover='平均月度换手率';run;

proc means data=lotteryw nway noprint;class khh;var month;output out=t4 n=;run;
proc means data=lotteryw nway noprint;where lottery=1;class khh;var proportionR proportionRN proportion proportionN ewlsV ewlsN;
	output out=t4_(drop=_type_ _freq_) sum=;run;
data t4_1;merge t4(in=a) t4_;by khh;if a;run;
data t4_1;set t4_1;proportionR=proportionR/month;proportionRN=proportionRN/month;proportion=proportion/month;
	proportionN=proportionN/month;ewlsV=ewlsV/month;ewlsN=ewlsN/month;
	label ewlsV='彩票型偏好（交易金额）' ewlsN='彩票型偏好（交易次数）';drop _type_ _freq_ month;run;

data t5;set de_2;keep khh pgrV pgrN plrV plrN deV deN;run;
proc means data=t5 nway noprint;class khh;var pgrV pgrN plrV plrN deV deN;output out=t5_(drop=_type_ _freq_) 
	mean=pgrV pgrN plrV plrN deV deN;label pgrV='卖出盈利股票比例（金额）' pgrN='卖出盈利股票比例（个数）' 
	plrV='卖出亏损股票比例（金额）' plrN='卖出亏损股票比例（个数）' deV='处置效应（金额）' deN='处置效应（个数）';run;

data total;merge t1 t2 t3_ t4_1 t5_;by khh;run;


data WORK.KHXX;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'E:\njzq\分级基金客户信息.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
       informat VAR1 $12. ;
       informat date yymmdd10. ;
       informat VAR3 best32. ;
       informat VAR4 yymmdd10. ;
       informat VAR5 yymmdd10. ;
        informat VAR6 best32. ;
        informat VAR7 best32. ;
        informat VAR8 best32. ;
        informat VAR9 yymmdd10. ;
        informat VAR10 best32. ;
        informat VAR11 best32. ;
        informat VAR12 best32. ;
        format VAR1 $12.;
        format date yymmdd10. ;
        format VAR3 best12. ;
        format VAR4 yymmdd10. ;
        format VAR5 yymmdd10. ;
        format VAR6 best12. ;
        format VAR7 best12. ;
        format VAR8 best12. ;
        format VAR9 yymmdd10. ;
        format VAR10 best12. ;
        format VAR11 best12. ;
        format VAR12 best12. ;
     input
                 VAR1
                 date
                 VAR3
                 VAR4
                 VAR5
                 VAR6
                 VAR7
                 VAR8
                 VAR9
                 VAR10
                 VAR11
                 VAR12
     ;
     if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     run;

proc sort data=khxx;by var1;run;
data total;merge total(in=a) khxx(rename=(var1=khh));by khh;if a;run;
data total;merge total(in=a) cishu(keep=khh _freq_);by khh;if a;run;

PROC EXPORT DATA= WORK.Total 
            OUTFILE= "E:\njzq\total.csv" 
            DBMS=CSV LABEL REPLACE;
     PUTNAMES=YES;
RUN;
