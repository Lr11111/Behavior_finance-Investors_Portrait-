/*个人交易的盈亏，简单算下 注意分红 上下折 单独买b 单独买a 做ab套利的*/

data t;set zj1501;where khh='118000019522';run;


data t1;set wt1501;where khh='118000019522';run;

data t3;set fenji2;where khh='019000064460';keep bs stock price vol;run;
data t3;set t3;cje=price*vol;if bs='卖' then v+(-vol);
if bs='买' then v+(vol);run;

data a;set fenji2;keep _col1-vol;run; 

proc sort data=fenji nodupkey;by khh;run;


proc freq data=fenji2 noprint ;table khh/out=z;run;

data a1;set a3;where price=.;run;
data a2;set a1;zy1=Tranwrd(zy,"(手)","");/*用空格代替AB*/run;

data a3;set a2;t1=scan(zy1,2,')');t2=scan(t1,1,'*');t3=scan(t1,2,'*');
if t3~='' then do;price=input(strip(t3),8.);vol=input(strip(compress(t2,'股')),8.);end;
drop t1-t3;run;


data tt;set fenji;if  find(zy,'认购') and bs~='';run;
data tt1;set fenji;cha=date- _col15;if cha<0;run;
proc univariate data=z noprint;histogram count;quit;
proc freq data=tt1 noprint ;table cha/out=z;run;


data tt1;set fenji;if date-_col15<0 and bs~='';run;
data tt2;set tt1;where year(date)=year(_col15);run;

/*********对07-15年配对的fenji观测进行进一步整理*************/

data fenji;set njzq.fenji_2007-njzq.fenji_2015;run;
/*对 数据集中的申购、认购进行处理*/
data fenji;set fenji;if bs~='买' and bs~='卖' then bs='';run;
/*删除07年买了12年份发行的同种基金*/
data fenji;set fenji;if date-_col15<0 and bs~='' then delete;where stock~='恒立实业' ;run;
data fenji;set fenji;if date-_col15<-30 then delete;run;
/*将fenji保存到njzq*/

/*标记 认购 申购*/
data fenji1;set fenji;if bs='' and find(zy,'认购') then bs='认';
if bs='' and find(zy,'申购') then bs='申';run;

data a1;set fenji1;zy1=Tranwrd(zy,"(手)","");/*用空格代替AB*/run;

data a2;set a1;t1=scan(zy1,2,')');t2=scan(t1,1,'*');t3=scan(t1,2,'*');
if t3~='' then do;price=input(strip(t3),8.);vol=input(strip(compress(t2,'股')),8.);end;
drop t1-t3 zy1;run;

data a3;set a2;if price=. then do;vol=compress(scan(zy,2,')'),'.','kd'); price=1;end;run;

proc freq data=a3 order=FREQ  noprint ;table khh/out=z;run;


/*对各个品种的分级基金的盈亏分析*/
proc sort data=a3;by khh code _col15 date;run;
data a4;retain n;set a3;if lag(khh)~=khh or lag(code)~=code or lag(_col15)~=_col15 then n+1;run;
data a5;set a4;
by n;if first.n then do;value=0;volum=0;end;
if bs='买' then do;value+(-vol*price);volum+vol;end;
 if bs='卖' then do;volum+(-vol);value+(vol*price);end;balance=volum*price;
if last.n then return=value+balance;
label value='单个品种资金流动' volum='单个品种持有量变动' balance='单个品种市值变动'
      return='单个品种浮动盈亏';
run;

data a6;set a5;where return~=.;run;

proc means data=a6 noprint;var return;by khh;output out=z sum= ;;run;

/*区分ab类型*/
proc sort data=a6;by khh _col2;run;
proc means data=a6 noprint;var return;by khh _col2;output out=z1 sum= ;;run;



/*将客户信息数据进行合并*/
proc sql;create table total as select * from z left join khxx on
z.khh=khxx.khh ;quit;
/*引入客户交易次数*/
proc freq data=a3 order=FREQ  noprint ;table khh/out=freq;run;
proc sql;create table total1 as select * from freq left join total on
freq.khh=total.khh ;quit;

data total1;
set total1;
   label count='交易次数' PERCENT='交易次数占比' _freq_='交易品种数' return='07-15年总盈亏';
   drop _type_;
run;quit;
/*对数化处理*/
data t;set total1;if return>0 then log_return=log10(abs(return));
if return<0 then log_return=-log10(abs(return));
label log_return='总盈亏取对数';run;

proc univariate data=t noprint;histogram log_return/MIDPOINTS=-7.5 TO 7.5 BY 0.1;
where -7.5<log_return<7.5;quit;


data z1_1;set z1;where _col2='分级基金优先级';run;
data z1_2;set z1;where _col2='分级基金普通级';run;
data z1_1;set z1_1;if return>0 then log_return=log10(abs(return));
if return<0 then log_return=-log10(abs(return));
label log_return='优先级总盈亏取对数';run;
data z1_2;set z1_2;if return>0 then log_return=log10(abs(return));
if return<0 then log_return=-log10(abs(return));
label log_return='普通级总盈亏取对数';run;

proc univariate data=z1_1 noprint;histogram log_return/MIDPOINTS=-7.5 TO 7.5 BY 0.1;
where -7.5<log_return<7.5;quit;
proc univariate data=z1_2 noprint;histogram log_return/MIDPOINTS=-7.5 TO 7.5 BY 0.1;
where -7.5<log_return<7.5;quit;

data t;set a5;where khh='026000014947';run;




data t;set njzq.zj1501;where ywkm='14201';run;
