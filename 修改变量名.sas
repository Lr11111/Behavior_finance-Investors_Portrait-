data class;
   set sashelp.class;
run;

data tmp;
   if 0 then set sashelp.class(keep=age--weight);
run;

proc sql noprint;
     select distinct strip(name)||"=Q1_"||strip(name) 
           into : rename separated by " "
           from dictionary.columns
           where libname="WORK" and memname="TMP";
quit;
%put &rename;

proc datasets library=work;
modify class;
   rename &rename;
run;
quit;
/*��ȡ����״̬��tablesΪ���״̬*/
proc sql noprint;
create table zz as
     select *
           from dictionary.columns    /*tables*/
           where libname="WORK" and memname="LABEL1";
quit;

/********************  ����var��label�����ж�Ӧ���ݽ��б�ǩ���  **********************/
/*��ȡ������*/
proc contents data=b noprint out=z;run;quit;
/*�������еı����� �� ���еı�ǩ���ж�ӳ*/
proc sql;create table label1 as select z.name,label.f2 from z left join label on strip(z.name)=strip(label.f1);quit;
/*����2����������ֱ�洢����������ǩ��*/
/*proc sql noprint ;select name,f2 into:name separated by " ",:label separated by " "*/
/*from label1 ;quit;*/
/*���ݱ�������ǩ�ı�񣬹��� var1='����1' �ĸ�ʽ */
proc sql noprint;
select distinct strip(name)||"='"||strip(F2)||"'"
   into : rename separated by " "
           from label1;
quit;
/*label�����б�ǩ*/
data b1;set b(obs=1);label &rename;run;

data test01;
        format id v1 8. v2 $10. v3 date9.;
        id = 1;
        v1 = 2;
        v2 = 'a';
        v3 = "01Jan2012"d;
run;
proc contents data = test01 out = test02(keep = name) noprint; run;
proc sql noprint;
        select compress(name||'='||name||'_n') into:renames separated by ' '
                from test02
                where lowcase(name) ne 'id';
quit;
%put &renames;
data test03;
        set test01(rename=(&renames.));
run;
/***************************************************/

/*********************************************/

data test;
        format id v1 8. v2 $10. v3 date9.;
        id = 1;
        v1 = 2;
        v2 = 'a';
        v3 = "01Jan2012"d;
run;


/*%MACRO renamall(libref, dataset, prefix); */
  %LOCAL namelist i ; 
PROC CONTENTS DATA=&libref..&dataset OUT=dumm9999; 
RUN; 
PROC SQL STIMER NOPRINT; 
  SELECT name 
    INTO :namelist SEPARATED BY ' ' 
    FROM dumm9999 
  ; 
QUIT; 
PROC DATASETS NOLIST LIBRARY=&libref; 
  MODIFY &dataset; 
    RENAME 
  %LET i = 1; 
  %LET token = %SCAN(&namelist,&i); 
  %DO %WHILE(%LENGTH(&token)); 
      &token = &token&prefix. 
    %LET i = %EVAL(&i + 1); 
    %LET token = %SCAN(&namelist,&i); 
  %END; 
    ; 
  RUN; 
QUIT; 
%MEND; 

%renamall(work,test,_N) 



data xx; 
a=1; b=2; c=3; d=4; 
proc sql noprint; 
select compress(name||"=M_"||name)  into : rnmlist separated by ' ' 
from dictionary.columns 
where libname='WORK' and memname='XX' 
; 
data xx; 
  set xx(rename=(&rnmlist)); 
proc print; 
run;


quit;
