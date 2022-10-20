-- 0. проверка на длину полученного ключа и на наличие поля БК в таблице входной
-- 1. отсев по дистинкту ключей из нового набора
-- 2. отсев по дистинкту ключей из таблицы бк_рк
-- 3. поиск максимального значеня рк в таблице бк_рк (этот шаг не требуется, если использовать некствал, если его можно использовать в селекте)
-- 4. (2)лефт джоин (1) с расчетом флага соединения, где флаг =1 (новый ключ). для них генерируем новый рк.
-- 4.1. если параметр = dict, то пропуск шага. если =fact, то полученные пары в наборе данных (4 ) являются дамми записями.
-- 4.2. добавление в дамми-таблицу (указанную в параметре) полученных записей.
-- 4.3. структура?? -наименования колонок должны быть приведены к ддс_объектам на шаге бк. они соответствуют значениям таблицы дамми. в дамми таблице находятся нотналл поля и пк + технические (тип дамми и валидту фром процессед).
-- 4.4. забираем структуру из дамми таблицы
-- 4.5. при добавлении (4) в таблицу дамми, объявляем нотналл поля как дамми-констан(DUM  , -1) и инсертим
-- 5. добавление новых ключей в таблицу связей (инсерт)
-- 6. джоин входного набора на обновленную таблицу связей с забором рк

-- замечания: наименования бк могут различаться во входном наборе и таблице связей

drop function if exists  f_generate_rk;
create function f_generate_rk(fpInputTableName character varying (50)
							  ,fpOutputTableName character varying (50)
							  , fpBkRkTableName character varying (50)
							  , fpDummyTableName character varying (50)
							  , fpGenerateMode char(4)
							  , fpBkName character varying (32)
							  , fpRkName character varying (32))
							  returns void as $$
<<block>>
declare
	lvMaxRkValue int;
	lvNewBkRkCnt int;
	lmvVarType character varying(30);
	lmvVarLength integer;
	lvBkFormula character varying(100) = '';
	lvBkFieldNm character varying(50) = '';
	lvDummyColumnList character varying(1000) = '';
	lvDummyColumnCnt int;

	lvInputColumnList character varying(1000) = '';
	lvInputColumnCnt int;

begin

drop table if exists input_bk_list;

-- 1. отсев по дистинкту ключей из нового набора
execute format ('create temp table input_bk_list as
					select distinct %s
					from %s', fpBkName , fpInputTableName);

drop table if exists link_bk_list;
-- 2. отсев по дистинкту ключей из таблицы бк_рк
execute format ('create temp table link_bk_list as
					select distinct %s
					from %s', fpBkName , fpBkRkTableName);


-- 3. поиск максимального значеня рк в таблице бк_рк (этот шаг не требуется, если использовать некствал, если его можно использовать в селекте)
-- execute format ('select greatest(max(%s), 0) as max_rk
-- 					into lvMaxRkValue
-- 					from %s', fpRkName, fpBkRkTableName);

select greatest(max(fin_instrument_assoc_rk), 0) as max_rk
	into lvMaxRkValue
from etl_ia.test_table_bk_rk;

raise notice 'MaxRK = %',lvMaxRkValue;

-- 4. (2)лефт джоин (1) с расчетом флага соединения, где флаг =1 (новый ключ). для них генерируем новый рк.
drop table if exists new_bk_list_to_generate;
execute format ('create temp table new_bk_list_to_generate as
					select distinct bk.%s
							, %s + row_number() over () as %s
					from input_bk_list bk
					left join link_bk_list bk_rk
					on bk.%s = bk_rk.%s where coalesce(bk_rk.%s, ''1'') = ''1''
					order by %s'
				,fpBkName,  lvMaxRkValue, fpRkName ,fpBkName, fpBkName, fpBkName, fpBkName);

select count(*)
into lvNewBkRkCnt
from new_bk_list_to_generate;

raise notice ' Количество сгенерированных пар BK-RK = %', lvNewBkRkCnt;

IF lvNewBkRkCnt > 0 THEN
	raise notice ' Добавление сгенерированных пар BK-RK в таблицу %', fpBkRkTableName;

	execute format ('insert into %s (%s, %s)
					select %s
							, %s
					from new_bk_list_to_generate '
				,fpBkRkTableName,  fpBkName, fpRkName, fpBkName, fpRkName);

	-- 4.1. если параметр = dict, то пропуск шага. если =fact, то полученные пары в наборе данных (4 ) являются дамми записями.
	-- 4.2. добавление в дамми-таблицу (указанную в параметре) полученных записей.   fpDummyTableName
	IF upper(fpGenerateMode) = 'FACT' THEN
		--забираем структуру дамми
		select string_agg(quote_literal(upper(column_name)), ', ') as clmn_list, count(*) as cnt
						into lvDummyColumnList, lvDummyColumnCnt
					from information_schema.columns
					where upper(table_schema) = split_part(upper(fpDummyTableName), '.', 1)
					and upper(table_name) = split_part(upper(fpDummyTableName), '.', 2);

		raise notice ' lvDummyColumnList = %', lvDummyColumnList;
		raise notice ' lvDummyColumnCnt = %', lvDummyColumnCnt;

		--забираем структуру входного объекта
		--проверка наличия данной структуры с входным набором данных
		select (string_agg(column_name, ', ') || ', ' || fpRkName) as clmn_list , (count(*) + 1) as cnt
						into lvInputColumnList, lvInputColumnCnt
					from information_schema.columns
					where upper(table_schema) = split_part(upper(fpInputTableName), '.', 1)
					and upper(table_name) = split_part(upper(fpInputTableName), '.', 2)
					and upper(column_name) in (lvDummyColumnList);


		--todo надо сверить 2 списка, найти общие колонки, а для оставшихся проставить либо константу
		--либо наллы, результат записать во временную таблицу и пускай разраб сам возится (либо сразу в дамми таблицу)
		--но тогда нужно будет проверять список колонок на наличие в них валид ту валид фром процессед

		raise notice ' lvInputColumnList = %', lvInputColumnList;
		raise notice ' lvInputColumnCnt = %', lvInputColumnCnt;
		raise notice ' Добавление сгенерированных Dummy в таблицу %', fpDummyTableName;
		execute format ('insert into %s (%s, dummy_type_cd, valid_from_dttm, valid_to_dttm, processed_dttm)
						select %s
								, ''D3'' as dummy_type_cd
								, current_timestamp as valid_from_dttm
								, current_timestamp as valid_to_dttm
								, current_timestamp as processed_dttm
						from new_bk_list_to_generate '
					, fpDummyTableName
					, fpRkName
					, fpRkName);

	END IF;
END IF;


raise notice 'Сборка финальной таблицы %', fpOutputTableName;
execute format ('drop table if exists %s', fpOutputTableName);
execute format ('create table %s as
					select bk.*
							, bk_rk.%s
					from %s bk
					inner join %s bk_rk
					on bk.%s = bk_rk.%s'
				,fpOutputTableName,  fpRkName,  fpInputTableName, fpBkRkTableName, fpBkName, fpBkName);



end;

$$ language plpgsql;

select f_generate_rk( 'fcc.test_table_bk1'
							  , 'fcc.test_table_RK'
							  , 'etl_ia.test_table_bk_rk'
					 			,'etl_ia.fcc_test_dummy'
							  , 'FACT'
							  , 'FIN_INSTRUMENT_ASSOC_ID'
							  , 'FIN_INSTRUMENT_ASSOC_RK');


-- select * from etl_ia.test_table_bk_rk;

--create table etl_ia.test_table_bk_rk (FIN_INSTRUMENT_ASSOC_ID character varying (54), FIN_INSTRUMENT_ASSOC_RK int );

-- truncate table etl_ia.test_table_bk_rk;

select * from etl_ia.fcc_test_dummy;


