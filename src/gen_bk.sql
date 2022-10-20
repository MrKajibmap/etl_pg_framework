drop function if exists  f_get_bk_expression;
create function f_get_bk_expression(fpBkCd character varying(50)
									, fpInputTableNm character varying(50)
								   )
returns character varying(50) as $$
<<block>>
declare
	lmvVarType character varying(30);
	lmvVarLength integer;
	lvBkFormula character varying(1000) = '';
	input_cursor refcursor;
	input_row record;
	bk_dummy_postfix character varying(1000) = '';
	lvBkFieldNm character varying(50) = '';

	bk_cursor cursor for
		select bk_frml_full.part_full as part_formula
			,coalesce(dynamic_components.part_value, quote_literal(bk_frml_full.part_full)) as part_value
			,row_number() over () as rowid
		from
		(
			--выборка всех фрагментов формулы в табличном виде
			select regexp_split_to_table(bk_format_txt, '_') as part_full
			from etl_sys.etl_bk
			where bk_cd = fpBkCd
		) bk_frml_full
	left join
		(
		--собираем вместе фрагменты БК для подстановки значений колонок

		select bk_parts.part, bk_values.part_value
		from (
			--выборка фрагментов формулы БК для подстановки ({})
			select parts.part
				, row_number() over() as part_index
			from (
					select regexp_split_to_table(bk_format_txt, '_') as part, bk_format_txt
					from etl_sys.etl_bk
					where bk_cd = fpBkCd

				  ) parts
			where parts.part like '{%}'
			) bk_parts
		left join
			(
			--выборка колонок для подставления в формулу БК
			select innert2.part_value
				, row_number() over() as part_index
			from (
					select regexp_split_to_table(bk_column_list_txt, '\s+') as part_value
					from etl_sys.etl_bk
					where bk_cd = fpBkCd
					) innert2
			) bk_values
			on bk_parts.part_index = bk_values.part_index
		) dynamic_components
		on dynamic_components.part = bk_frml_full.part_full
	;
	bk_row record;
begin
	open bk_cursor;
	LOOP
		fetch from bk_cursor into bk_row;
		--заменить на вызов ошибки
		EXIT WHEN NOT FOUND;

		RAISE NOTICE 'bk_row.Part_formula >> %', bk_row.Part_formula;
		RAISE NOTICE 'bk_row.Part_value >> %', bk_row.Part_value;

		--проверка на наличие в формуле столбцов, а не констант
		IF strpos(bk_row.Part_formula, '{') = 1 AND strpos(bk_row.Part_formula, '{') != 0
		THEN
			--забираем метаданные (длину и тип) для указанного столбца
			select data_type as var_type
					,coalesce(character_maximum_length, 0) as var_length
					into lmvVarType
						,lmvVarLength
			from information_schema.columns
			where
				upper(table_schema) = split_part(upper(fpInputTableNm), '.', 1)
				and upper(table_name) = split_part(upper(fpInputTableNm), '.', 2)
				and upper(column_name) = upper(bk_row.Part_value);

			--проверка на наличие столбца во входном наборе данных
			IF NOT FOUND THEN
				RAISE EXCEPTION 'Переменная % не найдена в наборе данных %', bk_row.Part_value, fpInputTableNm;
			END IF;
				--Для числовых полей (или формата 000123456) -TODO доделать проверку данного типа
				IF (case
					when substr(bk_row.Part_formula, 2, 1) = 'C' then 'CHAR'
					when substr(bk_row.Part_formula, 2, 1) = 'N' then 'NUM'
					when substr(bk_row.Part_formula, 2, 1) = 'Z' then 'CHAR_DIGIT'
				end) != 'CHAR'
					--собираем часть выражения для numeric колонки, проверя, что значение не NULL
					--(в ином случае помечаем как дамми )
					THEN
						lvBkFormula = lvBkFormula || ' || ''_'' || ' ||  ' case when input_t.'
						|| bk_row.Part_value || ' is null then repeat(''F'',' ||
						substr(bk_row.Part_formula, 3, length(bk_row.Part_formula)-3)::int ||
						') else input_t.' || bk_row.Part_value || '::text end ';
						bk_dummy_postfix = bk_dummy_postfix ||  ' or input_t.' || bk_row.Part_value || ' is null';
						raise notice ' bk_dummy_postfix = %', bk_dummy_postfix;
						raise notice ' lvBkFormula = %', lvBkFormula;
				--ТОЛЬКО ДЛЯ Символьных полей: проверка на соответствие длин
				--фактической переменной и указанной в формуле
				ELSEIF (case
					when substr(bk_row.Part_formula, 2, 1) = 'C' then 'CHAR'
					when substr(bk_row.Part_formula, 2, 1) = 'N' then 'NUM'
					when substr(bk_row.Part_formula, 2, 1) = 'Z' then 'CHAR_DIGIT'
				end) = 'CHAR'
					AND cast(substr(bk_row.Part_formula, 3, length(bk_row.Part_formula)-3) as int) != lmvVarLength
					THEN
						RAISE EXCEPTION 'Некорректная длина переменной (%) указана в формуле = % . Фактическая = % .
						Будет сгенерирована DUMMY-запись.', bk_row.Part_value, bk_row.Part_formula, lmvVarLength;
				ELSEIF (case
					when substr(bk_row.Part_formula, 2, 1) = 'C' then 'CHAR'
					when substr(bk_row.Part_formula, 2, 1) = 'N' then 'NUM'
					when substr(bk_row.Part_formula, 2, 1) = 'Z' then 'CHAR_DIGIT'
				end) = 'CHAR'
					THEN
					lvBkFormula = lvBkFormula || ' || ''_'' || case when '
					|| 'input_t.' || bk_row.Part_value || ' is null then repeat(''F'',' ||
						substr(bk_row.Part_formula, 3, length(bk_row.Part_formula)-3)::int ||
						') else input_t.' || bk_row.Part_value || ' end ';
						raise notice ' lvBkFormula = %', lvBkFormula;
					bk_dummy_postfix = bk_dummy_postfix || ' or input_t.' || bk_row.Part_value || ' is null';
						raise notice ' bk_dummy_postfix = %', bk_dummy_postfix;

				END IF;
			RAISE NOTICE 'lmvVarType = %', lmvVarType;
			RAISE NOTICE 'lmvVarLength = %', lmvVarLength;
			RAISE NOTICE 'Full ROW = %', bk_row;
		--для констант
		ELSE
			lvBkFormula = lvBkFormula || ' || _ || ' || bk_row.Part_value;
		END IF;

		RAISE NOTICE 'Part_formula = %', bk_row.Part_formula;
		RAISE NOTICE 'Part_value = %', bk_row.Part_value;
		RAISE NOTICE 'Final formula = %', substr(lvBkFormula, 10) || '||' || '_' || ' case when '
		|| substr(bk_dummy_postfix, 5) || ' then nextval(''etl_sys.tech_dummy_seq'')::text else '''' end' ;

		RAISE NOTICE 'bk_dummy_postfix = %', bk_dummy_postfix;
	END LOOP;
	close bk_cursor;

	select bk_field_nm into lvBkFieldNm
	from etl_sys.etl_bk
	where bk_cd = fpBkCd;
	IF NOT FOUND THEN
		raise exception 'Для указанного BK (%) отсутствует значение столбца (bk_field_nm) в таблице ETL_SYS.ETL_BK.', fpBkCd;
	END IF;
	IF length(lvBkFormula) = 0 THEN
		RAISE EXCEPTION 'Указанный BK (%) не найден в таблице ETL_SYS.ETL_BK.', fpBkCd;
	END IF;

	return '( ' || substr(lvBkFormula, 10) || '||'  || ' case when '
		|| substr(bk_dummy_postfix, 5) || ' then ''_'' || nextval(''etl_sys.tech_dummy_seq'')::text else '''' end) as ' || lvBkFieldNm;


end;

$$ language plpgsql;




drop function if exists f_bk;
create function f_bk() returns void as $$

begin

execute FORMAT('drop table if exists %s'
					, 'FCC.TEST_TABLE_BK1');

	execute format('create table %s as
				   		select *
				   			, %s
				   		from %s as input_t'
				  		, 'FCC.TEST_TABLE_BK1'
						,  f_get_bk_expression('FIN_INSTR_ASSOC_KPS_FSP_OPT', 'FCC.FCC_TEST_TABLE')
				  		, 'FCC.FCC_TEST_TABLE');
end;

$$ language plpgsql;


select f_bk();

select f_get_bk_expression('FIN_INSTR_ASSOC_KPS_FSP_OPT'
						   , 'FCC.FCC_TEST_TABLE'
						  ) ;




