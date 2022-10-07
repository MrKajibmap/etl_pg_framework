drop function if exists  test;
create function test(fpBkCd character varying(50), fpInputTableNm character varying(32)) returns character varying(50) as $$
<<block>>
declare
	const integer = 30;
	lmvVarType character varying(30);
	lmvVarLength integer;
	lvTest character varying(50);
	lvBkFormula character varying(100) = '';
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
	select bk_format_txt into lvTest
	from etl_sys.etl_bk
	where bk_cd = fpBkCd;

	open bk_cursor;
	LOOP
		fetch from bk_cursor into bk_row;
		EXIT WHEN NOT FOUND;

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
				table_schema = 'fcc'
				and upper(table_name) = fpInputTableNm
				and column_name = upper(bk_row.Part_value);

			--проверка на наличие столбца во входно наборе данных
			IF NOT FOUND THEN
				RAISE EXCEPTION 'Переменная % не найдена в наборе данных %', bk_row.Part_value, fpInputTableNm;
			END IF;
				--Для числовых полей (или формата 000123456) -TODO доделать проверку данного типа
				IF (case
					when substr(bk_row.Part_formula, 2, 1) = 'C' then 'CHAR'
					when substr(bk_row.Part_formula, 2, 1) = 'N' then 'NUM'
					when substr(bk_row.Part_formula, 2, 1) = 'Z' then 'CHAR_DIGIT'
				end) != 'CHAR'
					THEN
						lvBkFormula = lvBkFormula || ' || ''_'' || ' || bk_row.Part_value;
				--ТОЛЬКО ДЛЯ Символьных полей: проверка на соответсвие длин фактической переменной и указанной в формуле
				ELSEIF (case
					when substr(bk_row.Part_formula, 2, 1) = 'C' then 'CHAR'
					when substr(bk_row.Part_formula, 2, 1) = 'N' then 'NUM'
					when substr(bk_row.Part_formula, 2, 1) = 'Z' then 'CHAR_DIGIT'
				end) = 'CHAR' AND cast(substr(bk_row.Part_formula, 3, length(bk_row.Part_formula)-3) as int) != lmvVarLength
					THEN
						RAISE NOTICE 'Некорректная длина переменной (%) указана в формуле = % . Фактическая = % . Будет сгенерирована DUMMY-запись.', bk_row.Part_value, bk_row.Part_formula, lmvVarLength;
						lvBkFormula = lvBkFormula || ' || ''_'' || ' || repeat('F', cast(substr(bk_row.Part_formula, 3, length(bk_row.Part_formula)-3) as int));
				ELSE
					lvBkFormula = lvBkFormula || ' || ''_'' || ' || bk_row.Part_value;
				END IF;
			RAISE NOTICE 'lmvVarType = %', lmvVarType;
			RAISE NOTICE 'lmvVarLength = %', lmvVarLength;
			RAISE NOTICE 'Full ROW = %', bk_row;
		ELSE
			lvBkFormula = lvBkFormula || ' || _ || ' || bk_row.Part_value;
		END IF;

		RAISE NOTICE 'Part_formula = %', bk_row.Part_formula;
		RAISE NOTICE 'Part_value = %', bk_row.Part_value;
		RAISE NOTICE 'Final formula = %', substr(lvBkFormula, 10);


	END LOOP;

	RAISE NOTICE 'Кол-во  = %',lvTest;
	return lvTest;
end;

$$ language plpgsql;

select test('FIN_INSTR_ASSOC_KPS_FSP_OPT', 'FCC_TEST_TABLE') ;

