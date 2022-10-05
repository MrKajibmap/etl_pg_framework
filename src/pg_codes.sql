drop function if exists  test;
create function test(integ int) returns int as $$
declare
	const integer = 30;
	privet character varying(30);
	lvTest character varying(50);
begin
	select bk_format_txt into lvTest
	from etl_sys.etl_bk
	where bk_cd = 'FIN_INSTR_ASSOC_KPS_FSP_OPT';


	RAISE NOTICE 'Кол-во  = %',lvTest;
	return integ * 2;

end;

$$ language plpgsql;

select test(2) ;



select *
	from etl_sys.etl_bk
	where bk_cd = 'FIN_INSTR_ASSOC_KPS_FSP_OPT';


	select * from etl_sys.etl_bk_type

	select * from fcc.fcc_test_table;

	select bk_parts.*, bk_values.part_value
	from (
		select parts.part
			, row_number() over() as part_index
		from (
				select regexp_split_to_table(bk_format_txt, '_') as part, bk_format_txt
				from etl_sys.etl_bk
			  ) parts
		where parts.part like '{%}'
		) bk_parts
	left join
		(
			select innert2.part_value
				, row_number() over() as part_index
			from (
					select regexp_split_to_table(bk_column_list_txt, '\s+') as part_value
					from etl_sys.etl_bk
					where bk_cd = 'FIN_INSTR_ASSOC_KPS_FSP_OPT'
				) innert2
		) bk_values
		on bk_parts.part_index = bk_values.part_index




