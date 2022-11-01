DROP procedure if exists etl_ia.load_inc(fpInputTableNm varchar(32)
								 ,fpTargetTableNm varchar(50)
								 ,fpPartialUpdateFlg varchar(10))
								 ;

CREATE procedure etl_ia.load_inc(fpInputTableNm varchar(32)
								 ,fpTargetTableNm varchar(50)
								,fpPartialUpdateFlg varchar(10))
--	RETURNS integer
    LANGUAGE 'plpgsql'
--     COST 100
--     VOLATILE NOT LEAKPROOF
AS $BODY$
declare
	lmvCountRows int default 0;
	lmvCountRowsSrc int default 0;
	lmvCountRowsTrg int default 0;
	lmvCountRowsUpd int default 0;
	lmvCountRowsAct int default 0;
	lmvTableNm varchar(50) := split_part($1,'.', 2) ;
	lmvResId varchar(64);
	lmvResTypeLoad varchar(80);
	lmvResFieldTimeFrame varchar(80);
	lmvResTimeFrameVal varchar(80);
	lmvPkList varchar(800);
	lmvPkListFULL varchar(800);
	lmvColumnsList varchar(1500);
	lmvFullColumnsList varchar(1500);
	lmvPkListForCloseDelta  varchar(800);
	lmvCurrentTimestamp timestamp without time zone DEFAULT current_timestamp;
	lmvFutureTimestamp timestamp without time zone DEFAULT '5999-01-01 00:00:00';
BEGIN
	/* Проверка на валидность входных параметров (Сущ-е ресурса) */
-- 	EXECUTE FORMAT('SELECT resource_id
-- 					FROM etl_cfg.cfg_resource
-- 					WHERE lower(resource_nm) = %L',lmvTableNm)
-- 			INTO STRICT lmvResId;
-- 				BEGIN
-- 					EXCEPTION
-- 					WHEN NO_DATA_FOUND THEN
-- 					RAISE EXCEPTION 'Resource %s not found', lmvTableNm;
-- 					WHEN TOO_MANY_ROWS THEN
-- 					RAISE EXCEPTION 'More than one resource existing with name %s', lmvTableNm;
-- 				END;

	/* вычисляем констрейнты для таблицы */
	drop table if exists res_constraints;
	EXECUTE format ('create temp table if not exists res_constraints as
						SELECT c.column_name
								, c.data_type
								, ccu.constraint_name
								, tc.constraint_type
						FROM inFORMATion_schema.columns as c
						left JOIN inFORMATion_schema.constraint_column_usage as ccu
							ON c.table_name = ccu.table_name
							AND c.column_name = ccu.column_name
						left JOIN inFORMATion_schema.table_constraints as tc
							ON c.table_name = tc.table_name
							AND ccu.constraint_name = tc.constraint_name
						WHERE upper(c.table_schema) =%L
				and upper(c.table_name) = %L'
				,split_part(upper(fpInputTableNm), '.', 1)
				,split_part(upper(fpInputTableNm), '.', 2));

	/* Записываем констрейнты в отдельные переменные */
	SELECT array_agg(DISTINCT column_name)
	INTO lmvPkList
	FROM res_constraints
	WHERE constraint_type = 'PRIMARY KEY'
		AND lower(column_name) NOT IN ('valid_from_dttm', 'valid_to_dttm', 'processed_dttm', 'source_system_cd');

	SELECT array_agg(DISTINCT column_name)
	INTO lmvPkListFULL
	FROM res_constraints
	WHERE constraint_type = 'PRIMARY KEY';

	SELECT  array_agg(DISTINCT column_name)
	INTO lmvColumnsList
	FROM res_constraints
	WHERE (constraint_type <> 'PRIMARY KEY' OR constraint_type IS NULL)
		AND column_name NOT IN ('valid_from_dttm', 'valid_to_dttm', 'processed_dttm', 'source_system_cd');

	SELECT  array_agg(DISTINCT column_name)
	INTO lmvFullColumnsList
	FROM res_constraints
	WHERE lower(column_name) NOT IN ('processed_dttm');

	SELECT array_agg(DISTINCT column_name)
	INTO lmvPkListForCloseDelta
	FROM res_constraints
	WHERE constraint_type = 'PRIMARY KEY'
		or lower(column_name) IN ('valid_from_dttm', 'valid_to_dttm',  'source_system_cd');

	raise notice 'lmvPkList = %', lmvPkList;
	raise notice 'lmvPkListFULL = %', lmvPkListFULL;
	raise notice 'lmvColumnsList = %', lmvColumnsList;
	raise notice 'lmvFullColumnsList = %', lmvFullColumnsList;
	raise notice 'lmvPkListForCloseDelta = %', lmvPkListForCloseDelta;

	/* Создаем таблицу со свежей выгрузкой и расчитанным хешем*/
	EXECUTE FORMAT('drop table if exists %s'
			, split_part(upper(fpInputTableNm), '.', 2) || '_incr_hsh');
	EXECUTE FORMAT('create temporary table if not exists %s as
					SELECT *
					,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as pk_hash
				    ,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as dim_hash
					FROM %s'
			, split_part(upper(fpInputTableNm), '.', 2) || '_incr_hsh'
			, replace(replace(lmvPkList, '{', ''),'}','')
			, replace(replace(lmvColumnsList, '{', ''),'}','')
			, fpInputTableNm);

	EXECUTE FORMAT('SELECT count(*) as cnt
					FROM %s'
			, split_part(upper(fpInputTableNm), '.', 2) || '_incr_hsh')
			INTO lmvCountRowsSrc;
		IF lmvCountRowsSrc = 0 THEN
			RAISE NOTICE 'Входная таблица пустая.';
		END IF;
	RAISE NOTICE 'Входная таблица CNT = %', lmvCountRowsSrc;
	/* Забираем актуальный срез из главной таблицы и расчитываем хеши (по фактам и по ключам)*/
	/* Обработка переменных для расчета хеша */
	EXECUTE FORMAT('drop table if exists %s'
			, split_part(upper(fpTargetTableNm), '.', 2) ||'_snap_hsh');
		EXECUTE FORMAT('create temp table if not exists %s as
							SELECT decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as pk_hash
								,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as dim_hash
					   			, %s
					   			,valid_from_dttm as valid_from_dttm_snap
								,valid_to_dttm as valid_to_dttm_snap
								,processed_dttm as processed_dttm_snap
					   			, %s
							FROM %s
							WHERE valid_to_dttm>current_timestamp'
				, split_part(upper(fpTargetTableNm), '.', 2) || '_snap_hsh'
				, replace(replace(lmvPkList, '{', ''),'}','')
				, replace(replace(lmvPkList, '{', ''),'}','')
				, replace(replace(lmvColumnsList, '{', ''),'}','')
				, replace(replace(lmvPkList, '{', ''),'}','')
				, fpTargetTableNm);

	/* Вычисляем различия в таблицах через FULL JOIN по хеш значению PK */
	DROP TABLE IF EXISTS calc_diffs_fj;
	EXECUTE FORMAT('CREATE TEMP TABLE IF NOT EXISTS calc_diffs_fj AS
					SELECT CASE
							WHEN incr.pk_hash = act.pk_hash AND incr.dim_hash <> act.dim_hash then ''N''
							WHEN incr.pk_hash is null then ''C''
							WHEN act.pk_hash is null then ''1''
						END as etl_delta_cd
						 ,COALESCE(incr.pk_hash, act.pk_hash) as pk_hash
						,COALESCE(incr.dim_hash, act.dim_hash) as dim_hash
					FROM %s incr
					FULL JOIN %s act
						ON incr.pk_hash = act.pk_hash
					WHERE CASE
							WHEN incr.pk_hash = act.pk_hash AND incr.dim_hash <> act.dim_hash then ''N''
							WHEN incr.pk_hash is null then ''C''
							WHEN act.pk_hash is null then ''1''
						END in (''1'',''N'',''C'')'
			,split_part(upper(fpInputTableNm), '.', 2)  || '_incr_hsh'
			,split_part(upper(fpTargetTableNm), '.', 2) ||'_snap_hsh');

		DROP TABLE IF EXISTS etl_ia.privet;
		create table etl_ia.privet as select * from calc_diffs_fj;

		SELECT count(*) as cnt INTO lmvCountRows
					FROM calc_diffs_fj;
		RAISE NOTICE 'Кол-во строк с расхождениями для таблицы = %',lmvCountRows;
		SELECT count(*) as cnt INTO lmvCountRowsUpd
					FROM calc_diffs_fj
					WHERE etl_delta_cd = 'N';
		RAISE NOTICE 'Кол-во новых строк для таблицы = %',lmvCountRowsUpd;
		SELECT count(*) as cnt INTO lmvCountRowsTrg
					FROM calc_diffs_fj
					WHERE etl_delta_cd = '1';
		RAISE NOTICE 'Кол-во новых строк для таблицы = %',lmvCountRowsTrg;

		IF lmvCountRows > 0 THEN
			EXECUTE FORMAT ('DROP TABLE IF EXISTS %s'
							,'etl_dds.' || split_part(upper(fpInputTableNm), '.', 2) || '_delta');
			-- строки на инсерт
			 -- все отсутствующие в инкременте записи, но присутствующие в снапе -
							-- к закрытию но только для кусочного
						   --TODO объявить все атрибуты как НАЛЛ
						   -- для всех изменений - закрываем предыдущую версию (джоин на снап таблицу)
						   --TODO объявить все атрибуты как НАЛЛ
						   -- переписать на инсерт а до этого транкейт
			raise notice 'создаем структуры дельта-таблицы';
			--создаем структуры дельта-таблицы
			EXECUTE FORMAT('create table  %s as
						   	select %s
						   		,''N'' as ETL_DELTA_CD
						   		, cast(current_timestamp as timestamp without time zone) as processed_dttm
						    from %s
						   	where 1=0'
						   , 'etl_dds.' || split_part(upper(fpInputTableNm), '.', 2) || '_delta'
						   , replace(replace(lmvFullColumnsList, '{', ''),'}','')
						   , fpTargetTableNm
						   );
			raise notice 'строки на инсерт';
			-- строки на инсерт
			EXECUTE FORMAT('insert into %s (%s
						  					,etl_delta_cd
						  					, processed_dttm)
						   select %s
						   		, ''N'' as etl_delta_cd
						   		, %L as processed_dttm
						   from %s incr
								INNER JOIN calc_diffs_fj diffs
									ON incr.pk_hash = diffs.pk_hash
									and etl_delta_cd in (''1'',''N'') '
						   , 'etl_dds.' || split_part(upper(fpInputTableNm), '.', 2) || '_delta'
						   , replace(replace(lmvFullColumnsList, '{', ''),'}','')
						   , replace(replace(lmvFullColumnsList, '{', ''),'}','')
						   , lmvCurrentTimestamp
						   , split_part(upper(fpInputTableNm), '.', 2)  || '_incr_hsh'
						   );
			raise notice 'все отсутствующие в инкременте записи, но присутствующие в снапе';
			-- все отсутствующие в инкременте записи, но присутствующие в снапе -
			-- к закрытию но только НЕ для кусочного
			--TODO объявить все атрибуты как НАЛЛ
			EXECUTE FORMAT('insert into %s (%s
						  					,etl_delta_cd
						  					, processed_dttm)
						   select %s
						   		, etl_delta_cd
						   		, %L as processed_dttm
						   FROM %s snap
								INNER JOIN calc_diffs_fj diffs
									ON snap.pk_hash = diffs.pk_hash
									and etl_delta_cd in (''C'')
						  		where %L = ''FULL'' '
						   , 'etl_dds.' || split_part(upper(fpInputTableNm), '.', 2) || '_delta'
						   , replace(replace(lmvPkListForCloseDelta, '{', ''),'}','')
						   , replace(replace(replace(replace(lmvPkListForCloseDelta, '{', ''),'}','')
									 , 'valid_to_dttm', 'valid_to_dttm_snap as valid_to_dttm')
									 , 'valid_from_dttm', 'valid_from_dttm_snap as valid_from_dttm')
						   , lmvCurrentTimestamp
						   , split_part(upper(fpTargetTableNm), '.', 2)  || '_snap_hsh'
						   , upper(fpPartialUpdateFlg)
						   );
			raise notice 'для всех изменений - закрываем предыдущую версию';
			-- для всех изменений - закрываем предыдущую версию (джоин на снап таблицу)
		    --TODO объявить все атрибуты как НАЛЛ
			EXECUTE FORMAT('insert into %s (%s
						  					, etl_delta_cd
						  					, processed_dttm)
						   SELECT %s
						   			, ''C'' as etl_delta_cd
						   			, %L as processed_dttm
									FROM %s snap
								INNER JOIN calc_diffs_fj diffs
									ON snap.pk_hash = diffs.pk_hash
									and etl_delta_cd in (''N'') '
						   , 'etl_dds.' || split_part(upper(fpInputTableNm), '.', 2) || '_delta'
						   , replace(replace(lmvPkListForCloseDelta, '{', ''),'}','')
						   , replace(replace(replace(replace(lmvPkListForCloseDelta, '{', ''),'}','')
									 , 'valid_to_dttm', 'valid_to_dttm_snap as valid_to_dttm')
									 , 'valid_from_dttm', 'valid_from_dttm_snap as valid_from_dttm')
						   , lmvCurrentTimestamp
						   , split_part(upper(fpTargetTableNm), '.', 2)  || '_snap_hsh'
						   );
		ELSE
			RAISE NOTICE 'No diffs were founded. Next steps are not required.';
		END IF;

END;
$BODY$;

CALL etl_ia.load_inc('etl_ia.test_rk_table', 'dwh_dds.test_target_table', 'full11');


select * from etl_ia.privet;

select *
from etl_ia.test_rk_table

select *
from dwh_dds.test_target_table

-- INSERT INTO dwh_dds.test_target_table(
-- 	var_rk, attr_1, attr_var_1, attr_var_2, attr_var_3, attr_2, attr_3, attr_4, valid_from_dttm, valid_to_dttm, processed_dttm)
-- 	VALUES (1222, 555, 'test1212', 'test121212', '1121', current_timestamp
-- 			, 1211212, current_timestamp, current_timestamp, current_timestamp, current_timestamp);

select *
from etl_dds.test_rk_table_delta





