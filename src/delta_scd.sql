DROP procedure if exists etl_ia.load_inc(tabname varchar(32));

CREATE procedure etl_ia.load_inc(fpInputTableNm varchar(32)
								 ,fpTargetTableNm varchar(50)
								 , tabname varchar(32))
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
	EXECUTE 'create temp table if not exists res_constraints as
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
						WHERE upper(table_schema) = split_part(upper(fpInputTableNm), '.', 1)
				and upper(table_name) = split_part(upper(fpInputTableNm), '.', 2)';

	/* Записываем констрейнты в отдельные переменные */
	SELECT array_agg(DISTINCT column_name)
	INTO lmvPkList
	FROM res_constraints
	WHERE constraint_type = 'PRIMARY KEY'
		AND lower(column_name) NOT IN ('valid_from_dttm', 'valid_to_dttm', 'processed_dttm');

	SELECT array_agg(DISTINCT column_name)
	INTO lmvPkListFULL
	FROM res_constraints
	WHERE constraint_type = 'PRIMARY KEY';

	SELECT  array_agg(DISTINCT column_name)
	INTO lmvColumnsList
	FROM res_constraints
	WHERE (constraint_type <> 'PRIMARY KEY' OR constraint_type IS NULL)
		AND column_name NOT IN ('valid_from_dttm', 'valid_to_dttm', 'processed_dttm');

	SELECT  array_agg(DISTINCT column_name)
	INTO lmvFullColumnsList
	FROM res_constraints;


	/* Создаем таблицу со свежей выгрузкой и расчитанным хешем*/
	EXECUTE FORMAT('drop table if exists %s'
			, lmvTableNm || '_incr_hsh');
	EXECUTE FORMAT('create temporary table if not exists %s as
					SELECT *
					,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as pk_hash
				    ,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as dim_hash
					,%L::timestamp without time zone as valid_from_dttm
					,%L::timestamp without time zone as valid_to_dttm
				    ,%L::timestamp without time zone as processed_dttm
					FROM %s'
			, split_part(upper(fpInputTableNm), '.', 1) || '_incr_hsh'
			, replace(replace(lmvPkList, '{', ''),'}','')
			, replace(replace(lmvColumnsList, '{', ''),'}','')
			, lmvCurrentTimestamp
			, lmvFutureTimestamp
			, lmvCurrentTimestamp
			, lvInputTableNm);

	EXECUTE FORMAT('SELECT count(*) as cnt
					FROM %s'
			, lmvTableNm || '_incr_hsh')
			INTO lmvCountRowsSrc;
		IF lmvCountRowsSrc = 0 THEN
			RAISE NOTICE 'Входная таблица пустая.';
		END IF;

	/* Забираем актуальный срез из главной таблицы и расчитываем хеши (по фактам и по ключам)*/
	/* Обработка переменных для расчета хеша */
	EXECUTE FORMAT('drop table if exists %s'
			, lmvTableNm||'_snap_hsh');
		EXECUTE FORMAT('create temp table if not exists %s as
							SELECT decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as pk_hash
								,decode(md5(concat_ws($t1$_$t1$, %s)), $t2$hex$t2$) as dim_hash
					   			,valid_from_dttm as valid_from_dttm_snap
								,valid_to_dttm as valid_to_dttm_snap
								,processed_dttm as processed_dttm_snap
							FROM %s
							WHERE valid_to_dttm>=current_timestamp'
				, split_part(upper(fpInputTableNm), '.', 1) || '_snap_hsh'
				, replace(replace(lmvPkList, '{', ''),'}','')
				, replace(replace(lmvColumnsList, '{', ''),'}','')
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
			,fpInputTableNm || '_incr_hsh'
			,fpInputTableNm||'_snap_hsh');

		SELECT count(*) as cnt INTO lmvCountRows
					FROM calc_diffs_fj;
		RAISE NOTICE 'Кол-во строк с расхождениями для таблицы = %',lmvCountRows;
		SELECT count(*) as cnt INTO lmvCountRowsUpd
					FROM calc_diffs_fj
					WHERE diff_flg = 2;
		RAISE NOTICE 'Кол-во строк с обновлениями данных для таблицы = %',lmvCountRowsUpd;
		SELECT count(*) as cnt INTO lmvCountRowsTrg
					FROM calc_diffs_fj
					WHERE diff_flg = 1;
		RAISE NOTICE 'Кол-во новых строк для таблицы = %',lmvCountRowsTrg;

		IF lmvCountRows > 0 THEN
			DROP TABLE IF EXISTS delta;
			EXECUTE FORMAT('CREATE TEMP TABLE IF NOT EXISTS delta AS
						   -- строки на инсерт
								(SELECT %s
						   				,ETL_DELTA_CD
								FROM %s incr
								INNER JOIN calc_diffs_fj diffs
									ON incr.pk_hash = diffs.pk_hash
									and diff_flg in (''1'',''N''))
								UNION
						   -- все отсутствующие в инкременте записи, но присутствующие в снапе - к закрытию
								(SELECT %s
						   				,ETL_DELTA_CD
								FROM %s snap
								INNER JOIN calc_diffs_fj diffs
									ON snap.pk_hash = diffs.pk_hash
									and diff_flg in (''C''))
						   -- для всех изменений - закрываем предыдущую версию (джоин на снап таблицу)
						  		(SELECT %s
						   				,''C'' as ETL_DELTA_CD
								FROM %s snap
								INNER JOIN calc_diffs_fj diffs
									ON snap.pk_hash = diffs.pk_hash
									and diff_flg in (''N''))'
						   ,REPLACE(REPLACE(REPLACE(lmvFullColumnsList, '{', ''),'}',''),'"','')
						   ,lmvTableNm || '_incr_hsh'
						   ,REPLACE(REPLACE(REPLACE(lmvFullColumnsList, '{', ''),'}',''),'"','')
						   ,lmvTableNm || '_snap_hsh');
		ELSE
			RAISE NOTICE 'No diffs were founded. Next download of data is not required.';
		END IF;

END;
$BODY$;

CALL etl_ia.load_inc('etl_ia.product_chain');