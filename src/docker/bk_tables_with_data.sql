CREATE TABLE ETL_SYS.ETL_BK 
   (	BK_CD character varying(32) NOT NULL,
	BK_TYPE_CD character varying(32) NOT NULL,
	BK_FIELD_NM character varying(32) NOT NULL,
	BK_FORMAT_TXT character varying(100) NOT NULL,
	BK_COLUMN_LIST_TXT character varying(200) NOT NULL,
	 CONSTRAINT PK_ETL_BK PRIMARY KEY (BK_CD)
   );

CREATE UNIQUE INDEX ON ETL_SYS.ETL_BK (BK_CD) ;

COMMENT ON TABLE ETL_SYS.ETL_BK IS 'Бизнес-ключ';
COMMENT ON COLUMN ETL_SYS.ETL_BK.BK_CD IS 'Идентификатор бизнес-ключа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK.BK_TYPE_CD IS 'Код типа бизнес-ключа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK.BK_FIELD_NM IS 'Поле бизнес-ключа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK.BK_FORMAT_TXT IS 'Шаблон бизнес-ключа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK.BK_COLUMN_LIST_TXT IS 'Список полей, по которым строится бизнес-ключ.';

CREATE TABLE ETL_SYS.ETL_BK_TYPE
   (	BK_TYPE_CD character varying(32) NOT NULL,
	BK_LENGTH numeric DEFAULT 32 NOT NULL,
	BK_RX_TXT character varying(100) NOT NULL,
	DUMMY_RX_TXT character varying(100) NOT NULL,
	 CONSTRAINT PK_ETL_BK_TYPE PRIMARY KEY (BK_TYPE_CD)
   ) ;

CREATE UNIQUE INDEX ON ETL_SYS.ETL_BK_TYPE (BK_TYPE_CD) ;

COMMENT ON TABLE ETL_SYS.ETL_BK_TYPE IS 'Тип бизнес-ключа';
COMMENT ON COLUMN ETL_SYS.ETL_BK_TYPE.BK_TYPE_CD IS 'Код типа бизнес-ключа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK_TYPE.BK_LENGTH IS 'Длина бизнес-ключей этого типа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK_TYPE.BK_RX_TXT IS 'Маска бизнес-ключей этого типа.';
COMMENT ON COLUMN ETL_SYS.ETL_BK_TYPE.DUMMY_RX_TXT IS 'Маска дамми бизнес-ключей этого типа.';

INSERT INTO ETL_SYS.ETL_BK (BK_CD, BK_TYPE_CD, BK_FIELD_NM, BK_FORMAT_TXT, BK_COLUMN_LIST_TXT) VALUES('FIN_INSTR_ASSOC_KPS_FSP_OPT', 'FIN_INSTR_ASSOC_KPS_FSP_OPT', 'FIN_INSTRUMENT_ASSOC_ID', 'KPS_{C3}_{N26}', 'FINANCIAL_INSTR_ASSOC_TYPE_CD BLOCKNUMBER');
INSERT INTO ETL_SYS.ETL_BK_TYPE (BK_TYPE_CD, BK_LENGTH, BK_RX_TXT, DUMMY_RX_TXT) VALUES('FIN_INSTR_ASSOC_KPS_FSP_OPT', 54, '\w{3,3}_\w{3,3}_\w{26,26}', '(\w{3,3}_\w{3,3}_\w{26,26})_(\d+)');


create table etl_ia.test_table_bk_rk (FIN_INSTR_ASSOC_ID character varying (54), FIN_INSTR_ASSOC_RK int );


CREATE TABLE IF NOT EXISTS etl_ia.fcc_test_dummy
(
    fin_instrument_assoc_rk numeric NOT NULL,
    valid_from_dttm timestamp NOT NULL,
	valid_to_dttm timestamp NOT NULL,
	processed_dttm timestamp NOT NULL,
	dummy_type_cd character varying(3) NOT NULL
);