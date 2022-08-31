create table if not exists first_table_arch
(
  id numeric,
  fld varchar(255),
  mark varchar(255),
  nil varchar(255),
  etl_extract_id numeric not null,
  etl_available_dttm timestamp not null,
  source_system_cd char(3)
);