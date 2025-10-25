create table if not exists fact_sim
(
	row_id bigserial primary key,
	simulation_id uuid,
	simulation_num int,
	ca float,
	cb float,
	cc float,
	cd float,
	temperature float,
	t_sensor float,
	rxn_time float
);

create table if not exists dim_rxn
(
	simulation_id uuid primary key,
	simulation_num serial,
	reaction_name varchar(128),
	activation_energy float,
	ca0 float,
	cb0 float,
	T0 float,
	date_run date,
	stop_reason varchar(128),
	stop_time_s float
);

create sequence simulation_num_incrementor as int start 1 owned by dim_rxn.simulation_num;

alter table dim_rxn
	alter column simulation_num set default nextval('simulation_num_incrementor');

create table if not exists etl_run_log
(
	etl_id uuid primary key default gen_random_uuid(),
	started_at timestamp not null default now(),
	finished_at timestamp,
	simulation_id uuid,
	etl_type varchar(16),
	records_inserted int,
	records_updated int,
	status text,
	error_message text,
	duration_seconds numeric
);