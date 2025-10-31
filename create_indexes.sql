create index if not exists idx_fact_sim_simulation_num
    on fact_sim (simulation_num);

create index if not exists idx_fact_sim_ca_cb_rxn_time_temperature
    on fact_sim (ca, cb, rxn_time, temperature);

create index if not exists idx_fact_sim_rxn_time_temperature
    on fact_sim (rxn_time, temperature);

create index if not exists idx_dim_rxn_simulation_num
    on dim_rxn (simulation_num);

create index if not exists idx_dim_rxn_ca0_cb0
    on dim_rxn (ca0, cb0);