1. Create table `raw_data__general`:
```sql
CREATE TABLE raw_data__general (
  id                         uuid        PRIMARY KEY   DEFAULT gen_random_uuid(),
  account_id                 uuid                                                    NOT NULL,
  target_timestamp           timestamp                                               NOT NULL
);
```

2. Create table `raw_data__acc_wir_chrg_dt_sum_usg__wireless_number`:
```sql
CREATE TABLE raw_data__acc_wir_chrg_dt_sum_usg__wireless_number (
  general_id                 uuid                                                   NOT NULL,
  rd_bill_cycle_date         text                                                   NOT NULL,
  rd_account_number          text                                                   NOT NULL,
  rd_wireless_number         text                                                   NOT NULL,
  rd_item_category           text                                                   NOT NULL,
  rd_date                    text                                                   NOT NULL,
  rd_item_type               text                                                   NOT NULL,
  rd_item_description        text                                                   NOT NULL,
  rd_share_description       text                                                   NOT NULL,
  rd_usage_period            text                                                   NOT NULL,
  rd_allowance               text                                                   NOT NULL,
  rd_used                    text                                                   NOT NULL,
  pd_used_in_bytes           numeric                 DEFAULT NULL                           ,
  pd_used                    int4                    DEFAULT NULL                           ,

  FOREIGN KEY (general_id) REFERENCES raw_data__general (id) ON DELETE CASCADE
);
```

3. Create table `wireless_number__attribute`:

```sql
CREATE TABLE wireless_number__attribute (
  wireless_number            text        PRIMARY KEY                                NOT NULL,
  created_at                 timestamp                                              NOT NULL,
  model                      text
);
```

4. Upload data for `raw_data__general` with `psql`:
```
\copy raw_data__general FROM '/path/to/raw_data__general.csv' WITH (FORMAT csv);
```

5. Upload data for `raw_data__acc_wir_chrg_dt_sum_usg__wireless_number` with `psql`:

```
\copy raw_data__acc_wir_chrg_dt_sum_usg__wireless_number FROM '/path/to/raw_data__acc_wir_chrg_dt_sum_usg__wireless_number.csv' WITH (FORMAT csv);
```

6. Upload data for `wireless_number__attribute` with `psql`:
```
\copy wireless_number__attribute FROM '/path/to/wireless_number__attribute.csv' WITH (FORMAT csv);
```
