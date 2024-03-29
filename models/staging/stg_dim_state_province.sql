WITH stg_dim_state_province__source AS(
  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_dim_state_province__rename_column AS(
  SELECT
  state_province_id AS state_province_key
  , state_province_name AS state_province_name
  FROM stg_dim_state_province__source
)

, stg_dim_state_province__cast_type AS(
  SELECT
  CAST( state_province_key AS INT) AS state_province_key
  , CAST( state_province_name AS STRING) AS state_province_name
  FROM stg_dim_state_province__rename_column
)

, stg_dim_state_province__add_undefined_record AS(
  SELECT
  state_province_key
  , state_province_name
  FROM stg_dim_state_province__cast_type

  UNION ALL
  SELECT
    0 AS state_province_key
    , 'Undefined' AS state_province_name
  
  UNION ALL
  SELECT
    -1 AS state_province_key
    , 'Unvalid' AS state_province_name
)

SELECT
  state_province_key
  , state_province_name
  FROM stg_dim_state_province__add_undefined_record