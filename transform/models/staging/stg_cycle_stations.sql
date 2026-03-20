-- models/staging/stg_cycle_stations.sql
-- =======================================
-- Cleans and enriches the raw cycle_stations table.
-- Adds zone classification based on lat/lon bounding boxes.

{{
  config(
    materialized = 'view',
    tags         = ['staging', 'cycle_stations']
  )
}}

with source as (

    select * from {{ source('raw', 'cycle_stations') }}

),

cleaned as (

    select
        -- ── identifiers ──────────────────────────────────────────
        cast(id as int64)                 as station_id,
        name                              as station_name,
        terminal_name,

        -- ── location ──────────────────────────────────────────────
        cast(latitude  as float64)        as latitude,
        cast(longitude as float64)        as longitude,

        -- ── zone classification (lat/lon bounding boxes) ──────────
        -- Simplified zones covering Greater London cycling area
        case
            when cast(latitude as float64) between 51.505 and 51.525
             and cast(longitude as float64) between -0.105 and -0.070
            then 'City & Shoreditch'

            when cast(latitude as float64) between 51.493 and 51.512
             and cast(longitude as float64) between -0.150 and -0.115
            then 'Westminster & Victoria'

            when cast(latitude as float64) between 51.495 and 51.512
             and cast(longitude as float64) between -0.115 and -0.090
            then 'Waterloo & Southbank'

            when cast(latitude as float64) between 51.525 and 51.560
             and cast(longitude as float64) between -0.150 and -0.085
            then 'Camden & Islington'

            when cast(latitude as float64) between 51.495 and 51.530
             and cast(longitude as float64) between -0.060 and  0.030
            then 'East End & Canary Wharf'

            when cast(latitude as float64) between 51.480 and 51.512
             and cast(longitude as float64) between -0.220 and -0.150
            then 'Kensington & Chelsea'

            else 'Other'
        end                               as zone,

        -- ── operational fields ────────────────────────────────────
        cast(docks_count as int64)        as nb_docks,
        cast(installed as bool)           as is_installed,
        cast(locked    as bool)           as is_locked,
        cast(temporary as bool)           as is_temporary,

        -- ── capacity tier ─────────────────────────────────────────
        case
            when cast(docks_count as int64) <= 15 then 'small'
            when cast(docks_count as int64) <= 24 then 'medium'
            else 'large'
        end                               as capacity_tier,

        cast(install_date as date)        as install_date

    from source

    where
        id is not null
        and cast(latitude  as float64) between 51.3 and 51.7
        and cast(longitude as float64) between -0.6 and 0.3
        and cast(docks_count   as int64)  > 0

)

select * from cleaned
