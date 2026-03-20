-- models/marts/dim_date.sql
-- ===========================
-- Date dimension — spine of dates covering the full ride history.
-- Includes UK bank holidays (hardcoded for 2015-2024 key dates).

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimension']
  )
}}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2015-01-01' as date)",
        end_date   = "cast('2025-12-31' as date)"
    ) }}

),

-- UK public holidays (key dates — extend as needed)
uk_bank_holidays as (

    select date('2023-01-02') as holiday_date, 'New Year Holiday'       as holiday_name union all
    select date('2023-04-07'),                  'Good Friday'                             union all
    select date('2023-04-10'),                  'Easter Monday'                           union all
    select date('2023-05-01'),                  'Early May Bank Holiday'                  union all
    select date('2023-05-29'),                  'Spring Bank Holiday'                     union all
    select date('2023-08-28'),                  'Summer Bank Holiday'                     union all
    select date('2023-12-25'),                  'Christmas Day'                           union all
    select date('2023-12-26'),                  'Boxing Day'                              union all
    select date('2024-01-01'),                  'New Year Day'                            union all
    select date('2024-03-29'),                  'Good Friday'                             union all
    select date('2024-04-01'),                  'Easter Monday'                           union all
    select date('2024-05-06'),                  'Early May Bank Holiday'                  union all
    select date('2024-05-27'),                  'Spring Bank Holiday'                     union all
    select date('2024-08-26'),                  'Summer Bank Holiday'                     union all
    select date('2024-12-25'),                  'Christmas Day'                           union all
    select date('2024-12-26'),                  'Boxing Day'

),

final as (

    select
        cast(format_date('%Y%m%d', d.date_day) as int64) as date_id,
        d.date_day                                        as full_date,
        extract(year    from d.date_day)                  as year,
        extract(quarter from d.date_day)                  as quarter,
        extract(month   from d.date_day)                  as month,
        format_date('%B', d.date_day)                     as month_name,
        format_date('%b', d.date_day)                     as month_abbr,
        extract(week    from d.date_day)                  as week_number,
        extract(dayofweek from d.date_day)                as day_of_week,
        format_date('%A', d.date_day)                     as day_name,
        format_date('%a', d.date_day)                     as day_abbr,

        -- Weekend flag (1=Sun, 7=Sat in BigQuery)
        extract(dayofweek from d.date_day) in (1, 7)      as is_weekend,

        -- UK Bank holiday
        h.holiday_date is not null                         as is_uk_bank_holiday,
        h.holiday_name                                     as bank_holiday_name,

        -- Season (Northern Hemisphere)
        case
            when extract(month from d.date_day) in (12, 1, 2) then 'winter'
            when extract(month from d.date_day) in (3, 4, 5)  then 'spring'
            when extract(month from d.date_day) in (6, 7, 8)  then 'summer'
            else                                                    'autumn'
        end as season

    from date_spine d

    left join uk_bank_holidays h
        on d.date_day = h.holiday_date

)

select * from final
