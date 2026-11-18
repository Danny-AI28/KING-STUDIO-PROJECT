DROP TABLE IF EXISTS public.flat_events;

CREATE TABLE public.flat_events AS
SELECT
  -- fix cols
  e.event_name::VARCHAR AS event_name,
  TO_DATE(e.event_date, 'YYYYMMDD') AS event_date,
  e.event_timestamp,
  TIMESTAMP '1970-01-01' + (e.event_timestamp / 1000000.0) * INTERVAL '1 second' AS event_ts,
  e.user_pseudo_id::VARCHAR AS user_pseudo_id,
  e.user_id::VARCHAR AS user_id,
  e.user_first_touch_timestamp,
  e.device.mobile_brand_name::VARCHAR AS mobile_brand_name,
  e.device.mobile_model_name::VARCHAR AS mobile_model_name,
  e.device.operating_system_version::VARCHAR AS operating_system_version,
  e.device.vendor_id::VARCHAR AS vendor_id,
  e.device.advertising_id::VARCHAR AS advertising_id,
  e.geo.country::VARCHAR AS country,
  e.geo.city::VARCHAR AS city,
  e.app_info.id::VARCHAR AS app_id,
  e.app_info.version::VARCHAR AS app_version,
  e.platform::VARCHAR AS platform,
  e.traffic_source.name::VARCHAR AS traffic_source_name,
  e.traffic_source.medium::VARCHAR AS traffic_source_medium,
  e.traffic_source.source::VARCHAR AS traffic_source,
  e.event_value_in_usd::FLOAT8 AS event_value_in_usd,

  -- user_properties (Đã kéo ::BIGINT ra ngoài hàm MAX)
  MAX(CASE WHEN up.key::VARCHAR = 'level' THEN up.value.string_value::VARCHAR END)::BIGINT AS up_level,
  MAX(CASE WHEN up.key::VARCHAR = 'win_streak' THEN up.value.string_value::VARCHAR END)::BIGINT AS up_win_streak,
  MAX(CASE WHEN up.key::VARCHAR = 'Segment' THEN up.value.string_value::VARCHAR END) AS up_segment,
  MAX(CASE WHEN up.key::VARCHAR = 'Skill' THEN up.value.string_value::VARCHAR END) AS up_skill,
  MAX(CASE WHEN up.key::VARCHAR = 'ua_network' THEN up.value.string_value::VARCHAR END) AS up_ua_network,
  MAX(CASE WHEN up.key::VARCHAR = 'attribution_id' THEN up.value.string_value::VARCHAR END) AS up_attribution_id,
  MAX(CASE WHEN up.key::VARCHAR = 'first_open_time' THEN up.value.int_value::BIGINT END) AS up_first_open_time,

  -- event_params
  -- ad_complete
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'ad_complete' THEN ep.value.int_value::BIGINT END) AS ad_complete_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'ad_complete' THEN ep.value.int_value::BIGINT END) AS ad_complete_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'ad_complete' THEN ep.value.int_value::BIGINT END) AS ad_complete_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_network' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_ad_network,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_platform' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_ad_platform,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_format' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_ad_format,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_duration' AND e.event_name = 'ad_complete' THEN ep.value.double_value::FLOAT8 END) AS ad_complete_ad_duration,
  MAX(CASE WHEN ep.key::VARCHAR = 'end_type' AND e.event_name = 'ad_complete' THEN ep.value.string_value::VARCHAR END) AS ad_complete_end_type,

  -- ad_impression
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'ad_impression' THEN ep.value.int_value::BIGINT END) AS ad_impression_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'ad_impression' THEN ep.value.int_value::BIGINT END) AS ad_impression_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'ad_impression' THEN ep.value.int_value::BIGINT END) AS ad_impression_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_network' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_ad_network,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_platform' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_ad_platform,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_format' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_ad_format,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_source' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_ad_source,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_unit_name' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_ad_unit_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'is_show' AND e.event_name = 'ad_impression' THEN ep.value.int_value::BIGINT END) AS ad_impression_is_show,
  MAX(CASE WHEN ep.key::VARCHAR = 'value' AND e.event_name = 'ad_impression' THEN ep.value.double_value::FLOAT8 END) AS ad_impression_value,
  MAX(CASE WHEN ep.key::VARCHAR = 'currency' AND e.event_name = 'ad_impression' THEN ep.value.string_value::VARCHAR END) AS ad_impression_currency,

  -- ad_request
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'ad_request' THEN ep.value.int_value::BIGINT END) AS ad_request_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'ad_request' THEN ep.value.int_value::BIGINT END) AS ad_request_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'ad_request' THEN ep.value.string_value::VARCHAR END) AS ad_request_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'ad_request' THEN ep.value.int_value::BIGINT END) AS ad_request_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_network' AND e.event_name = 'ad_request' THEN ep.value.string_value::VARCHAR END) AS ad_request_ad_network,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_platform' AND e.event_name = 'ad_request' THEN ep.value.string_value::VARCHAR END) AS ad_request_ad_platform,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_format' AND e.event_name = 'ad_request' THEN ep.value.string_value::VARCHAR END) AS ad_request_ad_format,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'ad_request' THEN ep.value.string_value::VARCHAR END) AS ad_request_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'is_load' AND e.event_name = 'ad_request' THEN ep.value.int_value::BIGINT END) AS ad_request_is_load,
  MAX(CASE WHEN ep.key::VARCHAR = 'load_time' AND e.event_name = 'ad_request' THEN ep.value.int_value::BIGINT END) AS ad_request_load_time,

  -- ad_reward
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'ad_reward' THEN ep.value.int_value::BIGINT END) AS ad_reward_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'ad_reward' THEN ep.value.int_value::BIGINT END) AS ad_reward_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'ad_unit_code' AND e.event_name = 'ad_reward' THEN ep.value.string_value::VARCHAR END) AS ad_reward_ad_unit_code,
  MAX(CASE WHEN ep.key::VARCHAR = 'reward_type' AND e.event_name = 'ad_reward' THEN ep.value.string_value::VARCHAR END) AS ad_reward_reward_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'reward_value' AND e.event_name = 'ad_reward' THEN ep.value.int_value::BIGINT END) AS ad_reward_reward_value,

  -- iap_click
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'iap_click' THEN ep.value.int_value::BIGINT END) AS iap_click_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'iap_click' THEN ep.value.int_value::BIGINT END) AS iap_click_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'iap_click' THEN ep.value.string_value::VARCHAR END) AS iap_click_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'iap_click' THEN ep.value.int_value::BIGINT END) AS iap_click_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'iap_click' THEN ep.value.string_value::VARCHAR END) AS iap_click_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'pack_name' AND e.event_name = 'iap_click' THEN ep.value.string_value::VARCHAR END) AS iap_click_pack_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'show_type' AND e.event_name = 'iap_click' THEN ep.value.string_value::VARCHAR END) AS iap_click_show_type,

  -- iap_purchase
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'iap_purchase' THEN ep.value.int_value::BIGINT END) AS iap_purchase_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'iap_purchase' THEN ep.value.int_value::BIGINT END) AS iap_purchase_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'iap_purchase' THEN ep.value.int_value::BIGINT END) AS iap_purchase_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'pack_name' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_pack_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'show_type' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_show_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'currency' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_currency,
  MAX(CASE WHEN ep.key::VARCHAR = 'price' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_price,
  MAX(CASE WHEN ep.key::VARCHAR = 'first_payment' AND e.event_name = 'iap_purchase' THEN ep.value.string_value::VARCHAR END) AS iap_purchase_first_payment,

  -- iap_show
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'iap_show' THEN ep.value.int_value::BIGINT END) AS iap_show_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'iap_show' THEN ep.value.int_value::BIGINT END) AS iap_show_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'iap_show' THEN ep.value.string_value::VARCHAR END) AS iap_show_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'iap_show' THEN ep.value.int_value::BIGINT END) AS iap_show_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'iap_show' THEN ep.value.string_value::VARCHAR END) AS iap_show_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'pack_name' AND e.event_name = 'iap_show' THEN ep.value.string_value::VARCHAR END) AS iap_show_pack_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'show_type' AND e.event_name = 'iap_show' THEN ep.value.string_value::VARCHAR END) AS iap_show_show_type,

  -- in_app_purchase
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'in_app_purchase' THEN ep.value.int_value::BIGINT END) AS in_app_purchase_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'in_app_purchase' THEN ep.value.int_value::BIGINT END) AS in_app_purchase_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'product_id' AND e.event_name = 'in_app_purchase' THEN ep.value.string_value::VARCHAR END) AS in_app_purchase_product_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'currency' AND e.event_name = 'in_app_purchase' THEN ep.value.string_value::VARCHAR END) AS in_app_purchase_currency,
  MAX(CASE WHEN ep.key::VARCHAR = 'price' AND e.event_name = 'in_app_purchase' THEN ep.value.int_value::BIGINT END) AS in_app_purchase_price,
  MAX(CASE WHEN ep.key::VARCHAR = 'value' AND e.event_name = 'in_app_purchase' THEN ep.value.int_value::BIGINT END) AS in_app_purchase_value,
  MAX(CASE WHEN ep.key::VARCHAR = 'quantity' AND e.event_name = 'in_app_purchase' THEN ep.value.int_value::BIGINT END) AS in_app_purchase_quantity,

  -- first_open
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'first_open' THEN ep.value.int_value::BIGINT END) AS first_open_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'first_open' THEN ep.value.int_value::BIGINT END) AS first_open_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'previous_first_open_count' AND e.event_name = 'first_open' THEN ep.value.int_value::BIGINT END) AS first_open_previous_first_open_count,

  -- session_start
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'session_start' THEN ep.value.int_value::BIGINT END) AS session_start_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'session_start' THEN ep.value.int_value::BIGINT END) AS session_start_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'session_engaged' AND e.event_name = 'session_start' THEN ep.value.int_value::BIGINT END) AS session_start_session_engaged,

  -- user_engagement
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'user_engagement' THEN ep.value.int_value::BIGINT END) AS user_engagement_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'user_engagement' THEN ep.value.int_value::BIGINT END) AS user_engagement_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'engagement_time_msec' AND e.event_name = 'user_engagement' THEN ep.value.int_value::BIGINT END) AS user_engagement_engagement_time_msec,

 -- resource_earn (Regex protection for FLOAT8)
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'resource_earn' THEN ep.value.int_value::BIGINT END) AS resource_earn_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'resource_earn' THEN ep.value.int_value::BIGINT END) AS resource_earn_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'resource_earn' THEN ep.value.string_value::VARCHAR END) AS resource_earn_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'reason' AND e.event_name = 'resource_earn' THEN ep.value.string_value::VARCHAR END) AS resource_earn_reason,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_amount' AND e.event_name = 'resource_earn' AND REPLACE(ep.value.string_value::VARCHAR, ',', '.') ~ '^-?[0-9]*\.?[0-9]+$' THEN REPLACE(ep.value.string_value::VARCHAR, ',', '.') END)::FLOAT8 AS resource_earn_resource_amount,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_name' AND e.event_name = 'resource_earn' THEN ep.value.string_value::VARCHAR END) AS resource_earn_resource_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_balance' AND e.event_name = 'resource_earn' AND REPLACE(ep.value.string_value::VARCHAR, ',', '.') ~ '^-?[0-9]*\.?[0-9]+$' THEN REPLACE(ep.value.string_value::VARCHAR, ',', '.') END)::FLOAT8 AS resource_earn_resource_balance,

  -- resource_spend (Regex protection for FLOAT8)
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'resource_spend' THEN ep.value.int_value::BIGINT END) AS resource_spend_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'resource_spend' THEN ep.value.int_value::BIGINT END) AS resource_spend_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'resource_spend' THEN ep.value.string_value::VARCHAR END) AS resource_spend_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'reason' AND e.event_name = 'resource_spend' THEN ep.value.string_value::VARCHAR END) AS resource_spend_reason,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_amount' AND e.event_name = 'resource_spend' AND REPLACE(ep.value.string_value::VARCHAR, ',', '.') ~ '^-?[0-9]*\.?[0-9]+$' THEN REPLACE(ep.value.string_value::VARCHAR, ',', '.') END)::FLOAT8 AS resource_spend_resource_amount,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_name' AND e.event_name = 'resource_spend' THEN ep.value.string_value::VARCHAR END) AS resource_spend_resource_name,
  MAX(CASE WHEN ep.key::VARCHAR = 'resource_balance' AND e.event_name = 'resource_spend' AND REPLACE(ep.value.string_value::VARCHAR, ',', '.') ~ '^-?[0-9]*\.?[0-9]+$' THEN REPLACE(ep.value.string_value::VARCHAR, ',', '.') END)::FLOAT8 AS resource_spend_resource_balance,
  -- egg_hatching
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'egg_hatching' THEN ep.value.int_value::BIGINT END) AS egg_hatching_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'egg_hatching' THEN ep.value.int_value::BIGINT END) AS egg_hatching_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'island' AND e.event_name = 'egg_hatching' THEN ep.value.string_value::VARCHAR END) AS egg_hatching_island,
  MAX(CASE WHEN ep.key::VARCHAR = 'rarity' AND e.event_name = 'egg_hatching' THEN ep.value.string_value::VARCHAR END) AS egg_hatching_rarity,
  MAX(CASE WHEN ep.key::VARCHAR = 'reason' AND e.event_name = 'egg_hatching' THEN ep.value.string_value::VARCHAR END) AS egg_hatching_reason,

  -- grinch_gift_heist_end
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'grinch_gift_heist_end' THEN ep.value.int_value::BIGINT END) AS grinch_gift_heist_end_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'grinch_gift_heist_end' THEN ep.value.int_value::BIGINT END) AS grinch_gift_heist_end_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'race_number' AND e.event_name = 'grinch_gift_heist_end' THEN ep.value.int_value::BIGINT END) AS grinch_gift_heist_end_race_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'event_day' AND e.event_name = 'grinch_gift_heist_end' THEN ep.value.int_value::BIGINT END) AS grinch_gift_heist_end_event_day,
  MAX(CASE WHEN ep.key::VARCHAR = 'position' AND e.event_name = 'grinch_gift_heist_end' THEN ep.value.int_value::BIGINT END) AS grinch_gift_heist_end_position,

  -- island_race_finish
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'island_race_finish' THEN ep.value.int_value::BIGINT END) AS island_race_finish_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'island_race_finish' THEN ep.value.int_value::BIGINT END) AS island_race_finish_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'result' AND e.event_name = 'island_race_finish' THEN ep.value.string_value::VARCHAR END) AS island_race_finish_result,
  MAX(CASE WHEN ep.key::VARCHAR = 'position' AND e.event_name = 'island_race_finish' THEN ep.value.string_value::VARCHAR END) AS island_race_finish_position,

  -- land_decor_unlock
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'land_decor_unlock' THEN ep.value.int_value::BIGINT END) AS land_decor_unlock_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'land_decor_unlock' THEN ep.value.int_value::BIGINT END) AS land_decor_unlock_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'placement' AND e.event_name = 'land_decor_unlock' THEN ep.value.string_value::VARCHAR END) AS land_decor_unlock_placement,
  MAX(CASE WHEN ep.key::VARCHAR = 'island' AND e.event_name = 'land_decor_unlock' THEN ep.value.string_value::VARCHAR END) AS land_decor_unlock_island,
  MAX(CASE WHEN ep.key::VARCHAR = 'option_choose' AND e.event_name = 'land_decor_unlock' THEN ep.value.string_value::VARCHAR END) AS land_decor_unlock_option_choose,

  -- level_end
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'level_end' THEN ep.value.string_value::VARCHAR END) AS level_end_mode,
  (MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END)) AS level_end_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_type' AND e.event_name = 'level_end' THEN ep.value.string_value::VARCHAR END) AS level_end_play_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_index' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_play_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'result' AND e.event_name = 'level_end' THEN ep.value.string_value::VARCHAR END) AS level_end_result,
  MAX(CASE WHEN ep.key::VARCHAR = 'lose_by' AND e.event_name = 'level_end' THEN ep.value.string_value::VARCHAR END) AS level_end_lose_by,
  MAX(CASE WHEN ep.key::VARCHAR = 'lose_index' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_lose_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'cleared_items' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_cleared_items,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_items' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_total_items,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_start' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_total_duration_start,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_end' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_total_duration_end,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_duration' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_play_duration,
  MAX(CASE WHEN ep.key::VARCHAR = 'remain_duration' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_remain_duration,
  MAX(CASE WHEN ep.key::VARCHAR = 'use_unlimited_ticket' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_use_unlimited_ticket,
  MAX(CASE WHEN ep.key::VARCHAR = 'use_get_more_time' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_use_get_more_time,
  MAX(CASE WHEN ep.key::VARCHAR = 'use_prebooster_timer' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_use_prebooster_timer,
  MAX(CASE WHEN ep.key::VARCHAR = 'use_booster_freeze' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_use_booster_freeze,
  MAX(CASE WHEN ep.key::VARCHAR = 'use_infitity_time' AND e.event_name = 'level_end' THEN ep.value.int_value::BIGINT END) AS level_end_use_infitity_time,

  -- level_exit
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'level_exit' THEN ep.value.string_value::VARCHAR END) AS level_exit_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_type' AND e.event_name = 'level_exit' THEN ep.value.string_value::VARCHAR END) AS level_exit_play_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_index' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_play_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'lose_index' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_lose_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'cleared_items' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_cleared_items,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_items' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_total_items,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_start' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_total_duration_start,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_end' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_total_duration_end,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_duration' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_play_duration,
  MAX(CASE WHEN ep.key::VARCHAR = 'remain_duration' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_remain_duration,
  MAX(CASE WHEN ep.key::VARCHAR = 'exit_index' AND e.event_name = 'level_exit' THEN ep.value.int_value::BIGINT END) AS level_exit_exit_index,

  -- level_reopen
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'level_reopen' THEN ep.value.string_value::VARCHAR END) AS level_reopen_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_index' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_play_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'lose_index' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_lose_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_start' AND e.event_name = 'level_reopen' THEN ep.value.int_value::BIGINT END) AS level_reopen_total_duration_start,

  -- level_start
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'mode' AND e.event_name = 'level_start' THEN ep.value.string_value::VARCHAR END) AS level_start_mode,
  MAX(CASE WHEN ep.key::VARCHAR = 'level' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_level,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_type' AND e.event_name = 'level_start' THEN ep.value.string_value::VARCHAR END) AS level_start_play_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'play_index' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_play_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'lose_index' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_lose_index,
  MAX(CASE WHEN ep.key::VARCHAR = 'total_duration_start' AND e.event_name = 'level_start' THEN ep.value.int_value::BIGINT END) AS level_start_total_duration_start,

  -- lucky_spin
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'lucky_spin' THEN ep.value.int_value::BIGINT END) AS lucky_spin_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'lucky_spin' THEN ep.value.int_value::BIGINT END) AS lucky_spin_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'spin_by' AND e.event_name = 'lucky_spin' THEN ep.value.string_value::VARCHAR END) AS lucky_spin_spin_by,

  -- segment_change
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'segment_change' THEN ep.value.int_value::BIGINT END) AS segment_change_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'segment_change' THEN ep.value.int_value::BIGINT END) AS segment_change_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'segment_before' AND e.event_name = 'segment_change' THEN ep.value.string_value::VARCHAR END) AS segment_change_segment_before,
  MAX(CASE WHEN ep.key::VARCHAR = 'segment_after' AND e.event_name = 'segment_change' THEN ep.value.string_value::VARCHAR END) AS segment_change_segment_after,

  -- sky_race_finish (Đã an toàn hơn với COALESCE lấy thẳng ra BIGINT)
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'sky_race_finish' THEN ep.value.int_value::BIGINT END) AS sky_race_finish_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'sky_race_finish' THEN ep.value.int_value::BIGINT END) AS sky_race_finish_ga_session_id,
  COALESCE(MAX(CASE WHEN ep.key::VARCHAR = 'position' AND e.event_name = 'sky_race_finish' THEN ep.value.int_value::BIGINT END), MAX(CASE WHEN ep.key::VARCHAR = 'position' AND e.event_name = 'sky_race_finish' THEN ep.value.string_value::VARCHAR END)::BIGINT) AS sky_race_finish_position,
  MAX(CASE WHEN ep.key::VARCHAR = 'result' AND e.event_name = 'sky_race_finish' THEN ep.value.string_value::VARCHAR END) AS sky_race_finish_result,
  MAX(CASE WHEN ep.key::VARCHAR = 'stage' AND e.event_name = 'sky_race_finish' THEN ep.value.int_value::BIGINT END) AS sky_race_finish_stage,

  -- user_info
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_number' AND e.event_name = 'user_info' THEN ep.value.int_value::BIGINT END) AS user_info_ga_session_number,
  MAX(CASE WHEN ep.key::VARCHAR = 'ga_session_id' AND e.event_name = 'user_info' THEN ep.value.int_value::BIGINT END) AS user_info_ga_session_id,
  MAX(CASE WHEN ep.key::VARCHAR = 'unlock_by' AND e.event_name = 'user_info' THEN ep.value.string_value::VARCHAR END) AS user_info_unlock_by,
  MAX(CASE WHEN ep.key::VARCHAR = 'type' AND e.event_name = 'user_info' THEN ep.value.string_value::VARCHAR END) AS user_info_type,
  MAX(CASE WHEN ep.key::VARCHAR = 'detail' AND e.event_name = 'user_info' THEN ep.value.string_value::VARCHAR END) AS user_info_detail

FROM spectrum_db.bronze_events e
LEFT JOIN e.user_properties up ON true
LEFT JOIN e.event_params ep ON true
GROUP BY
  e.event_name,
  e.event_date,
  e.event_timestamp,
  e.user_pseudo_id,
  e.user_id,
  e.user_first_touch_timestamp,
  e.device.mobile_brand_name,
  e.device.mobile_model_name,
  e.device.operating_system_version,
  e.device.vendor_id,
  e.device.advertising_id,
  e.geo.country,
  e.geo.city,
  e.app_info.id,
  e.app_info.version,
  e.platform,
  e.traffic_source.name,
  e.traffic_source.medium,
  e.traffic_source.source,
  e.event_value_in_usd;