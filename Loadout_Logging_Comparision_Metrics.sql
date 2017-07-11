
-- Player Count and Number of Loadouts used summary

-- IW Player count Summary

select 'IW' as title 
, game_type_desc
, avg(player_count) 
, stddev(player_count)
, min(player_count)
, approx_percentile(player_count, 0.5)
, approx_percentile(player_count, 0.8)
, approx_percentile(player_count, 0.9)
, approx_percentile(player_count, 0.95)
, approx_percentile(player_count, 0.99)
, approx_percentile(player_count, 0.995)
, approx_percentile(player_count, 0.999)
, max(player_count) 
, count(*) 
from (
select distinct match_id, game_type_desc , player_count from ads_iw_ms.fact_match_mp where dt >= date('2016-11-04') and dt <= date('2017-03-31')
--and private_match_flag = 0 
--and game_type_desc in ('dom', 'war') 
and player_count > 0
)
group by 1,2

-- Player Count Vs # Matches

select 'IW' as title 
--, game_type_desc
, player_count
, count(*) 
from (
select distinct match_id, game_type_desc, player_count from ads_iw_ms.fact_match_mp where dt >= date('2016-11-04') and dt <= date('2017-03-31') 
--and private_match_flag = 0 
--and game_type_desc in ('dom', 'war') 
and player_count > 0
)
group by 1,2
--,3 


-- IW Loadout Times Used 

select 'IW' as title
, game_type_desc
, avg(loadout_count) 
, stddev(loadout_count)
, min(loadout_count)
, approx_percentile(loadout_count, 0.5)
, approx_percentile(loadout_count, 0.8)
, approx_percentile(loadout_count, 0.9)
, approx_percentile(loadout_count, 0.95)
, approx_percentile(loadout_count, 0.99)
, approx_percentile(loadout_count, 0.995)
, approx_percentile(loadout_count, 0.999)
, max(loadout_count) 
, count(*) 
from (
select match_id, game_type_desc, dim_user_player_id, count(distinct loadout_index) as loadout_count
from ads_iw_ms.fact_weapon_loadout_mp 
where dt >= date('2016-11-04') and dt <= date('2017-03-31')
--and private_match_flag = 0 
and game_type_desc in ('war', 'war hc', 'dm', 'infect', 'sd', 'dom', 'gun', 'dom hc', 'koth', 'conf hc', 'conf', 'front', 'sd hc', 'dm hc', 'ball', 'tdef', 'ctf', 'grnd')
group by 1,2,3
)
group by 1,2

-- IW Loadout Times Used - Loadout Count Approach

select 'IW' as title
, game_type_desc
, loadout_count
, count(*) 
from (
select match_id, game_type_desc, dim_user_player_id, count(loadout_index) as loadout_count
from 
(
select distinct match_id, game_type_desc, dim_user_player_id, loadout_index from ads_iw_ms.fact_weapon_loadout_mp 
where dt >= date('2016-11-04') and dt <= date('2017-03-31')
--and private_match_flag = 0 
and game_type_desc in ('dom', 'war')
)
group by 1,2,3
)
group by 1,2,3

-- IW Loadout Used Loadou Count Appproach 18 Game Types

select 'IW' as title
--, game_type_desc
, loadout_count
, count(*) 
from (
select match_id, game_type_desc, dim_user_player_id, count(distinct loadout_index) as loadout_count
from ads_iw_ms.fact_weapon_loadout_mp 
where dt >= date('2016-11-04') and dt <= date('2017-03-31')
--and private_match_flag = 0 
and game_type_desc in ('war', 'war hc', 'dm', 'infect', 'sd', 'dom', 'gun', 'dom hc', 'koth', 'conf hc', 'conf', 'front', 'sd hc', 'dm hc', 'ball', 'tdef', 'ctf', 'grnd')
group by 1,2,3
)
group by 1,2

-- IW Total Kills in a Game

select 'IW' as title 
, game_type_desc
, avg(player_count) 
, stddev(player_count)
, min(player_count)
, approx_percentile(player_count, 0.9)
, approx_percentile(player_count, 0.95)
, approx_percentile(player_count, 0.99)
, approx_percentile(player_count, 0.995)
, approx_percentile(player_count, 0.999)
, max(player_count) 
, count(*) 
from 
(select match_id, game_type_desc, count(*) as player_count
from 
(
select distinct match_id, game_type_desc , dim_user_victim_id, dim_user_attacker_id, spawn_time_ms_from_match_start from ads_iw_ms.fact_kills_mp where dt >= date('2016-11-04') and dt <= date('2017-03-31') 
and private_match_flag = 0 
)
group by 1,2
)
group by 1,2


##################################################################################################################################################################################

---------------------------------------------------------------------------------- BO3 -------------------------------------------------------------------------------------------
-- BO3 Player count Summary

select 'BO3' as title 
, game_type_description
, avg(player_count) 
, stddev(player_count)
, min(player_count)
--, approx_percentile(player_count, 0.5)
--, approx_percentile(player_count, 0.8)
, approx_percentile(player_count, 0.9)
, approx_percentile(player_count, 0.95)
, approx_percentile(player_count, 0.99)
, approx_percentile(player_count, 0.995)
, approx_percentile(player_count, 0.999)
, max(player_count) 
, count(*) 
from (
select distinct match_id, game_type_description, player_count from ads_ops3_ms.fact_match_mp 
where ds between 5789 and 5940 
and ds_date_id >= 5789 and ds_date_id <= 5935
--and private_match_flag = 0 
--and game_type_description in ('dom', 'tdm') 
and player_count > 0
)
group by 1,2

-- Player Count Level Summary

select 'BO3' as title 
, player_count
, count(*) 
from (
select distinct match_id, game_type_description, player_count from ads_ops3_ms.fact_match_mp 
where ds between 5789 and 5940 
and ds_date_id >= 5789 and ds_date_id <= 5935

)
group by 1,2 


-- BO3 Loadout Times Used
-- Including both default and cistom loadouts

select 'BO3' as title
, game_type_description
, avg(loadout_count1 + loadout_count2) 
, stddev(loadout_count1 + loadout_count2)
, min(loadout_count1 + loadout_count2)
--, approx_percentile(loadout_count1 + loadout_count2, 0.5)
--, approx_percentile(loadout_count1 + loadout_count2, 0.8)
, approx_percentile(loadout_count1 + loadout_count2, 0.9)
, approx_percentile(loadout_count1 + loadout_count2, 0.95)
, approx_percentile(loadout_count1 + loadout_count2, 0.99)
, approx_percentile(loadout_count1 + loadout_count2, 0.995)
, approx_percentile(loadout_count1 + loadout_count2, 0.999)
, max(loadout_count1 + loadout_count2) 
, count(*) 
from (
select match_id, game_type_description, dim_user_victim_id, count(distinct victim_custom_loadout_index) as loadout_count1, count(distinct victim_default_loadout_index) as loadout_count2
from ads_ops3_ms.fact_kills_mp 
where ds between 5789 and 5940 
and ds_date_id >= 5789 and ds_date_id <= 5935
--and private_match_flag = 0 
and game_type_description in ('dom', 'tdm')
group by 1,2,3
)
group by 1,2

-- Only custom loadout 

select 'BO3' as title
, game_type_description
, avg(loadout_count) 
, stddev(loadout_count)
, min(loadout_count)
--, approx_percentile(loadout_count, 0.5)
--, approx_percentile(loadout_count, 0.8)
, approx_percentile(loadout_count, 0.9)
, approx_percentile(loadout_count, 0.95)
, approx_percentile(loadout_count, 0.99)
, approx_percentile(loadout_count, 0.995)
, approx_percentile(loadout_count, 0.999)
, max(loadout_count) 
, count(*) 
from (
select match_id, game_type_description, dim_user_victim_id, count(distinct victim_custom_loadout_index) as loadout_count 
from ads_ops3_ms.fact_kills_mp 
where ds between 5789 and 5940 
and ds_date_id >= 5789 and ds_date_id <= 5935
--and private_match_flag = 0 
and game_type_description in ('tdm', 'dom'
--'dm', 'sd', 'hctdm', 'gun', 'hcdom', 'conf', 'fr', 'koth', 'escort' 
--'ball', 'hcconf', 'ctf', 'dem', 'hcdm', 'hcsd', 'hcctf' 
) 
AND victim_custom_loadout_index IS NOT NULL
group by 1,2,3 
--having loadout_count > 0 
)
group by 1,2



-- BO3 Loadout Times Used - Loadout Count Approach
-- Executing For all the Game Types at one go pushes Presto beyond its 50 GB cache limit, Hence only for Top 5 Game Types considered

select 'BO3' as title
, game_type_description
, loadout_count
, count(*) 
from 
(
select match_id, game_type_description, dim_user_victim_id, count(distinct victim_custom_loadout_index) as loadout_count 
from ads_ops3_ms.fact_kills_mp 
where ds between 5789 and 5940 
and ds_date_id >= 5789 and ds_date_id <= 5935
and game_type_description in ('dom', 'tdm') 
and victim_custom_loadout_index is not null 
group by 1,2,3 

)
group by 1,2,3 


select 'BO3' as title 
, game_type_description
, avg(player_count) 
, stddev(player_count)
, min(player_count)
, approx_percentile(player_count, 0.9)
, approx_percentile(player_count, 0.95)
, approx_percentile(player_count, 0.99)
, approx_percentile(player_count, 0.995)
, approx_percentile(player_count, 0.999)
, max(player_count) 
, count(*) 
from 
(
select match_id, game_type_description, count(*) as player_count
from 
(
select distinct match_id, game_type_description , dim_user_victim_id, dim_user_killer_id, spawn_time, death_time from ads_ops3_ms.fact_kills_mp 
where ds between 5789 and 5850 
and ds_date_id >= 5789 and ds_date_id <= 5811
)
group by 1,2
)
group by 1,2



-- WW2 Lives COUNT

select 
 'S2' as title 
, gametype_s
, avg(player_count) 
, stddev(player_count)
, min(player_count)
, approx_percentile(player_count, 0.9)
, approx_percentile(player_count, 0.95)
, approx_percentile(player_count, 0.99)
, approx_percentile(player_count, 0.995)
, approx_percentile(player_count, 0.999)
, max(player_count) 
, count(*) 
from 
(
select context_data_matchid_i, gametype_s, count(*) as player_count 
from 
(
select distinct 
 a.player_i 
, a.killer_i
, a.context_data_matchid_i
, b.map_s
, b.gametype_s
, round(durationdeciseconds_i/10,1) as time_to_death 
, durationdeciseconds_i
, spawntimedecisecondsfrommatchstart_i
from datapress_v2_merged_s2_dev.mp_mp_matchdata_data_lives a
join (
    select distinct map_s, gametype_s, context_data_matchid_i, starttimeutc_i, lifecount_i, playercount_i, hasbots_b from datapress_v2_merged_s2_dev.mp_mp_matchdata_data 
    where context_data_matchid_i in (select distinct context_data_matchid_i from datapress_v2_merged_s2_dev.mp_mp_matchdata_headers where cast(trim(ds_upload_timestamp_s) as bigint) >= 1497030330)
	) b 
on a.context_data_matchid_i = b.context_data_matchid_i 
where b.lifecount_i > 50
and b.playercount_i >= 12
and b.hasbots_b = FALSE 
and date(from_unixtime(b.starttimeutc_i, -7, 0)) BETWEEN date('2017-06-13') and date('2017-06-15')
and b.gametype_s in ('war') -- Game Type filter war-TDM, dom-WAR, dom-DOM

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0 
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)
)
group by 1,2 
) 
group by 1,2


/*
'tdm', 'dom'
'dm', 'sd', 'hctdm', 'gun', 'hcdom', 'conf', 'fr', 'koth', 'escort' 
'ball', 'hcconf', 'ctf', 'dem', 'hcdm', 'hcsd', 'hcctf'
*/
