-- COD: World War 2 

-- Time to deaths for all Spawns

select 
 avg(time_to_death) as avg_time_to_death
,stddev(time_to_death) as sd_time_to_death
,min(time_to_death) as min_time_to_Death
, approx_percentile(time_to_death, 0.05) as fifth_perc_time_to_death
, approx_percentile(time_to_death, 0.25) as twentyfifth_perc_time_to_death
, approx_percentile(time_to_death, 0.50) as fifteeth_perc_time_to_death
, approx_percentile(time_to_death, 0.75) as seventyfifth_perc_time_to_death 
, approx_percentile(time_to_death, 0.95) as ninetyfifth_perc_time_to_death
,max(time_to_death) as max_time_to_death
,count(*) 
from 
(
select distinct 
 a.player_i 
, a.killer_i
, a.context_headers_event_id_s
, b.map_s
, b.gametype_s
, round(durationdeciseconds_i/10,1) as time_to_death 
, durationdeciseconds_i
, spawntimedecisecondsfrommatchstart_i
from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives a
join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
    ) b 
on a.context_headers_event_id_s = b.context_headers_event_id_s 

-- Match Data Filter
where b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war') -- Game Type filter war-TDM, dom-WAR, dom-DOM

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0 
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)
)

----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------- SHARE AND EXPLAIN THE LOGIC USED WITH DYLAN TO AVOID CAVEATS IF EXISTS ANY --------------------------------------
-- Time to first blood (death)

select 
 avg(time_to_first_blood)
,stddev(time_to_first_blood) as sd_time_to_death
,min(time_to_first_blood) as min_time_to_Death
, approx_percentile(time_to_first_blood, 0.05) as fifth_perc_time_to_first_blood
, approx_percentile(time_to_first_blood, 0.25) as twentyfifth_perc_time_to_first_blood
, approx_percentile(time_to_first_blood, 0.50) as fifteeth_perc_time_to_first_blood
, approx_percentile(time_to_first_blood, 0.75) as seventyfifth_perc_time_to_first_blood 
, approx_percentile(time_to_first_blood, 0.95) as ninetyfifth_perc_time_to_first_blood
,max(time_to_first_blood) as max_time_to_death
,count(*)
from(
select 
 context_headers_event_id_s
,death_time as time_to_first_blood
from 
(
 select x.killer_i
,x.player_i
,x.context_headers_event_id_s
,rank() over (partition by x.context_headers_event_id_s order by (x.spawntimedecisecondsfrommatchstart_i + x.durationdeciseconds_i)) as kill_rank
,(x.spawntimedecisecondsfrommatchstart_i + x.durationdeciseconds_i)/10 as death_time 
from 
(
select distinct 
 a.killer_i
,a.player_i
,a.context_headers_event_id_s
,(a.spawntimedecisecondsfrommatchstart_i/10) as spawn_time
,(a.spawntimedecisecondsfrommatchstart_i + a.durationdeciseconds_i)/10 as death_time 
,a.durationdeciseconds_i
,a.spawntimedecisecondsfrommatchstart_i 
,b.utc_start_time_i

from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives a 

join (
   select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b 
on a.context_headers_event_id_s = b.context_headers_event_id_s 

-- Match Data Filter
where b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war') -- Game Type filter 'war'-TDM, 'dom'-WAR, 'dom'-DOM

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0 
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)
) x 
) y
where kill_rank =1
group by 1,2) as z

----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------

--bad spawn group 1 

select 
 sum(Case when durationseconds_i <=3 then 1 else 0 end) as bad_spawns
,count(*) as total_spawns
,sum(Case when durationseconds_i <=3 then 1 else 0 end)*1.0/count(*) as bad_spawn_rate_group1
from(
select distinct  a.player_i
, a.killer_i
, a.context_headers_event_id_s
, a.durationdeciseconds_i/10 as durationseconds_i
,1 as victim_flag 
, (a.spawntimedecisecondsfrommatchstart_i + a.durationdeciseconds_i)/10 as death_time 
, a.durationdeciseconds_i as durationdeciseconds_i
, a.spawntimedecisecondsfrommatchstart_i
, b.utc_start_time_i
from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives as a                                         
join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b 
on a.context_headers_event_id_s = b.context_headers_event_id_s 

-- Match Data Filter
where b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war') -- Game Type filter 'war'-TDM, 'dom'-WAR, 'dom'-DOM
-- Lives Data Filters

and a.player_i <> 0 
and a.killer_i <> 0 
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)
     )

--------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------


-- Time to First Engagement

select 
 avg(engagement_time) as avg_eng_time
,stddev(engagement_time) as std_eng_time
,min(engagement_time) as min_eng_time
,approx_percentile(engagement_time, 0.05) as fifth_perc_eng_time
,approx_percentile(engagement_time, 0.25) as twentyfifth_perc_eng_time
,approx_percentile(engagement_time, 0.50) as fiftieth_perc_eng_time
,approx_percentile(engagement_time, 0.75) as seventyfifth_perc_eng_time
,approx_percentile(engagement_time, 0.95) as ninetyfifth_perc_eng_time
,max(engagement_time) as max_eng_time
,count(*)
from (
select 
 user_id
,time_of_action
,match_id
,engagement_time
,rank() over (partition by user_id, match_id order by time_of_action) as spawn_rank
from (
select 
 user_id
,time_of_action
,match_id
,time_of_next_action
,(time_of_next_action-time_of_action) as engagement_time
from
(
select 
 user_id
,time_of_action
,spawn_flag
,match_id
,lead(time_of_action,1,0) over (partition by user_id,match_id order by time_of_action) as time_of_next_action

from (
select distinct 
 a.player_i as user_id
, (b.utc_start_time_i + (a.spawntimedecisecondsfrommatchstart_i/10)) as time_of_action 
, a.spawntimedecisecondsfrommatchstart_i
, d.connecttimeutc_i 
, b.utc_start_time_i
, a.context_headers_event_id_s as match_id
, 1 as spawn_flag 

from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives as a 

join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b -- Filter Valid Matches
on a.context_headers_event_id_s = b.context_headers_event_id_s 

join (select distinct context_headers_event_id_s, context_players_index, connecttimeutc_i from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players) d 

on a.player_i  = d.context_players_index
and a.context_headers_event_id_s = d.context_headers_event_id_s

-- Player Match Data Filter 
where 
(d.connecttimeutc_i - b.utc_start_time_i) <= 10 

-- Match Data Filters 
and b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war')

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)

-- Identifying act of Kill by the user

union all

select distinct 
  a.killer_i as user_id
, (b.utc_start_time_i + (a.spawntimedecisecondsfrommatchstart_i/10)) + (a.durationdeciseconds_i/10) as time_of_action 
, a.spawntimedecisecondsfrommatchstart_i
, a.context_headers_event_id_s as match_id 
, d.connecttimeutc_i 
, b.utc_start_time_i
, 0 as spawn_flag
from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives as a 

join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b -- Joining with Match Data to filter out the valid matches from SHG Playlist and also get the match start and end time
on a.context_headers_event_id_s = b.context_headers_event_id_s 

join (select distinct  context_headers_event_id_s, context_players_index, connecttimeutc_i from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players) d 
on a.player_i  = d.context_players_index
and a.context_headers_event_id_s = a.context_headers_event_id_s 

-- Player Match Data Filter 
where (d.connecttimeutc_i - b.utc_start_time_i) <= 10 

-- Match Data Filters 
and b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war')

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)

-- Identifying act of Death by the user
union all

select distinct 
  a.player_i as user_id
, (b.utc_start_time_i + (a.spawntimedecisecondsfrommatchstart_i/10)) + (a.durationdeciseconds_i/10) as time_of_action
, a.context_headers_event_id_s as match_id 
, a.spawntimedecisecondsfrommatchstart_i
, d.connecttimeutc_i 
, b.utc_start_time_i
,0 as spawn_flag
from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_lives as a 

join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b -- Joining with Match Data to filter out the valid matches from SHG Playlist and also get the match start and end time
on a.context_headers_event_id_s = b.context_headers_event_id_s 

join (select distinct  context_headers_event_id_s, context_players_index, connecttimeutc_i from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players) d 
on a.player_i  = d.context_players_index
and a.context_headers_event_id_s = d.context_headers_event_id_s

-- Player Match Data Filter 
where (d.connecttimeutc_i - b.utc_start_time_i) <= 10 

-- Match Data Filters 
and b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom', 'war')

-- Lives Data Filters
and a.player_i <> 0 
and a.killer_i <> 0
and a.durationdeciseconds_i > 0 
and (spawnpos_ai[1] > 0 or spawnpos_ai[2] > 0 or spawnpos_ai[3] > 0)
) as z 
) as y
where spawn_flag =1 and time_of_next_action > time_of_action 
) as x 
) as a 
where spawn_rank =1 and engagement_time > 0





-- KD RATIO ---------------------------------------------------------- This needs to be resolved --------------------------------------------------------------------------------------
-- Player Id to Xuid Mapping Table does not exist 

-- Alternate way is to use datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players_client table

select avg(kd_ratio) as avg_kd_ratio
,stddev(kd_ratio) as std_kd_ratio
,min(kd_ratio) as min_kd_ratio
,approx_percentile(kd_ratio, 0.05) as fifth_perc_kd_ratio
,approx_percentile(kd_ratio, 0.25) as twentyfifth_perc_kd_ratio
,approx_percentile(kd_ratio, 0.50) as fiftieth_perc_kd_ratio
,approx_percentile(kd_ratio, 0.75) as seventyfifth_perc_kd_ratio
,approx_percentile(kd_ratio, 0.95) as ninetyfifth_perc_kd_ratio
,max(kd_ratio) as max_kd_ratio
,count(*)
from 
(
select 
gamer_tag_s, 
case 
		when deaths_total=0 then kills_total
        else kills_total*1.0/deaths_total
        end as kd_ratio
from 
(
select c.gamer_tag_s
    , sum(enddeaths_i - startdeaths_i) as deaths_total
     , sum(endkills_i - startkills_i) as kills_total 
from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players a

join (
    select distinct map_s, gametype_s, context_headers_event_id_s, utc_start_time_i, life_count_i, player_count_i, has_bots_b from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_commonmatchdata 
	) b  -- Joining with Match Data to filter out the valid matches from SHG Playlist and also get the match start and end time
on a.context_headers_event_id_s = b.context_headers_event_id_s 

join (select * from datapress_v3_merged_s2_dev.mp_ddl_mp_matchdata_data_players_client where trim(gamer_tag_s) <> '') c 

on a.context_headers_event_id_s = c.context_headers_event_id_s
and a.context_players_index =  c.context_players_index 

-- Match Data Filters
where b.life_count_i > 50
and b.player_count_i >= 12
and b.has_bots_b = FALSE 
and b.gametype_s in ('dom') 

group by 1
) 
) foo 
