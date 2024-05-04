
--use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

--alter table pd modify L1_Status varchar(30);
alter table player_details alter column L1_Status type varchar(30);
--alter table pd modify L2_Status varchar(30);
alter table player_details alter column L2_Status type varchar(30);
--alter table pd modify P_ID int primary key;
alter table player_details alter column P_ID set data type integer, add primary key (P_ID);
--alter table pd drop myunknowncolumn;

-- alter table ld drop myunknowncolumn;
-- alter table ld change timestamp start_datetime datetime;
-- alter table ld modify Dev_Id varchar(10);
-- alter table ld modify Difficulty varchar(15);
-- alter table level_details add primary key(P_ID,Dev_id,start_datetime);


create table level_details (id int, P_ID int, Dev_ID varchar, TimeStamp timestamp, Stages_crossed int, Level int, Difficulty varchar, Kill_Count	int, Headshots_Count int, Score int, Lives_Earned int);
copy level_details from 'C:\Program Files\PostgreSQL\16\data\data copy\level_details2.csv' delimiter ',' csv header;
select * from level_details;

create table player_details (id int, P_ID int, PName varchar, L1_Status int, L2_Status int, L1_Code varchar, L2_Code varchar);
copy player_details from 'C:\Program Files\PostgreSQL\16\data\data copy\player_details.csv' delimiter ',' csv header;
select * from player_details;


select * from player_details;
select * from level_details;

-- pd (P_ID,PName, L1_status, L2_Status, L1_code, L2_Code)
-- ld (P_ID, Dev_ID, start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)


-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0
select pd.P_ID, ld.Dev_ID, pd.PName, ld.Difficulty 
from player_details as pd 
inner join level_details as ld 
on pd.P_ID = ld.P_ID  
where ld.level = 0;
--done


-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed
select pd.L1_Code, avg(ld.kill_count) as Avg_Kill_Count 
from player_details as pd 
join level_details as ld 
on pd.P_ID = ld.P_ID
where ld.Lives_Earned = 2 and ld.stages_crossed >= 3
group by pd.L1_Code;
--done


-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.
select difficulty, sum(stages_crossed) as total_stages_crossed 
from level_details
where level = 2 and Dev_ID like 'zm%'
group by Difficulty 
order by total_stages_crossed desc;
--done


-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.
select P_ID, count(distinct date(timestamp)) as total_unique_dates 
from level_details 
group by P_ID
having count(distinct date(timestamp)) > 1;
--done


-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.
select P_ID, level, sum(Kill_Count) as "sum of kill_counts"
from level_details
where Difficulty = 'Medium'
group by P_ID, level
having sum(Kill_Count) > (select avg(Kill_Count) from level_details where Difficulty = 'Medium');
--done


-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.
select ld.level, pd.L1_Code, pd.L2_Code, sum(ld.Lives_Earned) as total_lives_earned
from level_details as ld
inner join player_details as pd
on ld.P_ID = pd.P_ID
where ld.level != 0
group by ld.level,pd.L1_Code, pd.L2_Code  
order by ld.level asc;
--done


-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 
select dev_id, difficulty, score, rank
from (select score, dev_id, difficulty,
	  row_number() over (partition by dev_id order by score asc ) as rank 
	  from level_details ) as ranked_scores
where rank <= 3;
--done


-- Q8) Find first_login datetime for each device id
select dev_id, min(timestamp) as first_login_datetime
from level_details
group by dev_id;
--order by min(timestamp);
--done


-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.
select dev_id, difficulty, score, rank
from (select score, dev_id, difficulty,
	  rank() over (partition by difficulty order by score asc ) as rank 
	  from level_details ) as ranked_scores
where rank <= 5;
--done


-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.
select p_id, dev_id, min(timestamp) as first_login_datetime
from level_details
group by dev_id, p_id;
--done


-- Q11) For each player and date, how many kill_count played so far by the player. 
-- That is, the total number of games played by the player until that date.

-- a) window function
select distinct p_id, date(timestamp) as date, 
sum(kill_count) over(partition by p_id, date(timestamp) order by date(timestamp)) as "kill_counts"
from level_details 
-- group by p_id, date(timestamp)
order by p_id, date(timestamp);
--done


-- b) without window function
select p_id, date(timestamp), sum(kill_count) as "kill_counts"
from level_details
group by p_id, date(timestamp)
order by p_id, date(timestamp);
--done


-- Q12) Find the cumulative sum of stages crossed over a start_datetime 
select timestamp, sum(Stages_crossed) as sum_of_stages
from level_details
group by timestamp;
--done

-- Q13) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime
select p_id, TimeStamp, sum(Stages_crossed) as sum_of_stages
from (select p_id, 
	  timestamp,
	  stages_crossed, 
	  row_number() over (partition by p_id order by TimeStamp desc) as rank 
	  from level_details 
	 )
where rank > 1
group by p_id, TimeStamp
order by p_id;
--done


-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id
select P_id, dev_id, sum(score) as total, rank
from (select P_id, dev_id, score,
	 row_number() over (partition by dev_id order by sum(score) desc) as rank
	 from level_details
	 group by p_id, dev_id, score) as tb
where rank < 4
group by P_id, dev_id, rank
order by p_id;
--done


-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id
select p_id, sum(score) as total_score
from level_details
group by p_id
having sum(score)  > 0.5 * (select avg(score) from level_details)
-- done


-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

-- PROCEDURE: public.top_n_headshots_procedure(integer)
-- DROP PROCEDURE IF EXISTS public.top_n_headshots_procedure(integer);
CREATE OR REPLACE PROCEDURE public.top_n_headshots_procedure(
	IN n integer, inout get_result refcursor)
LANGUAGE 'plpgsql'
AS $BODY$
begin

	open get_result for 	
	select dev_id, Headshots_Count, difficulty, rank
	from (select dev_id, difficulty, Headshots_Count, row_number() over (partition by dev_id order by Headshots_Count asc) as rank
		  from level_details
		 )as new_table
	where rank <= n
--group by dev_id, rank, difficulty, Headshots_Count
	order by dev_id, rank; 

end
$BODY$;
ALTER PROCEDURE public.top_n_headshots_procedure(integer)
    OWNER TO postgres;

call public.top_n_headshots_procedure (5, 'result');
fetch all in "result";
--done


-- Q17) Create a function to return sum of Score for a given player_id

-- function name - total_score_func
create or replace function total_score_func(player_id int) 
returns int
as $body$
declare total_score int;
begin
	select sum(score) into total_score
	from level_details
	where p_id = player_id; 
	
	return total_score;

	DROP FUNCTION total_score_func(integer);
end;
$body$
language plpgsql;

select total_score_func(368) as "sum of Score ";
--done

--     The End