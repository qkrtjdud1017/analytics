select
   category,
   avg(cpc) as avg_cpc 
from
   (
(
      select
         left(p.category_id, 3) as category, pu.ref_campaign, avg(pu.point) / count(*) as cpc 
      from
         (
            select
               r.content_id,
               r.ref_campaign,
               pup.pay_sum as point 
            from
               (
                  select
                     content_id,
                     ref_source,
                     ref_campaign 
                  from
                     bun_log_db.app_event_type_view 
                  where
                     year || month || day >= '20200401' 
                     and year || month || day <= '20200430' 
                     and ref_source in 
                     (
                        'power_up'
                     )
               )
               r 
               join
                  service1_quicket.ad_power_up_point pup 
                  on pup.pu_id = r.ref_campaign 
            where
               pup.created_at >= '2020-03-01 00:00:00'
         )
         pu 
         join
            service1_quicket.product_info p 
            on pu.content_id = p.id 
      group by
         left(p.category_id, 3), pu.content_id, pu.ref_campaign) 
      UNION
(
      select
         left(p.category_id, 3) as category, sus.ref_campaign, avg(sus.point) / count(*) as cpc 
      from
         (
            select
               r.content_id,
               r.ref_campaign,
               susp.pay_sum as point 
            from
               (
                  select
                     content_id,
                     ref_source,
                     ref_campaign 
                  from
                     bun_log_db.app_event_type_view 
                  where
                     year || month || day >= '20200401' 
                     and year || month || day <= '20200430' 
                     and ref_source in 
                     (
                        'ad_super_up_shop'
                     )
               )
               r 
               join
                  service1_quicket.ad_super_up_shop_point susp 
                  on susp.sus_id = r.ref_campaign 
            where
               susp.create_at >= '2020-03-01 00:00:00'
         )
         sus 
         join
            service1_quicket.product_info p 
            on sus.content_id = p.id 
      group by
         left(p.category_id, 3), sus.content_id, sus.ref_campaign) 
      union
( 
      select
         left(p.category_id, 3) as category, su.ref_campaign, avg(su.point) / count(*) as cpc 
      from
         (
            select
               r.content_id,
               r.ref_campaign,
               sup.pay_sum as point 
            from
               (
                  select
                     content_id,
                     ref_source,
                     ref_campaign 
                  from
                     bun_log_db.app_event_type_view 
                  where
                     year || month || day >= '20200401' 
                     and year || month || day <= '20200430' 
                     and ref_source in 
                     (
                        'ad_super_up'
                     )
               )
               r 
               join
                  service1_quicket.ad_super_up_point sup 
                  on sup.suid = r.ref_campaign 
            where
               sup.create_at >= '2020-03-01 00:00:00'
         )
         su 
         join
            service1_quicket.product_info p 
            on su.content_id = p.id 
      group by
         left(p.category_id, 3), su.content_id, su.ref_campaign ) 
   )
group by
   category
;

SELECT
   depth1,
   Count(DISTINCT uid) AS user_cnt,
   Count(DISTINCT content_id) AS content_cnt,
   case
      when
         user_cnt = 0 
         or user_cnt is NULL 
      then
         0 
      else
         Cast(content_cnt AS FLOAT) / Cast(user_cnt AS FLOAT) 
   end
   AS avg_content_cnt, Avg(ctr) AS avg_ctr 
FROM
   (
      SELECT
         c.content_id,
         c.imp_cnt,
         c.view_cnt,
         p.uid,
         Cast(c.view_cnt AS FLOAT) / Cast(c.imp_cnt AS FLOAT) AS ctr,
         LEFT(p.category_id, 3) AS depth1
      FROM
         (
            SELECT
               imp_content_id AS content_id,
               Count(*) AS imp_cnt,
               COALESCE(Count(view_content_id), 0) AS view_cnt 
            FROM
               (
                  SELECT
                     i.content_id AS imp_content_id,
                     v.content_id AS view_content_id
                  FROM
                     bun_log_db.app_event_type_impression i 
                     LEFT OUTER JOIN
                        bun_log_db.app_event_type_view v 
                        ON v.imp_id = i.imp_id 
                  WHERE
                     i.year || i.month || i.day >= '20200401' 
                     AND i.year || i.month || i.day <= '20200430' 
                     AND i.ref_source IN 
                     (
                        'ad_super_up',
                        'ad_super_up_shop',
                        'power_up' 
                     )
               )
               l 
            GROUP BY
               imp_content_id 
         )
         c 
         JOIN
            service1_quicket.product_info p 
            ON c.content_id = p.id 
   )
GROUP BY
   depth1;




select 
	case 
		when s.ref_term is not null then s.ref_term
		when ia.ref_term is not null then ia.ref_term
	else
		inon.ref_term
	end as keyword, s.count as search, ia.count as imp_ad, inon.count as imp_non_ad
from 
(
select search_term as ref_term, count(*)
from bun_log_db.app_event_type_search
where year||month||day >= '20200401' and year||month||day <= '20200414'
group by search_term
limit 200
) s
full OUTER JOIN
(
select ref_term, count(*)
from bun_log_db.app_event_type_impression
where year||month||day >= '20200401' and year||month||day <='20200414' 
    and ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up') 
    and ref_term is not null and ref_term != ''
group by ref_term
limit 200
) ia on s.ref_term = ia.ref_term
full OUTER JOIN
(
select ref_term, count(*)
from bun_log_db.app_event_type_impression
where year||month||day >= '20200401' and year||month||day <='20200414' 
    and ref_campaign is null
    and ref_term is not null and ref_term != ''
group by ref_term
limit 200
) inon on s.ref_term = inon.ref_term
order by s.count, ia.count, inon.count;

select *
from bun_log_db.app_event_type_view
where year||month||day = '20200201'
limit 10;

SELECT
	month, ref_source, avg(ctr) as avg_ctr
FROM
   (
      		SELECT
      		   month,
               ref_source,
               imp_content_id AS content_id,
               Count(*) AS imp_cnt,
               COALESCE(Count(view_content_id), 0) AS view_cnt,
               cast(view_cnt as float)/cast(imp_cnt as float) as ctr  
            FROM
               (
                  SELECT
                  	 i.year||i.month as month,
                  	 i.ref_source,
                     i.content_id AS imp_content_id,
                     v.content_id AS view_content_id
                  FROM
                     bun_log_db.app_event_type_impression i 
                     LEFT OUTER JOIN
                        bun_log_db.app_event_type_view v 
                        ON v.imp_id = i.imp_id 
                  WHERE
                     i.year || i.month >= '202001' 
                     AND i.year || i.month <= '202003' 
                     AND i.ref_source IN 
                     (
                        'ad_super_up',
                        'ad_super_up_shop',
                        'power_up' 
                     )
               )
               l 
            GROUP BY
               month, ref_source, imp_content_id 
         )
GROUP BY
   month, ref_source;
   
WITH temp
     AS (SELECT p.UID,
                p.create_date,
                Row_number()
                  over (
                    ORDER BY p.UID, p.create_date) AS row_num
         FROM   service1_quicket.product_info p
         WHERE  p.create_date BETWEEN '{start_date}' AND '{end_date}'
         GROUP  BY p.UID,
                   p.create_date
         ORDER  BY p.UID,
                   p.create_date)
SELECT base.UID,
       base.create_date,
       Datediff(hour, base.create_date :: timestamp,
       future.create_date :: timestamp)
       AS diff
FROM   temp AS base
       left join temp AS future
              ON base.row_num + 1 = future.row_num
WHERE  base.create_date BETWEEN '{start_date}' AND '{end_date}'
ORDER  BY base.UID,
          base.create_date;
          
with temp as (select user_id, content_id, event_time, row_number() over(order by user_id, content_id, event_time asc) as row_num
			  from bun_log_db.app_event_type_view 
			  where year||month||day='20200510' 
			  group by user_id, content_id, event_time
			  order by user_id, content_id, event_time)
select user_id, content_id, avg(diff) as avg_sec, count(*)+1 as same_product_click_cnt
from (
select base.user_id, base.content_id, datediff(second, base.event_time::timestamp, future.event_time::timestamp) as diff
from temp as base
	left join temp as future on base.row_num +1 = future.row_num
where base.content_id = future.content_id
order by base.user_id, base.content_id, base.event_time
)
where diff > 0
group by user_id, content_id;


with temp as (
	select user_id, event_time, row_number() over(order by user_id, event_time asc) as num
	from (
		select user_id, content_id, event_time, row_number() over(partition by user_id, content_id order by event_time asc) as row_num
		from bun_log_db.app_event_type_view
		where year||month||day = '20200510'
	)
	where row_num = 1
	group by user_id, event_time
	order by user_id, event_time asc
)
select user_id, diff
from (
select base.user_id, datediff(second, base.event_time::timestamp, future.event_time::timestamp) as diff
from temp as base
	left join temp as future on base.num + 1 = future.num
order by base.user_id, base.event_time
)
where diff > 0;


select v.
from bun_log_db.app_event_type_view v
join service1_quicket.product_info p on v.content_id=p.id
where v.year||v.month||v.day = '20200510'
group by v.user_id, v.content_id;

select v.user_id, v.content_id, v.click_cnt, v.if_ad, u.bizlicense as product_biz
from(
	select user_id, content_id, count(*) as click_cnt,
	case when ref_source is null then 0
		else 1
	end as if_ad
	from bun_log_db.app_event_type_view
	where year||month||day = '20200510'
	group by user_id, content_id, ref_source
order by user_id, content_id) v
join service1_quicket.product_info p on v.content_id = p.id
join service1_quicket.user_ u on p.uid = u.id;

select interest
from service1_quicket.product_ext
where pid = '124119291';

select *
from bun_log_db.app_event_type_view
where year||month||day = '20200510'
limit 100;

-- 상품 노출 캡
select i.content_id, i.product_imp_cnt, u.bizlicense as product_biz
from (
select content_id, count(*) as product_imp_cnt
from bun_log_db.app_event_type_impression
where year||month||day = '20200510' and ref_source is null
	and ref_term in (
			select ref_term
			from bun_log_db.app_event_type_impression
			where year||month||day = '20200510' and ref_source is null and ref_term != '' and ref_term is not null
			group by ref_term
			order by count(*) desc
			limit 200
	)
group by content_id
) i
join service1_quicket.product_info p on p.id = i.content_id
join service1_quicket.user_ u on p.uid = u.id;

select ref_term, count(*)
from bun_log_app.app_event_type_impression
where year||month||day >= '20200401' and year||month||day <='20200414' 
    and ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up') 
    and ref_term is not null and ref_term != ''
group by ref_term
order by count(*) desc
limit 200;

select i.content_id, i.product_imp_cnt, u.bizlicense as product_biz
from (
select content_id, count(*) as product_imp_cnt
from bun_log_db.app_event_type_impression
where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up')
group by content_id
) i
join service1_quicket.product_info p on p.id = i.content_id
join service1_quicket.user_ u on p.uid = u.id;

-- 샐러 총 노출 캡
select p.uid as user_id, count(*) as seller_imp_cnt
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on i.content_id = p.id
where i.year||i.month||i.day = '20200510' and i.ref_source is null
group by p.uid;

select i.user_id, i.seller_imp_cnt, i.content_cnt, i.avg_imp_cnt, u.bizlicense as biz
from
(
select p.uid as user_id, count(*) as seller_imp_cnt, count(distinct i.content_id) as content_cnt, seller_imp_cnt/content_cnt as avg_imp_cnt
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on i.content_id = p.id
where i.year||i.month||i.day = '20200510' and i.ref_source is null
group by p.uid
) i
join service1_quicket.user_ u on i.user_id = u.id;


-- 뷰어 노출 캡
select imp_cnt_for_viewer, count(*) as freq
from(
select user_id, content_id, count(*) as imp_cnt_for_viewer
from bun_log_db.app_event_type_impression
where year||month||day = '20200510' and ref_source is null
group by user_id, content_id
)
group by imp_cnt_for_viewer
;

-- ad_view
select ref_term, count(*)
from bun_log_db.app_event_type_view
where year||month||day >= '20200401' and year||month||day <= '20200414'
	and ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up')
	and ref_term is not null and ref_term != ''
group by ref_term
order by count(*) desc
limit 200;

-- 노출 기준 광고 top 200 키워드 대표 카테고리
select t1.ref_term, t1.category
from (
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_impression
				where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up')
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
) t1
join (select ref_term, count(*)
		from bun_log_db.app_event_type_impression
		where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
		group by ref_term
		order by count(*) desc
		limit 200) t2 on t1.ref_term = t2.ref_term
order by t2.count desc
;
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_impression
				where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up')
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1;

-- 노출 기준 일반 top 200 키워드 대표 카테고리
select t1.ref_term, t1.category
from (
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_impression
				where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_campaign is null
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
) t1
join (select ref_term, count(*)
		from bun_log_db.app_event_type_impression
		where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
		group by ref_term
		order by count(*) desc
		limit 200) t2 on t1.ref_term = t2.ref_term
order by t2.count desc
;
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_impression i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_impression
				where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_campaign is null
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
;

-- 클릭 기준 광고 top 200 키워드 대표 카테고리
select t1.ref_term, t1.category
from (
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_view i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_view
				where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up')
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
) t1
join (select ref_term, count(*)
		from bun_log_db.app_event_type_view
		where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
		group by ref_term
		order by count(*) desc
		limit 200) t2 on t1.ref_term = t2.ref_term
order by t2.count desc
;
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_view i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_view
				where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up') and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_source in ('ad_super_up_shop', 'ad_super_up', 'power_up')
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1;

-- 노출 기준 일반 top 200 키워드 대표 카테고리
select t1.ref_term, t1.category
from (
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_view i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_view
				where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_campaign is null
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
) t1
join (select ref_term, count(*)
		from bun_log_db.app_event_type_view
		where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
		group by ref_term
		order by count(*) desc
		limit 200) t2 on t1.ref_term = t2.ref_term
order by t2.count desc
;
select ref_term, category
from(
select i.ref_term, left(category_id, 3) as category, count(*) as cnt, row_number() over(partition by i.ref_term order by count(*) desc) as row_num
from bun_log_db.app_event_type_view i
join service1_quicket.product_info p on p.id = i.content_id
where i.ref_term in 
				(
				select ref_term
				from bun_log_db.app_event_type_view
				where year||month||day = '20200510' and ref_campaign is null and ref_term is not null and ref_term != ''
				group by ref_term
				order by count(*) desc
				limit 200
				)
	and i.ref_campaign is null
group by i.ref_term, left(p.category_id, 3)
)
where row_num = 1
;

================
SELECT
  yourtable.*
FROM
  yourtable INNER JOIN (
    SELECT
      id,
      GROUP_CONCAT(year ORDER BY rate DESC) grouped_year
    FROM
      yourtable
    GROUP BY id) group_max
  ON yourtable.id = group_max.id
     AND FIND_IN_SET(year, grouped_year) BETWEEN 1 AND 5
ORDER BY
  yourtable.id, yourtable.year DESC;
==============  

-- 키워드 ctr
SELECT
 c.content_id,
 c.imp_cnt,
 c.view_cnt,
 p.uid,
 Cast(c.view_cnt AS FLOAT) / Cast(c.imp_cnt AS FLOAT) AS ctr
FROM
 (
    SELECT
       imp_content_id AS content_id,
       Count(*) AS imp_cnt,
       COALESCE(Count(view_content_id), 0) AS view_cnt 
    FROM
       (
          SELECT
             i.content_id AS imp_content_id,
             v.content_id AS view_content_id
          FROM
             bun_log_db.app_event_type_impression i 
             LEFT OUTER JOIN
                bun_log_db.app_event_type_view v 
                ON v.imp_id = i.imp_id 
          WHERE
             i.year || i.month || i.day >= '20200401' 
             AND i.year || i.month || i.day <= '20200430' 
             AND i.ref_source IN 
             (
                'ad_super_up',
                'ad_super_up_shop',
                'power_up' 
             )
       )
       l 
    GROUP BY
       imp_content_id
);


-- 노출 기준 광고 top 200 키워드 ctr
SELECT ref_term,
       count(*) AS imp_cnt,
       coalesce(count(view_content_id), 0) AS view_cnt,
       cast(view_cnt AS float)/cast(imp_cnt AS float) AS ctr
FROM
  (SELECT i.content_id AS imp_content_id,
          v.content_id AS view_content_id,
          i.ref_term AS ref_term
   FROM bun_log_db.app_event_type_impression i
   LEFT OUTER JOIN bun_log_db.app_event_type_view v ON v.imp_id = i.imp_id
   WHERE i.year||i.month||i.day = '20200510'
     AND i.ref_source IN ('ad_super_up',
                          'ad_super_up_shop',
                          'power_up')
     AND i.ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_impression
        WHERE YEAR||MONTH||DAY = '20200510'
          AND ref_source IN ('ad_super_up',
                             'ad_super_up_shop',
                             'power_up')
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 200) )
GROUP BY ref_term;

-- 노출 기준 일반 top 200 키워드 ctr
SELECT ref_term,
       count(*) AS imp_cnt,
       coalesce(count(view_content_id), 0) AS view_cnt,
       cast(view_cnt AS float)/cast(imp_cnt AS float) AS ctr
FROM
  (SELECT i.content_id AS imp_content_id,
          v.content_id AS view_content_id,
          i.ref_term AS ref_term
   FROM bun_log_db.app_event_type_impression i
   LEFT OUTER JOIN bun_log_db.app_event_type_view v ON v.imp_id = i.imp_id
   WHERE i.year||i.month||i.day = '20200510'
     AND i.ref_campaign IS NULL
     AND i.ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_impression
        WHERE YEAR||MONTH||DAY = '20200510'
          AND ref_campaign IS NULL
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 200) )
GROUP BY ref_term
;
-- 검색 기준 광고 top 200 키워드 ctr
SELECT ref_term,
       count(*) AS imp_cnt,
       coalesce(count(view_content_id), 0) AS view_cnt,
       cast(view_cnt AS float)/cast(imp_cnt AS float) AS ctr
FROM
  (SELECT i.content_id AS imp_content_id,
          v.content_id AS view_content_id,
          i.ref_term AS ref_term
   FROM bun_log_db.app_event_type_impression i
   LEFT OUTER JOIN bun_log_db.app_event_type_view v ON v.imp_id = i.imp_id
   WHERE i.year||i.month||i.day = '20200510'
     AND i.ref_source IN ('ad_super_up',
                          'ad_super_up_shop',
                          'power_up')
     AND i.ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_view
        WHERE YEAR||MONTH||DAY = '20200510'
          AND ref_source IN ('ad_super_up',
                             'ad_super_up_shop',
                             'power_up')
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 200) )
GROUP BY ref_term;

-- 검색 기준 일반 top 200 키워드 ctr
SELECT ref_term,
       count(*) AS imp_cnt,
       coalesce(count(view_content_id), 0) AS view_cnt,
       cast(view_cnt AS float)/cast(imp_cnt AS float) AS ctr
FROM
  (SELECT i.content_id AS imp_content_id,
          v.content_id AS view_content_id,
          i.ref_term AS ref_term
   FROM bun_log_db.app_event_type_impression i
   LEFT OUTER JOIN bun_log_db.app_event_type_view v ON v.imp_id = i.imp_id
   WHERE i.year||i.month||i.day = '20200510'
     AND i.ref_campaign IS NULL
     AND i.ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_search
        WHERE YEAR||MONTH||DAY = '20200510'
          AND ref_campaign IS NULL
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 200) )
GROUP BY ref_term
;



select i.
from (
	select user_id, content_id, content_owner, count(*) as imp_cnt_for_viewer
	from bun_log_db.app_event_type_impression
	where year||month||day = '20200510' and ref_source is null
	group by user_id, content_id, content_owner
) i
join service1_quicket.user_ u on i.content_owner = u.id;

select *
from warehouse.vw_ad_view
where uid = '192522'
limit 10;

select content_id, count(*) as product_imp_cnt
from bun_log_db.app_event_type_impression i
join workspace.vw_ad_view a i.
where year||month||day = '20200510' and ref_source in ('ad_super_up', 'ad_super_up_shop', 'power_up')
group by content_id;

--0: 판매중  / 1: 예약중 / 2: 삭제 / 3: 판매완료
select p.uid, to_char(p.create_date, 'YYYY/MM/dd') as daily, count(*)
from service1_quicket.product_info p
join service1_quicket.user_ u on u.id = p.uid
where p.create_date between '2020-04-01' and '2020-04-30' and u.bizlicense = 0 and p.status = 0
group by p.uid, daily;



--to_char(dateadd(day, -datepart(dw, getdate()), dateadd(week, datediff(week, getdate(), ad_date), getdate())), 'YYYY-MM-dd')

select p.uid, count(*)
from service1_quicket.product_info p
join service1_quicket.user_ u on u.id = p.uid
where p.create_date between '2020-04-01' and '2020-04-30' and u.bizlicense = 1
group by p.uid;

select i.content_owner, avg(pup.pay_point) as pup_point, avg(sup.pay_point) as sup_point, avg(susp.pay_point) as susp_point, avg(pup.pay_point)+avg(sup.pay_point)+avg(susp.pay_point) as sum_point, count(*) as imp_cnt
from bun_log_db.app_event_type_impression i 
left join service1_quicket.ad_power_up_point pup on i.ref_campaign = pup.pu_id
left join service1_quicket.ad_super_up_point sup on i.ref_campaign = sup.suid
left join service1_quicket.ad_super_up_shop_point susp on i.ref_campaign = susp.sus_id
where i.year||i.month||i.day = '20200510' and i.ref_source in ('power_up','ad_super_up','ad_super_up_shop')
	and (pup.ad_date = '2020-05-10' or (to_char(sup.start_at, 'YYYY-MM-dd') <= '2020-05-10' and to_char(sup.end_at, 'YYYY-MM-dd') >= '2020-05-10') or susp.ad_date = '2020-05-10')
	and i.content_id is not null
group by i.content_id;



select i.content_owner, count(*) as imp_cnt
from bun_log_db.app_event_type_impression i
join service1_quicket.user_ u on i.content_owner = u.id
where i.year||i.month||i.day = '20200510' and u.bizlicense = 0 and i.ref_campaign is null and i.ref_campaign != ''
group by i.content_owner;

SELECT oneday.all_cnt,
       na.non_ad_cnt,
       cast(na.non_ad_cnt AS float)/cast(oneday.all_cnt AS float) AS non_ad_prop,
       a.ad_cnt,
       cast(a.ad_cnt AS float)/cast(oneday.all_cnt AS float) AS ad_prop
FROM
  (SELECT count(*) AS all_cnt
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20200518'
     AND content_id IS NOT NULL ) oneday,

  (SELECT count(*) AS non_ad_cnt
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20200518'
     AND ref_campaign IS NULL
     AND content_id IS NOT NULL
     AND ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_impression
        WHERE YEAR||MONTH||DAY = '20200518'
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 1000) ) na,

  (SELECT count(*) AS ad_cnt
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20200518'
     AND ref_campaign IS NOT NULL
     AND content_id IS NOT NULL
     AND ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_impression
        WHERE YEAR||MONTH||DAY = '20200518'
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 1000) ) a;


select cast(na_term.ref_term_non_ad_cnt as float)/cast(na.non_ad_cnt as float)
from 
  (SELECT count(*) AS ref_term_non_ad_cnt
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY = '20200518'
     AND ref_campaign IS NULL
     AND content_id IS NOT NULL
     AND ref_term IN
       (SELECT ref_term
        FROM bun_log_db.app_event_type_view
        WHERE YEAR||MONTH||DAY = '20200518' and ref_campaign is NULL and content_id is not null
        GROUP BY ref_term
        ORDER BY count(*) DESC
        LIMIT 1000) ) na_term,
   (select count(*) as non_ad_cnt
   from bun_log_db.app_event_type_view
   where year||month||day = '20200518'
   	and ref_campaign is null
   	and content_id is not null
   ) na;

select cast(a_term.ref_term_ad_cnt as float)/cast(a.ad_cnt as float)
from 
  (SELECT count(*) AS ref_term_ad_cnt
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY = '20200518'
     AND ref_source in ('power_up', 'ad_super_up', 'ad_super_up_shop')
     AND content_id IS NOT NULL
     AND ref_term IN
       (SELECT search_term
        FROM bun_log_db.app_event_type_search
        WHERE YEAR||MONTH||DAY = '20200518'
        GROUP BY search_term
        ORDER BY count(*) DESC
        LIMIT 1000) ) a_term,
   (select count(*) as ad_cnt
   from bun_log_db.app_event_type_view
   where year||month||day = '20200518'
   	and ref_source in ('power_up', 'ad_super_up', 'ad_super_up_shop')
   	and content_id is not null
   ) a;

-- user의 join_date 가 1월인 유저들 대상, 
select p.uid, to_char(p.create_date, 'YYYY-MM') as month, to_char(p.create_date, 'YYYY-MM-dd') as day, count(*)
from service1_quicket.product_info p
join service1_quicket.user_ u on p.uid = u.id
where u.join_date between '2020-01-01' and '2020-01-31' 
	and u.bizlicense = 0
	and p.create_date between '2020-02-01' and '2020-04-30' 
	and p.uid is not null 
	and p.id is not null
group by p.uid, month, day;



---------------------------------------
select pid, user_name, starttime, query
from stv_recents
where status='Running';
---------------------------------------
select *
from bun_log_db.ca_event_type_user_login_log
order by event_time asc
limit 10;

SELECT count(DISTINCT UID)
FROM
  (SELECT DISTINCT l.user_id AS UID
   FROM bun_log_db.app_event_type_login l
   JOIN service1_quicket.user_ q on l.user_id = q.id
   FULL JOIN service1_quicket.user_identification u ON l.user_id = u.uid
   WHERE l.year||l.month||l.day >= '20200428'
     AND ((u.updated_at >= l.event_time and u.cert_type in ('M','P'))
          OR u.uid IS NULL)
     AND l.user_id IS NOT NULL and q.status =0
   UNION SELECT DISTINCT i.viewer_uid AS UID
   FROM bun_log_db.ca_event_type_item_click_v5 i
   JOIN service1_quicket.user_ q on i.viewer_uid = q.id
   FULL JOIN service1_quicket.user_identification u ON i.viewer_uid = u.uid
   WHERE i.year||i.month||i.day >= '20200428'
     AND ((u.updated_at >= i.event_time and u.cert_type in ('M','P'))
          OR u.uid IS NULL)
     AND i.viewer_uid IS NOT NULL and q.status =0
   UNION SELECT DISTINCT l.viewer_uid AS UID
   FROM bun_log_db.ca_event_type_item_search_log l
   join service1_quicket.user_ q on l.viewer_uid = q.id
   FULL JOIN service1_quicket.user_identification u ON l.viewer_uid = u.uid
   WHERE l.year||l.month||l.day >= '20200428'
     AND ((u.updated_at >= l.event_time and u.cert_type in ('M','P'))
          OR u.uid IS NULL)
     AND l.viewer_uid IS NOT NULL and q.status =0
   UNION SELECT DISTINCT l.viewer_uid AS UID
   FROM bun_log_db.ca_event_type_shop_click l
   join service1_quicket.user_ q on l.viewer_uid = q.id
   FULL JOIN service1_quicket.user_identification u ON l.viewer_uid = u.uid
   WHERE l.year||l.month||l.day >= '20200428'
     AND ((u.updated_at >= l.event_time and u.cert_type in ('M','P'))
          OR u.uid IS NULL)
     AND l.viewer_uid IS NOT NULL and q.status =0) ;

SELECT daily AS "날짜",
       ad_type AS "광고 종류",
       keyword AS "신청 키워드",
       count(*) AS count
FROM
  (SELECT pid,
          split_part(keyword, ',', 1) AS keyword,
          ad_type,
          daily
   FROM
     (SELECT pid,
             ad_type,
             daily,
             p.keyword
      FROM
        (SELECT target_id AS pid,
                ad_type,
                to_char(created_at, 'YYYY/MM/dd') AS daily,
                count(*)
         FROM warehouse.vw_ad_view vw
         WHERE created_at > dateadd(MONTH, -2, getdate())
           AND ad_type IN ('슈퍼UP',
                           '상점UP',
                           '파워UP',
                           '홈추천PLUS',
                           '홈추천VIP')
         GROUP BY pid,
                  ad_type,
                  daily) t1
      INNER JOIN service1_quicket.product_info p ON t1.pid = p.id) t2
   WHERE keyword IS NOT NULL
     AND keyword != ''
   UNION ALL SELECT pid,
                    split_part(keyword, ',', 2) AS keyword,
                    ad_type,
                    daily
   FROM
     (SELECT pid,
             ad_type,
             daily,
             p.keyword
      FROM
        (SELECT target_id AS pid,
                ad_type,
                to_char(created_at, 'YYYY/MM/dd') AS daily,
                count(*)
         FROM warehouse.vw_ad_view vw
         WHERE created_at > dateadd(MONTH, -2, getdate())
           AND ad_type IN ('슈퍼UP',
                           '상점UP',
                           '파워UP',
                           '홈추천PLUS',
                           '홈추천VIP')
         GROUP BY pid,
                  ad_type,
                  daily) t1
      INNER JOIN service1_quicket.product_info p ON t1.pid = p.id) t2
   WHERE keyword IS NOT NULL
     AND keyword != ''
   UNION ALL SELECT pid,
                    split_part(keyword, ',', 3) AS keyword,
                    ad_type,
                    daily
   FROM
     (SELECT pid,
             ad_type,
             daily,
             p.keyword
      FROM
        (SELECT target_id AS pid,
                ad_type,
                to_char(created_at, 'YYYY/MM/dd') AS daily,
                count(*)
         FROM warehouse.vw_ad_view vw
         WHERE created_at > dateadd(MONTH, -2, getdate())
           AND ad_type IN ('슈퍼UP',
                           '상점UP',
                           '파워UP',
                           '홈추천PLUS',
                           '홈추천VIP')
         GROUP BY pid,
                  ad_type,
                  daily) t1
      INNER JOIN service1_quicket.product_info p ON t1.pid = p.id) t2
   WHERE keyword IS NOT NULL
     AND keyword != ''
   UNION ALL SELECT pid,
                    split_part(keyword, ',', 4) AS keyword,
                    ad_type,
                    daily
   FROM
     (SELECT pid,
             ad_type,
             daily,
             p.keyword
      FROM
        (SELECT target_id AS pid,
                ad_type,
                to_char(created_at, 'YYYY/MM/dd') AS daily,
                count(*)
         FROM warehouse.vw_ad_view vw
         WHERE created_at > dateadd(MONTH, -2, getdate())
           AND ad_type IN ('슈퍼UP',
                           '상점UP',
                           '파워UP',
                           '홈추천PLUS',
                           '홈추천VIP')
         GROUP BY pid,
                  ad_type,
                  daily) t1
      INNER JOIN service1_quicket.product_info p ON t1.pid = p.id) t2
   WHERE keyword IS NOT NULL
     AND keyword != ''
   UNION ALL SELECT pid,
                    split_part(keyword, ',', 5) AS keyword,
                    ad_type,
                    daily
   FROM
     (SELECT pid,
             ad_type,
             daily,
             p.keyword
      FROM
        (SELECT target_id AS pid,
                ad_type,
                to_char(created_at, 'YYYY/MM/dd') AS daily,
                count(*)
         FROM warehouse.vw_ad_view vw
         WHERE created_at > dateadd(MONTH, -2, getdate())
           AND ad_type IN ('슈퍼UP',
                           '상점UP',
                           '파워UP',
                           '홈추천PLUS',
                           '홈추천VIP')
         GROUP BY pid,
                  ad_type,
                  daily) t1
      INNER JOIN service1_quicket.product_info p ON t1.pid = p.id) t2
   WHERE keyword IS NOT NULL
     AND keyword != '') t3
WHERE t3.keyword != ''
  AND t3.keyword IS NOT NULL
GROUP BY t3.daily,
         t3.ad_type,
         t3.keyword
HAVING COUNT > 1
ORDER BY daily DESC,
         ad_type DESC,
         COUNT DESC;

select distinct event_type
from warehouse.ca_active_user
where event_time >= '2020-05-31 00:00:00';


select avg(u.review_count), count(distinct uid) 
from service1_quicket.user_ u
join service1_quicket.user_identification i on u.id = i.uid
join servi
where i.cert_type in ('M', 'P') and u.status = 0;




WITH revenue AS (
    SELECT updated_at, pid, total_price, type
    FROM workspace.redash_gmv_revenue_bunp
    WHERE updated_at >= '2020-01-01' AND updated_at < '2020-07-01'
    UNION
    SELECT updated_at, pid, total_price, type
    FROM workspace.redash_gmv_revenue_bunpay
    WHERE updated_at >= '2020-01-01' AND updated_at < '2020-07-01'
)
select paid.date, 
	paid.avg_paid_point, paid.median_paid_point,
	free.avg_free_point, free.median_free_point
from 
(SELECT date, 
	avg(paid_point) as avg_paid_point, 
	median(paid_point) as median_paid_point
FROM (
         SELECT date_trunc('month', updated_at) AS date,
                b.uid as uid,
                sum(a.total_price) as total_price,
                sum(c.paid_point) as paid_point
         FROM revenue a
              JOIN service1_quicket.product_info b ON a.pid = b.id
              JOIN warehouse.vw_ad_view c ON b.uid = c.uid
         WHERE c.status < 3
         GROUP BY 1, 2
         HAVING sum(a.total_price) >= 1000000
     )
GROUP BY 1) as paid
join 
(SELECT date, 
	avg(free_point) as avg_free_point, 
	median(free_point) as median_free_point
FROM (
         SELECT date_trunc('month', updated_at) AS date,
                b.uid as uid,
                sum(a.total_price) as total_price,
                sum(c.free_point) as free_point
         FROM revenue a
              JOIN service1_quicket.product_info b ON a.pid = b.id
              JOIN warehouse.vw_ad_view c ON b.uid = c.uid
         WHERE c.status < 3
         GROUP BY 1, 2
         HAVING sum(a.total_price) >= 1000000
     )
GROUP BY 1) as free on paid.date = free.date
order by 1 asc
;
     
WITH revenue AS (
    SELECT updated_at, pid, total_price, type
    FROM workspace.redash_gmv_revenue_bunp
    WHERE updated_at >= '2020-01-01' AND updated_at < '2020-07-01'
    UNION
    SELECT updated_at, pid, total_price, type
    FROM workspace.redash_gmv_revenue_bunpay
    WHERE updated_at >= '2020-01-01' AND updated_at < '2020-07-01'
)
SELECT t.date as date, c.ad_type as ad_type, count(*) as count
FROM (
         SELECT date_trunc('month', updated_at) AS date,
                b.uid as uid,
                sum(a.total_price) as total_price
         FROM revenue a
              JOIN service1_quicket.product_info b ON a.pid = b.id
         GROUP BY 1, 2
         HAVING sum(a.total_price) >= 1000000
     ) t 
JOIN warehouse.vw_ad_view c ON t.uid = c.uid
WHERE c.status < 3
GROUP BY 1, 2
ORDER BY 1 asc;


select
    pi.name as name,
    coalesce(pi.category_id, '0') as category_id,
    coalesce(pi.price_gap, 0) as price_gap,
    pd.description as description,
    case
        when lb.id is not null then 1
        when pi.status=2 and datediff(hours,pi.create_date,pi.modified_at) <= 1 then 1
        else  0
    end as label
from
    (select
        id,
        status,
        create_date,
        modified_at,
        name,
        category_id,
        price - median(price) over (partition by category_id) as price_gap
    from service1_quicket.product_info
    where create_date >= '2020-04-01 00:00:00'
    and create_date < '2020-05-01 00:00:00') as pi
    left join
    service1_quicket.product_description as pd
    on pi.id = pd.pid
    left join
    workspace.sang_tmp_fraud_product_200706 as lb
    on pi.id = lb.pid;


select
    pi.name as name,
    coalesce(pi.category_id, '0') as category_id,
    coalesce(pi.price_gap, 0) as price_gap,
    u.uid as uid,
    (getdate() - to_date(u.birthdate, 'YYYY-mm-dd'))/365 as age,
    case 
    	when age/10 = 0 then '0'
    	when age/10 = 1 then '10'
    	when age/10 = 2 then '20'
    	when age/10 = 3 then '30'
    	when age/10 = 4 then '40'
    	when age/10 = 5 then '50'
    	else 'none'
    end as age_scope,
    pd.description as description,
    case
        when lb.id is not null then 1
        when pi.status=2 and datediff(hours,pi.create_date,pi.modified_at) <= 1 then 1
        else  0
    end as label
from
    (select
        id,
        uid,
        status,
        create_date,
        modified_at,
        name,
        category_id,
        price - median(price) over (partition by category_id) as price_gap
    from service1_quicket.product_info
    where create_date >= '2020-04-01 00:00:00'
    and create_date < '2020-05-01 00:00:00') as pi
    left join
    service1_quicket.product_description as pd
    on pi.id = pd.pid
    left join
    workspace.sang_tmp_fraud_product_200706 as lb
    on pi.id = lb.pid
	join
	service1_quicket.user_extra_info u on pi.uid = u.uid;

select extract(YEAR_MONTH from created_at)
from warehouse.vw_ad_view
where created_at >= dateadd(day, -1, getdate())
limit 10;

select *
from bun_log_db.app_event_type_view
where year||month||day||hour = '2020080108'
limit 5;	
	
SELECT i.content_id,
       i.content_position,
       i.page_id,
       i.ref_source,
       i.ref_term,
       i.user_id,
       i.content_owner,
       u.sex,
       Coalesce(NULL, Ceil((Extract(DAY
                                    FROM Getdate() - u.birthdate :: timestamptz) / 365.25)), 0) AS age_detail,
       CASE
           WHEN age_detail < 20 THEN 10
           WHEN age_detail >= 20
                AND age_detail < 30 THEN 20
           WHEN age_detail >= 30
                AND age_detail < 40 THEN 30
           ELSE 40
       END AS age,
       left(p.category_id, 3) as category_id,
       CASE
           WHEN c.content_id IS NULL THEN 0
           ELSE 1
       END AS label
FROM bun_log_db.app_event_type_impression i
LEFT JOIN bun_log_db.app_event_type_view c ON i.imp_id = c.imp_id
JOIN service1_quicket.user_extra_info u ON i.user_id = u.uid
JOIN service1_quicket.product_info p on i.content_id = p.id
WHERE i.year||i.month||i.day||i.hour >= '2020080118'
  AND i.year||i.month||i.day||i.hour < '2020080120'
  AND To_date(u.birthdate, 'YYYY-MM-DD') > '1900-01-01'
  AND u.birthdate NOT LIKE '%02-29%'
  AND u.birthdate NOT LIKE '%02-30%'
  AND u.birthdate NOT LIKE '%02-31%';
  
  
select id
from service1_quicket.user_
where bizlicense = 1 and status = 0;


select * from bun_log_db.ca_event_type_searched_pids_v2;



SELECT i.ref_term,
       t.imp_cnt,
       i.device_type,
       CASE
           WHEN u.sex = 1 THEN 'female'
           WHEN u.sex = 2 THEN 'male'
           ELSE 'none'
       END AS sex,
       Coalesce(NULL, Ceil((Extract(DAY
                                    FROM Getdate() - u.birthdate :: timestamptz) / 365.25)), 0) AS age_detail,
       CASE
           WHEN age_detail < 20 THEN 10
           WHEN age_detail >= 20
                AND age_detail < 30 THEN 20
           WHEN age_detail >= 30
                AND age_detail < 40 THEN 30
           ELSE 40
       END AS age
FROM bun_log_db.app_event_type_impression i
JOIN
  (SELECT ref_term,
          count(*) AS imp_cnt
   FROM bun_log_db.app_event_type_impression
   WHERE ref_term IS NOT NULL
     AND device_type IN ('a',
                         'i')
     AND YEAR||MONTH||DAY = '20200810'
   GROUP BY 1
   ORDER BY count(*) DESC
   LIMIT 1000) t ON i.ref_term = t.ref_term
JOIN service1_quicket.user_extra_info u ON i.user_id = u.uid
WHERE i.year||i.month||i.day = '20200810'
  AND i.device_type IN ('a',
                        'i')
  AND u.is_identification = 1
  AND To_date(u.birthdate, 'YYYY-MM-DD') between '1930-01-01' and '2010-12-31';


drop table if exists workspace.emily_ctr_df;

create table if not exists workspace.emily_ctr_df
(
    label integer,
    day varchar(8),
    hour varchar(2),
    imp_content_id varchar,
    imp_content_position varchar,
    imp_page_id varchar,
    imp_ref_page_id varchar,
    imp_ref_term varchar,
    imp_ref_source varchar,
    imp_user_id varchar,
    imp_content_owner varchar,
    user_sex varchar,
    user_age integer,
    user_following_cnt integer,
    user_bunpay_count integer,
    owner_grade integer,
    owner_item_count integer,
    owner_interest integer,
    owner_follower_cnt integer,
    owner_bunpay_count integer,
    content_price integer,
    content_category_id varchar,
    content_emergency_cnt integer,
    content_comment_cnt integer,
    content_interest integer,
    content_pfavcnt integer
)
;

insert into workspace.emily_ctr_df
    (
    label,
    day,
    hour,
    imp_content_id,
    imp_content_position,
    imp_page_id,
    imp_ref_page_id,
    imp_ref_term,
    imp_ref_source,
    imp_user_id,
    imp_content_owner,
    user_sex,
    user_age,
    user_following_cnt,
    user_bunpay_count,
    owner_grade,
    owner_item_count,
    owner_interest,
    owner_follower_cnt,
    owner_bunpay_count,
    content_price,
    content_category_id,
    content_emergency_cnt,
    content_comment_cnt,
    content_interest,
    content_pfavcnt
    )
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       '20200601' AS DAY,
       i.hour AS hour,
       i.content_id AS imp_content_id,
       i.content_position AS imp_content_position,
       i.page_id AS imp_page_id,
       i.ref_page_id AS imp_ref_page_id,
       i.ref_term AS imp_ref_term,
       i.ref_source AS imp_ref_source,
       i.user_id AS imp_user_id,
       p.uid AS imp_content_owner,
       v.sex AS user_sex,
       v.age_detail AS user_age,
       v.following_cnt AS user_following_cnt,
       v.bunpay_count AS user_bunpay_count,
       s.grade AS owner_grade,
       s.item_count AS owner_item_count,
       s.interest AS owner_interest,
       s.follower_cnt AS owner_follower_cnt,
       s.bunpay_count AS owner_bunpay_count,
       p.price AS content_price,
       p.category_id AS content_category_id,
       p.emergency_cnt AS content_emergency_cnt,
       p.comment_cnt AS content_comment_cnt,
       p.interest AS content_interest,
       p.pfavcnt AS content_pfavcnt
FROM
  (SELECT imp_id,
          content_id,
          content_position,
          page_id,
          ref_page_id,
          ref_term,
          ref_source,
          user_id,
          event_time,
          YEAR,
          MONTH,
          DAY,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY||hour BETWEEN '2020080100' AND '2020080103'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN
  (SELECT imp_id,
          event_time
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY||hour BETWEEN '2020080100' AND '2020080103'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN
  (SELECT u.id,
          ue.sex,
          Coalesce(NULL, Ceil((Extract(DAY
                                       FROM Getdate() - ue.birthdate :: timestamptz) / 365.25)), 0) AS age_detail,
          ue.following_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1
     AND ue.birthdate BETWEEN '1930-01-01' AND '2010-12-31'
     AND ue.birthdate NOT LIKE '%02-29'
     AND ue.birthdate NOT LIKE '%02-30') AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          left(p.category_id, 6) AS category_id,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT u.id,
          u.grade,
          u.item_count,
          u.interest,
          ue.follower_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS s ON p.uid = s.id
WHERE v.id != s.id;

drop table if exists workspace.emily_pclick_df;

create table if not exists workspace.emily_pclick_df
(
    label integer,
    day varchar(8),
    hour varchar(2),
    imp_content_id varchar,
    imp_content_position varchar,
    imp_page_id varchar,
    imp_ref_page_id varchar,
    imp_ref_term varchar,
    imp_ref_source varchar,
    imp_user_id varchar,
    imp_content_owner varchar,
    user_sex varchar,
    user_age integer,
    user_following_cnt integer,
    user_bunpay_count integer,
    owner_grade integer,
    owner_item_count integer,
    owner_interest integer,
    owner_follower_cnt integer,
    owner_bunpay_count integer,
    content_price integer,
    content_category_id varchar,
    content_emergency_cnt integer,
    content_comment_cnt integer,
    content_interest integer,
    content_pfavcnt integer
)
;

insert into workspace.emily_pclick_df
    (
    label,
    day,
    hour,
    imp_content_id,
    imp_content_position,
    imp_page_id,
    imp_ref_page_id,
    imp_ref_term,
    imp_ref_source,
    imp_user_id,
    imp_content_owner,
    user_sex,
    user_age,
    user_following_cnt,
    user_bunpay_count,
    owner_grade,
    owner_item_count,
    owner_interest,
    owner_follower_cnt,
    owner_bunpay_count,
    content_price,
    content_category_id,
    content_emergency_cnt,
    content_comment_cnt,
    content_interest,
    content_pfavcnt
    )
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       '20200601' AS DAY,
       i.hour AS hour,
       i.content_id AS imp_content_id,
       i.content_position AS imp_content_position,
       i.page_id AS imp_page_id,
       i.ref_page_id AS imp_ref_page_id,
       i.ref_term AS imp_ref_term,
       i.ref_source AS imp_ref_source,
       i.user_id AS imp_user_id,
       p.uid AS imp_content_owner,
       v.sex AS user_sex,
       v.age_detail AS user_age,
       v.following_cnt AS user_following_cnt,
       v.bunpay_count AS user_bunpay_count,
       s.grade AS owner_grade,
       s.item_count AS owner_item_count,
       s.interest AS owner_interest,
       s.follower_cnt AS owner_follower_cnt,
       s.bunpay_count AS owner_bunpay_count,
       p.price AS content_price,
       p.category_id AS content_category_id,
       p.emergency_cnt AS content_emergency_cnt,
       p.comment_cnt AS content_comment_cnt,
       p.interest AS content_interest,
       p.pfavcnt AS content_pfavcnt
FROM
  (SELECT imp_id,
          content_id,
          content_position,
          page_id,
          ref_page_id,
          ref_term,
          ref_source,
          user_id,
          event_time,
          YEAR,
          MONTH,
          DAY,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY||hour >= '2020080118' AND YEAR||MONTH||DAY||hour <= '2020080120'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN
  (SELECT imp_id,
          event_time
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY||hour >= '2020080118' AND YEAR||MONTH||DAY||hour <= '2020080120'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN
  (SELECT u.id,
          ue.sex,
          Coalesce(NULL, Ceil((Extract(DAY
                                       FROM Getdate() - ue.birthdate :: timestamptz) / 365.25)), 0) AS age_detail,
          ue.following_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1
     AND ue.birthdate BETWEEN '1930-01-01' AND '2010-12-31'
     AND ue.birthdate NOT LIKE '%02-29'
     AND ue.birthdate NOT LIKE '%02-30') AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.name,
          p.uid,
          p.price,
          left(p.category_id, 6) AS category_id,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT u.id,
          u.grade,
          u.item_count,
          u.interest,
          ue.follower_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS s ON p.uid = s.id
WHERE v.id != s.id;

-------------------------------------------------------------------------------------------------
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       '20200820' AS DAY,
       i.hour AS hour,
       i.content_id AS imp_content_id,
       i.content_position AS imp_content_position,
       i.page_id AS imp_page_id,
       i.ref_page_id AS imp_ref_page_id,
       i.ref_term AS imp_ref_term,
       i.ref_source AS imp_ref_source,
       i.user_id AS imp_user_id,
       p.uid AS imp_content_owner,
       v.sex AS user_sex,
       v.age_detail AS user_age,
       v.following_cnt AS user_following_cnt,
       v.bunpay_count AS user_bunpay_count,
       s.grade AS owner_grade,
       s.item_count AS owner_item_count,
       s.interest AS owner_interest,
       s.follower_cnt AS owner_follower_cnt,
       s.bunpay_count AS owner_bunpay_count,
       p.price AS content_price,
       p.category_id AS content_category_id,
       p.emergency_cnt AS content_emergency_cnt,
       p.comment_cnt AS content_comment_cnt,
       p.interest AS content_interest,
       p.pfavcnt AS content_pfavcnt
FROM
  (SELECT imp_id,
          content_id,
          content_position,
          page_id,
          ref_page_id,
          ref_term,
          ref_source,
          user_id,
          event_time,
          YEAR,
          MONTH,
          DAY,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20200820'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN
  (SELECT imp_id,
          event_time
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY = '20200820'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN
  (SELECT u.id,
          ue.sex,
          Coalesce(NULL, Ceil((Extract(DAY
                                       FROM Getdate() - ue.birthdate :: timestamptz) / 365.25)), 0) AS age_detail,
          ue.following_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1
     AND ue.birthdate BETWEEN '1930-01-01' AND '2010-12-31'
     AND ue.birthdate NOT LIKE '%02-29'
     AND ue.birthdate NOT LIKE '%02-30') AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          left(p.category_id, 6) AS category_id,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT u.id,
          u.grade,
          u.item_count,
          u.interest,
          ue.follower_cnt,
          ue.bunpay_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS s ON p.uid = s.id
WHERE v.id != s.id;

select pid, user_name, starttime, query
from stv_recents
where status='Running';


select *
from service1_quicket.ad_power_up_point
where ad_date like '2020-08-10%'
limit 10;

select bid_keyword, count(*)
from bun_log_db.ca_event_type_item_click_v5
where bid_keyword is not null and bid_keyword != '' and ad_ref is not null and ad_ref != '' and year||month||day = '20200801'
group by 1
order by count(*) desc;

-------------------------------------------






