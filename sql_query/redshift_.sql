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
       i.
       _type,
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



DROP TABLE IF EXISTS workspace.emily_click_log;

CREATE TABLE workspace.emily_click_log AS
SELECT v.event_action, v.buid, v.content_id, v.content_position, v.content_type,
v.device_type, v.imp_id, v.session_id, v.user_id, v.server_time, v.content_owner, v.ref_term,
coalesce(extract('year'
                        FROM CURRENT_DATE) - u.birth_year,
                0) + 1 AS user_age,
       CASE
           WHEN user_age >= 10
                AND user_age < 20 THEN '10s'
           WHEN user_age >= 20
                AND user_age < 30 THEN '20s'
           WHEN user_age >= 30
                AND user_age < 40 THEN '30s'
           WHEN user_age >= 40
                AND user_age < 50 THEN '40s'
           WHEN user_age >= 50 THEN '50s'
       END AS user_age_group,
       u.gender AS user_gender,
       p.category_id,
       convert_timezone('KST', p.create_date) as content_created_at
from bun_log_db.app_event_type_view v
join service1_quicket.user_identification_v2 u on v.user_id = u.uid
join service1_quicket.product_info p on v.content_id = p.id
where v.year||v.month||v.day||v.hour between '2020091000' and '2020091010';
-----------------------



DROP TABLE IF EXISTS workspace.emily_ref_term_rank;


CREATE TABLE workspace.emily_ref_term_rank AS
SELECT replace(ref_term,
               ' ',
               '') AS ref_term,
       count(*), row_number() over(
                                   ORDER BY count(*) DESC) AS rank
FROM bun_log_db.app_event_type_impression
WHERE YEAR||MONTH||DAY = '20200928'
GROUP BY 1;

drop table if exists workspace.emily_pclick_df;

create table workspace.emily_pclick_df as 
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       i.hour AS hour,
       i.content_id AS imp_content_id,
       i.user_id AS imp_user_id,
       i.ref_term AS c_imp_ref_term,
       r.rank AS imp_ref_term_rank,
       i.ref_page_id AS c_imp_ref_page_id,
       c.event_time AS view_event_time,
       v.join_date_diff AS user_join_date_diff,
       v.gender AS c_user_gender,
       v.age AS c_user_age,
       v.following_cnt AS user_following_count,
       v.bunpay_count AS user_bunpay_count,
       v.parcel_post_count AS user_parcel_post_count,
       v.transfer_count AS user_transfer_count,
       v.bunp_meet_count AS user_bunp_meet_count,
       o.grade AS owner_grade,
       o.item_count AS owner_item_count,
       o.interest AS owner_interest_count,
       o.follower_cnt AS owner_follower_count,
       o.bunpay_count AS owner_bunpay_count,
       o.review_count AS owner_review_count,
       o.parcel_post_count AS owner_parcel_post_count,
       o.transfer_count AS owner_transfer_count,
       o.bunp_meet_count AS owner_bunp_meet_count,
       o.favorite_count AS owner_favorite_count,
       o.comment_count AS owner_comment_count,
       p.price AS content_price,
       p.category_id AS c_content_category_id,
       p.sub_category_id AS c_content_sub_category_id,
       p.second_sub_category_id AS c_content_second_sub_category_id,
       p.flag_used AS c_content_flag_used,
       p.create_date_diff AS content_create_date_diff,
       p.emergency_cnt AS content_emergency_count,
       p.comment_cnt AS content_comment_count,
       p.interest AS content_interest_count,
       p.pfavcnt AS content_favorite_count
FROM
  (SELECT imp_id,
          content_id,
          server_time,
          replace(ref_term, ' ', '') AS ref_term,
          ref_source,
          ref_page_id,
          user_id,
          event_time,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20200928'
     AND imp_id IS NOT NULL
     AND page_id = '검색결과'
     AND ref_term IS NOT NULL
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN workspace.emily_ref_term_rank r ON i.ref_term = r.ref_term
LEFT JOIN
  (SELECT imp_id,
          event_time
   FROM bun_log_db.app_event_type_view
   WHERE YEAR||MONTH||DAY = '20200928'
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN
  (SELECT u.id,
          DATEDIFF(YEAR, u.join_date, getdate()) AS join_date_diff,
          CASE ui.gender
              WHEN '1' THEN '1'
              WHEN '2' THEN '2'
              WHEN '0' THEN '2'
              ELSE '0'
          END AS gender,
          Coalesce(NULL, (to_char(getdate(), 'yyyy') - ui.birth_year), '0') AS age,
          ue.following_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   LEFT JOIN service1_quicket.user_identification_v2 ui ON ui.uid = u.id) AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          p.category_id AS category_id,
          left(p.category_id, 3) AS sub_category_id,
          left(p.category_id, 6) AS second_sub_category_id,
          datediff(DAY, p.create_date, getdate()) AS create_date_diff,
          p.flag_used AS flag_used,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND LENGTH(p.id) <= 10
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT u.id,
          u.favorite_count,
          u.grade,
          u.item_count,
          u.interest,
          u.review_count,
          u.comment_count,
          ue.follower_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS o ON p.uid = o.id
WHERE v.id != o.id
ORDER BY i.server_time ASC;

SELECT
    server_time,
    user_id,
    content_id,
    category_id,
    prev_content_id
FROM (
         SELECT server_time,
                user_id,
                content_id,
                category_id,
                lag(content_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_content_id,
                floor(date_part(EPOCH, server_time) / 300) time_window,
                lag(user_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_user_id,
                floor(date_part(EPOCH, lag(server_time, 1) OVER (ORDER BY user_id, server_time ASC)) /
                      300) prev_time_window
         FROM rec_rp003_view_refined
     )
WHERE user_id = prev_user_id AND time_window = prev_time_window
ORDER BY user_id, server_time ASC;

 SELECT server_time,
        user_id,
        content_id,
        lag(content_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_content_id,
        floor(date_part(EPOCH, server_time::timestamp) / 300) time_window,
        lag(user_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_user_id,
        floor(date_part(EPOCH, lag(server_time::timestamp, 1) OVER (ORDER BY user_id, server_time ASC)::timestamp) / 300) prev_time_window
 FROM (
 	select server_time, user_id, content_id
 	from bun_log_db.app_event_type_view
 	where year||month||day||hour = '2020090909'
 )
WHERE (user_id is not null and prev_user_id is null) or (user_id = prev_user_id) or (user_id is null and prev_user_id is not null)
ORDER BY user_id, prev_user_id;

-------------------------------------------------------------------

DROP TABLE IF EXISTS workspace.emily_ref_term_rank;


CREATE TABLE workspace.emily_ref_term_rank AS
SELECT replace(ref_term,
               ' ',
               '') AS ref_term,
       count(*), row_number() over(
                                   ORDER BY count(*) DESC) AS rank
FROM bun_log_db.app_event_type_impression
WHERE YEAR||MONTH||DAY = '20201010'
GROUP BY 1;


DROP TABLE IF EXISTS workspace.emily_view_temp;

CREATE TABLE workspace.emily_view_temp AS
SELECT imp_id, server_time, user_id, content_id
FROM bun_log_db.app_event_type_view
WHERE year||month||day = '20201010'
	 AND imp_id IS NOT NULL
	 AND user_id IS NOT NULL
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product';


DROP TABLE IF EXISTS workspace.emily_prev_view_temp;

CREATE TABLE workspace.emily_prev_view_temp AS
SELECT imp_id,
		server_time,
	    user_id,
	    content_id,
	    lag(content_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_content_id,
	    floor(date_part(EPOCH, server_time::timestamp) / 300) time_window,
	    lag(user_id, 1) OVER (ORDER BY user_id, server_time ASC) prev_user_id,
	    floor(date_part(EPOCH, lag(server_time::timestamp, 1) OVER (ORDER BY user_id, server_time ASC)::timestamp) / 300) prev_time_window
FROM workspace.emily_view_temp;

DROP TABLE IF EXISTS workspace.emily_prev_view;

CREATE TABLE workspace.emily_prev_view AS
SELECT imp_id,
	   CASE
	   	WHEN user_id IS NULL THEN prev_user_id
	   	ELSE user_id
	   END AS user_id,
	   content_id,
	   prev_content_id,
	   time_window   
FROM workspace.emily_prev_view_temp
WHERE user_id = prev_user_id AND time_window = prev_time_window
UNION
SELECT imp_id,
	   prev_user_id AS user_id,
	   prev_content_id AS content_id,
	   NULL AS prev_content_id,
	   time_window
FROM (
	SELECT *
	FROM workspace.emily_prev_view_temp
	WHERE user_id != prev_user_id
)
;

DROP TABLE IF EXISTS workspace.emily_imp_df;

CREATE TABLE workspace.emily_imp_df AS
SELECT i.imp_id,
	   CASE
           WHEN v.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       i.hour AS hour,
       i.content_id AS imp_content_id,
       i.ref_term AS c_imp_ref_term,
       i.ref_page_id AS c_imp_ref_page_id,
       i.user_id AS imp_user_id,
       v.prev_content_id AS click_prev_content_id,
       v.time_window AS click_time_window              
FROM
  (SELECT imp_id,
          content_id,
          server_time,
          replace(ref_term, ' ', '') AS ref_term,
          ref_page_id,
          user_id,
          event_time,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20201010'
     AND imp_id IS NOT NULL
     AND page_id = '검색결과'
     AND ref_term IS NOT NULL
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN workspace.emily_prev_view v ON i.imp_id = v.imp_id
;

DROP TABLE IF EXISTS workspace.emily_pclick_user;

CREATE TABLE workspace.emily_pclick_user AS
SELECT u.id AS user_id,
	   DATEDIFF(YEAR,
                u.join_date,
                getdate()) AS user_join_date_diff,
       CASE ui.gender
           WHEN '1' THEN '1'
           WHEN '2' THEN '2'
           WHEN '0' THEN '2'
           ELSE '0'
       END AS user_gender,
       Coalesce(NULL, (to_char(getdate(), 'yyyy') - ui.birth_year), '0') AS user_age,
	   ue.following_cnt AS user_following_cnt,
       ue.bunpay_count AS user_bunpay_count,
       ue.parcel_post_count AS user_parcel_post_count,
       ue.transfer_count AS user_transfer_count,
       ue.bunp_meet_count AS user_bunp_meet_count
FROM workspace.emily_imp_df i
LEFT JOIN service1_quicket.user_ u ON i.imp_user_id = u.id
LEFT JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
LEFT JOIN service1_quicket.user_identification_v2 ui ON u.id = ui.uid
;

DROP TABLE IF EXISTS workspace.emily_pclick_owner1;

CREATE TABLE workspace.emily_pclick_owner1 AS
SELECT p1.id AS content_id,
	   p1.price AS content_price,
       p1.category_id AS c_content_category_id,
       left(p1.category_id,
            3) AS c_content_sub_category_id,
       left(p1.category_id,
            6) AS c_content_second_sub_category_id,
       datediff(DAY,
                p1.create_date,
                getdate()) AS content_create_date_diff,
       p1.flag_used AS content_flag_used,
       pe1.emergency_cnt AS content_emergency_cnt,
       pe1.comment_cnt AS content_comment_cnt,
       pe1.interest AS content_interest,
       pe1.pfavcnt AS content_pfavcnt,
	   o1.id AS content_owner_id,
       DATEDIFF(YEAR,
                o1.join_date,
                getdate()) AS content_owner_join_date_diff,
       o1.favorite_count AS content_owner_favorite_count,
       o1.grade AS content_owner_grade,
       o1.item_count AS content_owner_item_count,
       o1.interest AS content_owner_interest,
       o1.review_count AS content_owner_review_count,
       o1.comment_count AS content_owner_comment_count,
       oe1.follower_cnt AS content_owner_follower_count,
       oe1.bunpay_count AS content_owner_bunpay_count,
       oe1.parcel_post_count AS content_owner_parcel_post_count,
       oe1.transfer_count AS content_owner_transfer_count,
       oe1.bunp_meet_count AS content_owner_bunp_meet_count
FROM workspace.emily_imp_df i
JOIN service1_quicket.product_info p1 ON i.imp_content_id = p1.id
JOIN service1_quicket.product_ext pe1 ON p1.id = pe1.pid       
JOIN service1_quicket.user_ o1 ON p1.uid = o1.id
LEFT JOIN service1_quicket.user_extra_info oe1 ON o1.id = oe1.uid
LEFT JOIN service1_quicket.user_identification_v2 oi1 ON o1.id = oi1.uid
;

DROP TABLE IF EXISTS workspace.emily_pclick_owner2;

CREATE TABLE workspace.emily_pclick_owner2 AS
SELECT p2.id AS prev_content_id,
	   p2.price AS prev_content_price,
       p2.category_id AS c_prev_content_category_id,
       left(p2.category_id,
            3) AS c_prev_content_sub_category_id,
       left(p2.category_id,
            6) AS c_prev_content_second_sub_category_id,
       datediff(DAY,
                p2.create_date,
                getdate()) AS prev_content_create_date_diff,
       p2.flag_used AS prev_content_flag_used,
       pe2.emergency_cnt AS prev_content_emergency_cnt,
       pe2.comment_cnt AS prev_content_comment_cnt,
       pe2.interest AS prev_content_interest,
       pe2.pfavcnt AS prev_content_pfavcnt,
       o2.id AS prev_content_owner_id,
       DATEDIFF(YEAR,
                o2.join_date,
                getdate()) AS prev_content_owner_join_date_diff,
       o2.favorite_count AS prev_content_owner_favorite_count,
       o2.grade AS prev_content_owner_grade,
       o2.item_count AS prev_content_owner_item_count,
       o2.interest AS prev_content_owner_interest,
       o2.review_count AS prev_content_owner_review_count,
       o2.comment_count AS prev_content_owner_comment_count,
       oe2.follower_cnt AS prev_content_owner_follower_count,
       oe2.bunpay_count AS prev_content_owner_bunpay_count,
       oe2.parcel_post_count AS prev_content_owner_parcel_post_count,
       oe2.transfer_count AS prev_content_owner_transfer_count,
       oe2.bunp_meet_count AS prev_content_owner_bunp_meet_count
FROM workspace.emily_imp_df i
JOIN service1_quicket.product_info p2 ON i.click_prev_content_id = p2.id
JOIN service1_quicket.product_ext pe2 ON p2.id = pe2.pid
JOIN service1_quicket.user_ o2 ON p2.uid = o2.id
LEFT JOIN service1_quicket.user_extra_info oe2 ON o2.id = oe2.uid
LEFT JOIN service1_quicket.user_identification_v2 oi2 ON o2.id = oi2.uid
;

DROP TABLE IF EXISTS workspace.emily_pclick_df;

CREATE TABLE workspace.emily_pclick_df AS
SELECT i.label,
       i.hour AS c_imp_hour,
       i.c_imp_ref_term,
       r.rank AS imp_ref_term_rank,
       i.c_imp_ref_page_id,
	   u.user_id,
	   u.user_join_date_diff,
	   u.user_gender,
	   u.user_age,
	   u.user_following_cnt,
	   u.user_bunpay_count,
	   u.user_parcel_post_count,
	   u.user_transfer_count,
	   u.user_bunp_meet_count,
	   o1.content_id,
	   o1.content_price,
	   o1.c_content_category_id,
	   o1.c_content_second_sub_category_id,
	   o1.c_content_sub_category_id,
	   o1.content_create_date_diff,
	   o1.content_flag_used,
	   o1.content_emergency_cnt,
	   o1.content_comment_cnt,
	   o1.content_interest,
	   o1.content_pfavcnt,
	   o1.content_owner_id,
	   o1.content_owner_join_date_diff,
	   o1.content_owner_favorite_count,
	   o1.content_owner_grade,
	   o1.content_owner_item_count,
	   o1.content_owner_interest,
       o1.content_owner_review_count,
       o1.content_owner_comment_count,
       o1.content_owner_follower_count,
       o1.content_owner_bunpay_count,
       o1.content_owner_parcel_post_count,
       o1.content_owner_transfer_count,
       o1.content_owner_bunp_meet_count,	   
	   o2.prev_content_id,
	   o2.prev_content_price,
       o2.c_prev_content_category_id,
       o2.c_prev_content_sub_category_id,
       o2.c_prev_content_second_sub_category_id,
       o2.prev_content_create_date_diff,
       o2.prev_content_flag_used,
       o2.prev_content_emergency_cnt,
       o2.prev_content_comment_cnt,
       o2.prev_content_interest,
       o2.prev_content_pfavcnt,
       o2.prev_content_owner_id,
       o2.prev_content_owner_join_date_diff,
       o2.prev_content_owner_favorite_count,
       o2.prev_content_owner_grade,
       o2.prev_content_owner_item_count,
       o2.prev_content_owner_interest,
       o2.prev_content_owner_review_count,
       o2.prev_content_owner_comment_count,
       o2.prev_content_owner_follower_count,
       o2.prev_content_owner_bunpay_count,
       o2.prev_content_owner_parcel_post_count,
       o2.prev_content_owner_transfer_count,
       o2.prev_content_owner_bunp_meet_count
FROM workspace.emily_imp_df i
LEFT JOIN workspace.emily_ref_term_rank r ON i.c_imp_ref_term = r.ref_term
LEFT JOIN workspace.emily_pclick_user u ON i.imp_user_id = u.user_id
LEFT JOIN workspace.emily_pclick_owner1 o1 ON i.imp_content_id = o1.content_id
LEFT JOIN workspace.emily_pclick_owner2 o2 ON i.click_prev_content_id = o2.prev_content_id
WHERE u.user_id != o1.content_owner_id
  AND u.user_id != o2.prev_content_owner_id
  AND o1.content_price % 10 = 0
  AND o1.content_price >= 1000
  AND o2.prev_content_price % 10 = 0
  AND o2.prev_content_price >= 1000
;


SHOW COLUMNS FROM database_Name.table_name;
-------------------------------




DROP TABLE IF EXISTS workspace.emily_ref_term_rank;

CREATE TABLE workspace.emily_ref_term_rank AS
SELECT replace(ref_term,
               ' ',
               '')::varchar AS ref_term,
       count(*)::int AS ref_term_imp_cnt, 
       row_number() over( ORDER BY count(*) DESC )::int AS rank
FROM bun_log_db.app_event_type_impression
WHERE YEAR||MONTH||DAY = '20201015'
GROUP BY 1;


DROP TABLE IF EXISTS workspace.emily_view_temp;

CREATE TABLE workspace.emily_view_temp AS
SELECT imp_id::varchar, 
	   server_time::timestamp, 
	   user_id::varchar, 
	   content_id::varchar
FROM bun_log_db.app_event_type_view
WHERE year||month||day = '20201015'
	 AND imp_id IS NOT NULL
	 AND user_id IS NOT NULL
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product';


DROP TABLE IF EXISTS workspace.emily_prev_view_temp;

CREATE TABLE workspace.emily_prev_view_temp AS
SELECT imp_id::varchar,
		server_time::timestamp,
	    user_id::varchar,
	    content_id::varchar,
	    lag(content_id, 1) OVER (ORDER BY user_id, server_time ASC)::varchar prev_content_id,
	    floor(date_part(EPOCH, server_time::timestamp) / 300)::int time_window,
	    lag(user_id, 1) OVER (ORDER BY user_id, server_time ASC)::varchar prev_user_id,
	    floor(date_part(EPOCH, lag(server_time::timestamp, 1) OVER (ORDER BY user_id, server_time ASC)::timestamp) / 300)::int prev_time_window
FROM workspace.emily_view_temp;

DROP TABLE IF EXISTS workspace.emily_prev_view;

CREATE TABLE workspace.emily_prev_view AS
SELECT imp_id::varchar,
	   CASE
	   	WHEN user_id IS NULL THEN prev_user_id
	   	ELSE user_id
	   END::varchar AS user_id,
	   content_id::varchar,
	   prev_content_id::varchar,
	   time_window::int   
FROM workspace.emily_prev_view_temp
WHERE user_id = prev_user_id AND time_window = prev_time_window
UNION
SELECT imp_id::varchar,
	   prev_user_id::varchar AS user_id,
	   prev_content_id::varchar AS content_id,
	   NULL AS prev_content_id,
	   time_window::int
FROM (
	SELECT *
	FROM workspace.emily_prev_view_temp
	WHERE user_id != prev_user_id
)
;

DROP TABLE IF EXISTS workspace.emily_imp_df;

CREATE TABLE workspace.emily_imp_df AS
SELECT i.imp_id,
	   CASE
           WHEN v.imp_id IS NOT NULL THEN 1
           ELSE 0
       END::int AS label,
       i.hour::int AS hour,
       i.content_id::varchar AS imp_content_id,
       i.ref_term::varchar AS c_imp_ref_term,
       i.ref_page_id::varchar AS c_imp_ref_page_id,
       i.user_id::varchar AS imp_user_id,
       v.prev_content_id::varchar AS click_prev_content_id,
       v.time_window::int AS click_time_window              
FROM
  (SELECT imp_id,
          content_id,
          server_time,
          replace(ref_term, ' ', '') AS ref_term,
          ref_page_id,
          user_id,
          event_time,
          hour
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR||MONTH||DAY = '20201015'
     AND imp_id IS NOT NULL
     AND page_id = '검색결과'
     AND ref_term IS NOT NULL
     AND ref_source IN ('power_up',
                        'ad_super_up',
                        'ad_super_up_shop')
     AND app_name != '번개장터 DEBUG'
     AND app_id != 'kr.co.quicket.debug'
     AND content_type = 'product') AS i
LEFT JOIN workspace.emily_prev_view v ON i.imp_id = v.imp_id
;

DROP TABLE IF EXISTS workspace.emily_pclick_user;

CREATE TABLE workspace.emily_pclick_user AS
SELECT u.id::varchar AS user_id,
	   DATEDIFF(YEAR,
                u.join_date,
                getdate())::int AS user_join_date_diff,
       CASE ui.gender
           WHEN '1' THEN '1'
           WHEN '2' THEN '2'
           WHEN '0' THEN '2'
           ELSE '0'
       END::int AS user_gender,
       Coalesce(NULL, (to_char(getdate(), 'yyyy') - ui.birth_year), '0')::int AS user_age,
	   ue.following_cnt::int AS user_following_cnt,
       ue.bunpay_count::int AS user_bunpay_count,
       ue.parcel_post_count::int AS user_parcel_post_count,
       ue.transfer_count::int AS user_transfer_count,
       ue.bunp_meet_count::int AS user_bunp_meet_count
FROM workspace.emily_imp_df i
LEFT JOIN service1_quicket.user_ u ON i.imp_user_id = u.id
LEFT JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
LEFT JOIN service1_quicket.user_identification_v2 ui ON u.id = ui.uid
;


DROP TABLE IF EXISTS workspace.emily_pclick_owner1;

CREATE TABLE workspace.emily_pclick_owner1 AS
SELECT p1.id::varchar AS content_id,
	   p1.price::int AS content_price,
       p1.category_id::varchar AS c_content_category_id,
       left(p1.category_id,
            3)::varchar AS c_content_sub_category_id,
       left(p1.category_id,
            6)::varchar AS c_content_second_sub_category_id,
       datediff(DAY,
                p1.create_date,
                getdate())::int AS content_create_date_diff,
       p1.flag_used::int AS content_flag_used,
       pe1.emergency_cnt::int AS content_emergency_cnt,
       pe1.comment_cnt::int AS content_comment_cnt,
       pe1.interest::int AS content_interest,
       pe1.pfavcnt::int AS content_pfavcnt,
	   o1.id::varchar AS content_owner_id,
       DATEDIFF(YEAR,
                o1.join_date,
                getdate())::int AS content_owner_join_date_diff,
       o1.favorite_count::int AS content_owner_favorite_count,
       o1.grade::int AS content_owner_grade,
       o1.item_count::int AS content_owner_item_count,
       o1.interest::int AS content_owner_interest,
       o1.review_count::int AS content_owner_review_count,
       o1.comment_count::int AS content_owner_comment_count,
       oe1.follower_cnt::int AS content_owner_follower_count,
       oe1.bunpay_count::int AS content_owner_bunpay_count,
       oe1.parcel_post_count::int AS content_owner_parcel_post_count,
       oe1.transfer_count::int AS content_owner_transfer_count,
       oe1.bunp_meet_count::int AS content_owner_bunp_meet_count
FROM workspace.emily_imp_df i
JOIN service1_quicket.product_info p1 ON i.imp_content_id = p1.id
JOIN service1_quicket.product_ext pe1 ON p1.id = pe1.pid       
JOIN service1_quicket.user_ o1 ON p1.uid = o1.id
LEFT JOIN service1_quicket.user_extra_info oe1 ON o1.id = oe1.uid
LEFT JOIN service1_quicket.user_identification_v2 oi1 ON o1.id = oi1.uid
;

DROP TABLE IF EXISTS workspace.emily_pclick_owner2;

CREATE TABLE workspace.emily_pclick_owner2 AS
SELECT p2.id::varchar AS prev_content_id,
	   p2.price::int AS prev_content_price,
       p2.category_id::varchar AS c_prev_content_category_id,
       left(p2.category_id,
            3)::varchar AS c_prev_content_sub_category_id,
       left(p2.category_id,
            6)::varchar AS c_prev_content_second_sub_category_id,
       datediff(DAY,
                p2.create_date,
                getdate())::int AS prev_content_create_date_diff,
       p2.flag_used::int AS prev_content_flag_used,
       pe2.emergency_cnt::int AS prev_content_emergency_cnt,
       pe2.comment_cnt::int AS prev_content_comment_cnt,
       pe2.interest::int AS prev_content_interest,
       pe2.pfavcnt::int AS prev_content_pfavcnt,
       o2.id::varchar AS prev_content_owner_id,
       DATEDIFF(YEAR,
                o2.join_date,
                getdate())::int AS prev_content_owner_join_date_diff,
       o2.favorite_count::int AS prev_content_owner_favorite_count,
       o2.grade::int AS prev_content_owner_grade,
       o2.item_count::int AS prev_content_owner_item_count,
       o2.interest::int AS prev_content_owner_interest,
       o2.review_count::int AS prev_content_owner_review_count,
       o2.comment_count::int AS prev_content_owner_comment_count,
       oe2.follower_cnt::int AS prev_content_owner_follower_count,
       oe2.bunpay_count::int AS prev_content_owner_bunpay_count,
       oe2.parcel_post_count::int AS prev_content_owner_parcel_post_count,
       oe2.transfer_count::int AS prev_content_owner_transfer_count,
       oe2.bunp_meet_count::int AS prev_content_owner_bunp_meet_count
FROM workspace.emily_imp_df i
JOIN service1_quicket.product_info p2 ON i.click_prev_content_id = p2.id
JOIN service1_quicket.product_ext pe2 ON p2.id = pe2.pid
JOIN service1_quicket.user_ o2 ON p2.uid = o2.id
LEFT JOIN service1_quicket.user_extra_info oe2 ON o2.id = oe2.uid
LEFT JOIN service1_quicket.user_identification_v2 oi2 ON o2.id = oi2.uid
;

DROP TABLE IF EXISTS workspace.emily_pclick_df1;

CREATE TABLE workspace.emily_pclick_df1 AS
SELECT i.label::int,
       i.hour::int AS c_imp_hour,
       i.c_imp_ref_term::varchar,
       r.rank::int AS imp_ref_term_rank,
       i.c_imp_ref_page_id::varchar,
       i.imp_content_id::varchar,
       i.click_prev_content_id::varchar,
	   u.user_id::varchar,
	   u.user_join_date_diff::int,
	   u.user_gender::int,
	   u.user_age::int,
	   u.user_following_cnt::int,
	   u.user_bunpay_count::int,
	   u.user_parcel_post_count::int,
	   u.user_transfer_count::int,
	   u.user_bunp_meet_count::int 
FROM workspace.emily_imp_df i
LEFT JOIN workspace.emily_ref_term_rank r ON i.c_imp_ref_term = r.ref_term
LEFT JOIN workspace.emily_pclick_user u ON i.imp_user_id = u.user_id;

DROP TABLE IF EXISTS workspace.emily_pclick_df2;

CREATE TABLE workspace.emily_pclick_df2 AS
SELECT i.*,
	   o1.content_id::varchar,
	   o1.content_price::int,
	   o1.c_content_category_id::varchar,
	   o1.c_content_second_sub_category_id::varchar,
	   o1.c_content_sub_category_id::varchar,
	   o1.content_create_date_diff::int,
	   o1.content_flag_used::int,
	   o1.content_emergency_cnt::int,
	   o1.content_comment_cnt::int,
	   o1.content_interest::int,
	   o1.content_pfavcnt::int,
	   o1.content_owner_id::varchar,
	   o1.content_owner_join_date_diff::int,
	   o1.content_owner_favorite_count::int,
	   o1.content_owner_grade::int,
	   o1.content_owner_item_count::int,
	   o1.content_owner_interest::int,
       o1.content_owner_review_count::int,
       o1.content_owner_comment_count::int,
       o1.content_owner_follower_count::int,
       o1.content_owner_bunpay_count::int,
       o1.content_owner_parcel_post_count::int,
       o1.content_owner_transfer_count::int,
       o1.content_owner_bunp_meet_count::int
FROM workspace.emily_pclick_df1 i
LEFT JOIN workspace.emily_pclick_owner1 o1 ON i.imp_content_id = o1.content_id
WHERE i.user_id != o1.content_owner_id
  AND o1.content_price % 10 = 0
  AND o1.content_price >= 1000;


DROP TABLE IF EXISTS workspace.emily_pclick_df;

CREATE TABLE workspace.emily_pclick_df AS
SELECT i.*,	   
	   o2.prev_content_id::varchar,
	   o2.prev_content_price::int,
       o2.c_prev_content_category_id::varchar,
       o2.c_prev_content_sub_category_id::varchar,
       o2.c_prev_content_second_sub_category_id::varchar,
       o2.prev_content_create_date_diff::int,
       o2.prev_content_flag_used::int,
       o2.prev_content_emergency_cnt::int,
       o2.prev_content_comment_cnt::int,
       o2.prev_content_interest::int,
       o2.prev_content_pfavcnt::int,
       o2.prev_content_owner_id::varchar,
       o2.prev_content_owner_join_date_diff::int,
       o2.prev_content_owner_favorite_count::int,
       o2.prev_content_owner_grade::int,
       o2.prev_content_owner_item_count::int,
       o2.prev_content_owner_interest::int,
       o2.prev_content_owner_review_count::int,
       o2.prev_content_owner_comment_count::int,
       o2.prev_content_owner_follower_count::int,
       o2.prev_content_owner_bunpay_count::int,
       o2.prev_content_owner_parcel_post_count::int,
       o2.prev_content_owner_transfer_count::int,
       o2.prev_content_owner_bunp_meet_count::int
FROM workspace.emily_pclick_df2 i
LEFT JOIN workspace.emily_pclick_owner2 o2 ON i.click_prev_content_id = o2.prev_content_id
WHERE o2.prev_content_price % 10 = 0
  AND o2.prev_content_price >= 1000
;




DROP TABLE IF EXISTS workspace.emily_pclick_df;

CREATE TABLE workspace.emily_pclick_df AS
SELECT i.label::int,
       i.hour AS c_imp_hour::int,
       i.c_imp_ref_term::varchar,
       r.rank AS imp_ref_term_rank::varchar,
       i.c_imp_ref_page_id::varchar,
	   u.user_id::varchar,
	   u.user_join_date_diff::int,
	   u.user_gender::int,
	   u.user_age::int,
	   u.user_following_cnt::int,
	   u.user_bunpay_count::int,
	   u.user_parcel_post_count::int,
	   u.user_transfer_count::int,
	   u.user_bunp_meet_count::int,
	   o1.content_id::varchar,
	   o1.content_price::int,
	   o1.c_content_category_id::varchar,
	   o1.c_content_second_sub_category_id::varchar,
	   o1.c_content_sub_category_id::varchar,
	   o1.content_create_date_diff::int,
	   o1.content_flag_used::int,
	   o1.content_emergency_cnt::int,
	   o1.content_comment_cnt::int,
	   o1.content_interest::int,
	   o1.content_pfavcnt::int,
	   o1.content_owner_id::varchar,
	   o1.content_owner_join_date_diff::int,
	   o1.content_owner_favorite_count::int,
	   o1.content_owner_grade::int,
	   o1.content_owner_item_count::int,
	   o1.content_owner_interest::int,
       o1.content_owner_review_count::int,
       o1.content_owner_comment_count::int,
       o1.content_owner_follower_count::int,
       o1.content_owner_bunpay_count::int,
       o1.content_owner_parcel_post_count::int,
       o1.content_owner_transfer_count::int,
       o1.content_owner_bunp_meet_count::int,	   
	   o2.prev_content_id::varchar,
	   o2.prev_content_price::int,
       o2.c_prev_content_category_id::varchar,
       o2.c_prev_content_sub_category_id::varchar,
       o2.c_prev_content_second_sub_category_id::varchar,
       o2.prev_content_create_date_diff::int,
       o2.prev_content_flag_used::int,
       o2.prev_content_emergency_cnt::int,
       o2.prev_content_comment_cnt::int,
       o2.prev_content_interest::int,
       o2.prev_content_pfavcnt::int,
       o2.prev_content_owner_id::varchar,
       o2.prev_content_owner_join_date_diff::int,
       o2.prev_content_owner_favorite_count::int,
       o2.prev_content_owner_grade::int,
       o2.prev_content_owner_item_count::int,
       o2.prev_content_owner_interest::int,
       o2.prev_content_owner_review_count::int,
       o2.prev_content_owner_comment_count::int,
       o2.prev_content_owner_follower_count::int,
       o2.prev_content_owner_bunpay_count::int,
       o2.prev_content_owner_parcel_post_count::int,
       o2.prev_content_owner_transfer_count::int,
       o2.prev_content_owner_bunp_meet_count::int
FROM workspace.emily_imp_df i
LEFT JOIN workspace.emily_ref_term_rank r ON i.c_imp_ref_term = r.ref_term
LEFT JOIN workspace.emily_pclick_user u ON i.imp_user_id = u.user_id
LEFT JOIN workspace.emily_pclick_owner1 o1 ON i.imp_content_id = o1.content_id
LEFT JOIN workspace.emily_pclick_owner2 o2 ON i.click_prev_content_id = o2.prev_content_id
WHERE u.user_id != o1.content_owner_id
  AND u.user_id != o2.prev_content_owner_id
  AND o1.content_price % 10 = 0
  AND o1.content_price >= 1000
  AND o2.prev_content_price % 10 = 0
  AND o2.prev_content_price >= 1000
;


--------------------------------------------------------------------------

DROP TABLE IF EXISTS workspace.emily_product_temp;

CREATE TABLE workspace.emily_product_temp AS
SELECT i.content_id,
	   p.uid 
FROM bun_log_db.app_event_type_impression i
JOIN service1_quicket.product_info p ON i.content_id = p.id
WHERE i.YEAR||i.MONTH||i.DAY||i.HOUR = '2020110110'
  AND i.ref_source IN ('ad_super_up',
                     'ad_super_up_shop',
                     'power_up')
  AND i.ref_term = '패딩';


SELECT ref_term, count(distinct content_id)
FROM bun_log_db.app_event_type_impression
WHERE year||month||day||hour = '2020110110'
	AND ref_source IN ('ad_super_up',
                     'ad_super_up_shop',
                     'power_up')
GROUP BY 1
ORDER BY 2 DESC;




SELECT *
FROM bun_log_db.app_event_type_impression
WHERE year||month||day||hour = '2020111717'
	AND page_id = '검색결과'
	AND ref_term IS NOT NULL
	AND ref_source IS NOT NULL
;


SELECT p.*,
       pe.*
FROM service1_quicket.product_info p
JOIN service1_quicket.product_ext pe ON p.id = pe.pid
JOIN service1_quicket.product_up u ON p.id = u.pid
WHERE u.modified_at >= dateadd(MONTH, -3, getdate());

SELECT YEAR||MONTH||DAY AS date, count(*) AS android_imp_cnt,
                                 android_imp_cnt * 1.8 AS pred_total_imp_cnt
FROM bun_log_db.api_event_type_impression_ad
WHERE ref_source IN ('sa',
                     'ca')
  AND device_type = 'a'
  AND YEAR||MONTH||DAY >= '20201214'
  AND YEAR||MONTH||DAY <= '20201220'
GROUP BY 1
ORDER BY 1 ASC;

SELECT YEAR||MONTH||DAY AS date, count(*) AS android_click_cnt,
                                 android_imp_cnt * 1.8 AS pred_total_click_cnt
FROM bun_log_db.api_event_type_click_ad
WHERE ref_source IN ('sa',
                     'ca')
  AND device_type = 'a'
  AND YEAR||MONTH||DAY >= '20201214'
  AND YEAR||MONTH||DAY <= '20201220'
GROUP BY 1
ORDER BY 1 ASC;

SELECT YEAR||MONTH||DAY AS date, count(*) AS android_imp_cnt,
                                 android_imp_cnt * 1.8 AS pred_total_imp_cnt
FROM bun_log_db.app_event_type_impression
WHERE device_type = 'a'
  AND YEAR||MONTH||DAY >= '20201130'
  AND YEAR||MONTH||DAY <= '20201206'
  AND ref_source IN ('ad_super_up_shop',
                     'ad_super_up',
                     'power_up')
GROUP BY 1
ORDER BY 1 ASC;

SELECT YEAR||MONTH||DAY AS date, count(*) AS android_click_cnt,
                                 android_imp_cnt * 1.8 AS pred_total_click_cnt
FROM bun_log_db.app_event_type_view
WHERE device_type = 'a'
  AND YEAR||MONTH||DAY >= '20201130'
  AND YEAR||MONTH||DAY <= '20201206'
  AND ref_source IN ('ad_super_up_shop',
                     'ad_super_up',
                     'power_up')
GROUP BY 1
ORDER BY 1 ASC;

SELECT imp.content_id,
       imp.imp_cnt,
       pi.name,
       bid.keyword,
       bid.bid_price
FROM
  (SELECT content_id,
          count(*) AS imp_cnt
   FROM bun_log_db.api_event_type_impression_ad
   WHERE YEAR||MONTH||DAY||hour = '2020121818'
     AND ref_term IS NOT NULL
   GROUP BY 1) AS imp
JOIN service1_quicket.product_info pi ON imp.content_id = pi.id
JOIN service1_quicket.ad_set_product p ON pi.id = p.pid
JOIN service1_quicket.ad_set_product_bid_keyword bid ON p.id = bid.ad_set_product_id
;


----------------------------------------------------------
----------------------------------------------------------
----------------------------------------------------------
DROP TABLE IF EXISTS workspace.emily_view_temp;
CREATE TABLE workspace.emily_view_temp AS
SELECT imp_id,
       event_time,
       page_id,
       bid_price,
       ref_source,
       app_name,
       app_id,
       content_type,
       YEAR,
       MONTH,
       DAY,
       hour,
       user_id,
       content_id
FROM bun_log_db.api_event_type_click_ad
WHERE YEAR||MONTH||DAY >= '20201223'
  AND YEAR||MONTH||DAY <= '20201229'
  AND event_action = 'click_ad'
  AND content_id IS NOT NULL
  AND imp_id IS NOT NULL
  AND ref_term IS NOT NULL
  AND ref_source = 'sa'
  AND app_name != '번개장터 DEBUG'
  AND app_id != 'kr.co.quicket.debug'
  AND app_version != 'staging'
  AND content_type = 'product';
  
DROP TABLE IF EXISTS workspace.emily_prev_view_temp;
CREATE TABLE workspace.emily_prev_view_temp AS
SELECT imp_id,
       event_time,
       user_id,
       content_id,
       lag(content_id,
           1) OVER (
                    ORDER BY user_id,
                             event_time ASC) prev_content_id,
                   --floor(date_part(EPOCH, event_time::TIMESTAMP) / 3600) time_window,
                        lag(user_id,
                            1) OVER (
                                     ORDER BY user_id,
                                              event_time ASC) prev_user_id
                                    --floor(date_part(EPOCH, lag(event_time::TIMESTAMP, 1) OVER (
                                                                                              -- ORDER BY user_id, event_time ASC)::TIMESTAMP) / 3600) prev_time_window
FROM workspace.emily_view_temp;


DROP TABLE IF EXISTS workspace.emily_view_df;
CREATE TABLE workspace.emily_view_df AS
SELECT imp_id,
       event_time,
       CASE
           WHEN user_id IS NULL THEN prev_user_id
           ELSE user_id
       END AS user_id,
       content_id,
       prev_content_id
--       time_window
FROM workspace.emily_prev_view_temp
WHERE user_id = prev_user_id
--  AND time_window = prev_time_window
UNION
SELECT imp_id,
	   event_time,
       prev_user_id AS user_id,
       prev_content_id AS content_id,
       NULL AS prev_content_id
--       time_window
FROM
  (SELECT *
   FROM workspace.emily_prev_view_temp
   WHERE user_id != prev_user_id ) ;
   
   
DROP TABLE IF EXISTS workspace.emily_imp_df;
CREATE TABLE workspace.emily_imp_df AS
SELECT imp_id,
       content_id,
       server_time,
       bid_price,
       lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS ref_term,
       user_id,
       event_time,
       NVL(YEAR||MONTH||DAY,
                        '0') AS DAY,
       NVL(HOUR,
           '0') AS hour
FROM bun_log_db.api_event_type_impression_ad
WHERE YEAR||MONTH||DAY >= '20201223'
  AND YEAR||MONTH||DAY <= '20201229'
  AND event_action = 'impression_ad'
  AND content_id IS NOT NULL
  AND imp_id IS NOT NULL
  AND ref_term IS NOT NULL
  AND ref_source = 'sa'
  AND bid_price > 0
  AND page_id = '검색결과'
  AND view_id = '검색상품'
  AND app_name != '번개장터 DEBUG'
  AND app_id != 'kr.co.quicket.debug'
  AND app_version != 'staging'
  AND content_type = 'product';
  
DROP TABLE IF EXISTS workspace.emily_pclick_df;
CREATE TABLE workspace.emily_pclick_df AS
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       i.day,
       i.hour,
       i.ref_term AS c_imp_ref_term,
       v.gender AS c_user_gender,
       v.age AS c_user_age,
       v.following_cnt AS user_following_count,
       v.bunpay_count AS user_bunpay_count,
       v.parcel_post_count AS user_parcel_post_count,
       v.transfer_count AS user_transfer_count,
       v.bunp_meet_count AS user_bunp_meet_count,
       o.grade AS owner_grade,
       o.item_count AS owner_item_count,
       o.interest AS owner_interest_count,
       o.follower_cnt AS owner_follower_count,
       o.bunpay_count AS owner_bunpay_count,
       o.review_count AS owner_review_count,
       o.parcel_post_count AS owner_parcel_post_count,
       o.transfer_count AS owner_transfer_count,
       o.bunp_meet_count AS owner_bunp_meet_count,
       o.favorite_count AS owner_favorite_count,
       o.comment_count AS owner_comment_count,
       i.bid_price AS content_bid_price,
       p.price AS content_price,
       p.flag_used AS c_content_flag_used,
       p.category_id_1 AS c_content_category_id_1,
       p.category_id_2 AS c_content_category_id_2,
       p.category_id_3 AS c_content_category_id_3,
       p.emergency_cnt AS content_emergency_count,
       p.comment_cnt AS content_comment_count,
       p.interest AS content_interest_count,
       p.pfavcnt AS content_favorite_count,
       prev_p.price AS prev_content_price,
       prev_p.flag_used AS c_prev_content_flag_used,
       prev_p.category_id_1 AS c_prev_content_category_id_1,
       prev_p.category_id_2 AS c_prev_content_category_id_2,
       prev_p.category_id_3 AS c_prev_content_category_id_3,
       prev_p.emergency_cnt AS prev_content_emergency_count,
       prev_p.comment_cnt AS prev_content_comment_count,
       prev_p.interest AS prev_content_interest_count,
       prev_p.pfavcnt AS prev_content_favorite_count,
       i.content_id AS imp_content_id,
       c.prev_content_id AS click_prev_content_id,
       i.server_time AS server_time
FROM workspace.emily_imp_df AS i
LEFT JOIN workspace.emily_view_df AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN
  (SELECT u.id,
          CASE ui.gender
              WHEN '1' THEN '1'
              WHEN '2' THEN '2'
              WHEN '0' THEN '2'
              ELSE '0'
          END AS gender,
          Coalesce(NULL, (to_char(getdate(), 'yyyy') - ui.birth_year), '0') AS age,
          ue.following_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   LEFT JOIN service1_quicket.user_identification_v2 ui ON ui.uid = u.id) AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          p.flag_used,
          CASE
              WHEN LEFT(p.category_id, 3)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 3), '0')
          END AS category_id_1,
          CASE
              WHEN LEFT(p.category_id, 6)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 6), '0')
          END AS category_id_2,
          CASE
              WHEN p.category_id='' THEN '0'
              ELSE COALESCE(p.category_id, '0')
          END AS category_id_3,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND LENGTH(p.id) <= 10
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          p.flag_used,
          CASE
              WHEN LEFT(p.category_id, 3)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 3), '0')
          END AS category_id_1,
          CASE
              WHEN LEFT(p.category_id, 6)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 6), '0')
          END AS category_id_2,
          CASE
              WHEN p.category_id='' THEN '0'
              ELSE COALESCE(p.category_id, '0')
          END AS category_id_3,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND LENGTH(p.id) <= 10
     AND p.price BETWEEN 10000 AND 10000000) prev_p ON c.prev_content_id = prev_p.id
LEFT JOIN
  (SELECT u.id,
          u.favorite_count,
          u.grade,
          u.item_count,
          u.interest,
          u.review_count,
          u.comment_count,
          ue.follower_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS o ON p.uid = o.id
WHERE v.id != o.id
ORDER BY i.server_time ASC;


DROP TABLE IF EXISTS workspace.emily_pclick_df;
CREATE TABLE workspace.emily_pclick_df AS
SELECT CASE
           WHEN c.imp_id IS NOT NULL THEN 1
           ELSE 0
       END AS label,
       i.day,
       i.hour,
       i.ref_term AS c_imp_ref_term,
       v.gender AS c_user_gender,
       v.age AS c_user_age,
       v.following_cnt AS user_following_count,
       v.bunpay_count AS user_bunpay_count,
       v.parcel_post_count AS user_parcel_post_count,
       v.transfer_count AS user_transfer_count,
       v.bunp_meet_count AS user_bunp_meet_count,
       o.grade AS owner_grade,
       o.item_count AS owner_item_count,
       o.interest AS owner_interest_count,
       o.follower_cnt AS owner_follower_count,
       o.bunpay_count AS owner_bunpay_count,
       o.review_count AS owner_review_count,
       o.parcel_post_count AS owner_parcel_post_count,
       o.transfer_count AS owner_transfer_count,
       o.bunp_meet_count AS owner_bunp_meet_count,
       o.favorite_count AS owner_favorite_count,
       o.comment_count AS owner_comment_count,
       i.bid_price AS content_bid_price,
       p.price AS content_price,
       p.flag_used AS c_content_flag_used,
       p.category_id_1 AS c_content_category_id_1,
       p.category_id_2 AS c_content_category_id_2,
       p.category_id_3 AS c_content_category_id_3,
       p.emergency_cnt AS content_emergency_count,
       p.comment_cnt AS content_comment_count,
       p.interest AS content_interest_count,
       p.pfavcnt AS content_favorite_count,
       prev_p.price AS prev_content_price,
       prev_p.flag_used AS c_prev_content_flag_used,
       prev_p.category_id_1 AS c_prev_content_category_id_1,
       prev_p.category_id_2 AS c_prev_content_category_id_2,
       prev_p.category_id_3 AS c_prev_content_category_id_3,
       prev_p.emergency_cnt AS prev_content_emergency_count,
       prev_p.comment_cnt AS prev_content_comment_count,
       prev_p.interest AS prev_content_interest_count,
       prev_p.pfavcnt AS prev_content_favorite_count,
       i.content_id AS imp_content_id,
       ip.prev_click_content_id AS click_prev_content_id,
       i.server_time AS server_time
FROM workspace.emily_imp_df AS i
LEFT JOIN workspace.emily_view_df AS c ON i.imp_id=c.imp_id
AND i.event_time <= c.event_time
LEFT JOIN workspace.emily_imp_prev AS ip ON i.imp_id = ip.imp_id
LEFT JOIN
  (SELECT u.id,
          CASE ui.gender
              WHEN '1' THEN '1'
              WHEN '2' THEN '2'
              WHEN '0' THEN '2'
              ELSE '0'
          END AS gender,
          Coalesce(NULL, (to_char(getdate(), 'yyyy') - ui.birth_year), '0') AS age,
          ue.following_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   LEFT JOIN service1_quicket.user_identification_v2 ui ON ui.uid = u.id) AS v ON i.user_id = v.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          p.flag_used,
          CASE
              WHEN LEFT(p.category_id, 3)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 3), '0')
          END AS category_id_1,
          CASE
              WHEN LEFT(p.category_id, 6)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 6), '0')
          END AS category_id_2,
          CASE
              WHEN p.category_id='' THEN '0'
              ELSE COALESCE(p.category_id, '0')
          END AS category_id_3,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND LENGTH(p.id) <= 10
     AND p.price BETWEEN 10000 AND 10000000) p ON i.content_id = p.id
LEFT JOIN
  (SELECT p.id,
          p.uid,
          p.price,
          p.flag_used,
          CASE
              WHEN LEFT(p.category_id, 3)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 3), '0')
          END AS category_id_1,
          CASE
              WHEN LEFT(p.category_id, 6)='' THEN '0'
              ELSE COALESCE(LEFT(p.category_id, 6), '0')
          END AS category_id_2,
          CASE
              WHEN p.category_id='' THEN '0'
              ELSE COALESCE(p.category_id, '0')
          END AS category_id_3,
          pe.emergency_cnt,
          pe.comment_cnt,
          pe.interest,
          pe.pfavcnt
   FROM service1_quicket.product_info p
   JOIN service1_quicket.product_ext pe ON p.id = pe.pid
   WHERE p.price % 10 = 0
     AND LENGTH(p.id) <= 10
     AND p.price BETWEEN 10000 AND 10000000) prev_p ON ip.prev_click_content_id = prev_p.id
LEFT JOIN
  (SELECT u.id,
          u.favorite_count,
          u.grade,
          u.item_count,
          u.interest,
          u.review_count,
          u.comment_count,
          ue.follower_cnt,
          ue.bunpay_count,
          ue.parcel_post_count,
          ue.transfer_count,
          ue.bunp_meet_count
   FROM service1_quicket.user_ u
   JOIN service1_quicket.user_extra_info ue ON u.id = ue.uid
   WHERE ue.is_identification = 1) AS o ON p.uid = o.id
ORDER BY i.server_time ASC;

DROP TABLE IF EXISTS workspace.emily_prev_temp;


CREATE TABLE workspace.emily_prev_temp AS
SELECT i.imp_id,
       i.event_time,
       i.user_id,
       i.content_id,
       max(v.event_time) AS click_time
FROM workspace.emily_imp_df i
LEFT JOIN workspace.emily_view_temp v ON v.user_id = i.user_id
AND v.event_time < i.event_time
GROUP BY 1,
         2,
         3,
         4;


DROP TABLE IF EXISTS workspace.emily_imp_prev;


CREATE TABLE workspace.emily_imp_prev AS
SELECT p.imp_id,
       p.event_time,
       p.user_id,
       p.content_id,
       v.imp_id AS prev_click_imp_id,
       v.event_time AS prev_click_event_time,
       v.content_id AS prev_click_content_id
FROM workspace.emily_prev_temp p
JOIN workspace.emily_view_temp v ON p.click_time = v.event_time
AND p.user_id = v.user_id
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7;



SELECT 
--c.imp_id,
--	   p.name AS product_name,
--       p.keyword AS product_tags,
--       a.keyword AS bid_keyword,
--       a.bid_price AS bid_keyword_price,
       c.ref_term AS search_term,
       c.ad_set_id,
       c.bid_price,
       c.content_id,
       c.content_position,
       c.ranking
FROM bun_log_db.api_event_type_click_ad c
JOIN service1_quicket.ad_set_product_bid_keyword a ON c.ad_set_product_id = a.ad_set_product_id
JOIN service1_quicket.product_info p ON c.content_id = p.id
WHERE c.year||c.month||c.day||c.hour = '2021020202'
  AND c.ref_source = 'sa'
GROUP BY 1, 2, 3, 4, 5, 6
--ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
;

SELECT imp_id, content_id, ref_term, ref_source
FROM bun_log_db.app_event_type_impression
WHERE YEAR||MONTH||DAY = '20201203'
  AND page_id = '검색결과'
  AND ref_term IS NOT NULL
  AND ref_term != '';

select * from stv_recents where status = 'Running'
;

select ad_set_id, ad_set_product_id, count(*)
from bun_log_db.api_event_type_click_ad
where year||'-'||month||'-'||day between {} and '2021-01-31' and ref_source = 'sa'
and ad_set_id in ({});

select ad_set_id, ad_set_product_id, count(*)
from bun_log_db.api_event_type_impression_ad
where year||'-'||month||'-'||day between {} and '2021-01-31' and ref_source = 'sa' and page_id = '검색결과'
and ad_set_id in ({});




--------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS workspace.search_term_log_modified;


CREATE TABLE workspace.search_term_log_modified AS
SELECT user_id,
       session_id,
       lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       LAG(keyword,
           1) OVER (PARTITION BY user_id
                    ORDER BY server_time_kst) AS prev_keyword,
                   LEAD(keyword,
                        1) OVER (PARTITION BY user_id
                                 ORDER BY server_time_kst) AS next_keyword,
                                REPLACE(search_term,
                                        ' ',
                                        '') AS keyword_no_space,
                                REPLACE(prev_keyword,
                                        ' ',
                                        '') AS prev_keyword_no_space,
                                REPLACE(next_keyword,
                                        ' ',
                                        '') AS next_keyword_no_space,
                                server_time_kst::TIMESTAMP AS new_server_time,
                                lag(server_time_kst,
                                    1) OVER (PARTITION BY user_id
                                             ORDER BY server_time_kst)::TIMESTAMP AS prev_server_time,
                                            lead(server_time_kst,
                                                 1) OVER (PARTITION BY user_id
                                                          ORDER BY server_time_kst)::TIMESTAMP AS next_server_time,
                                                         datediff(SECOND,
                                                                  prev_server_time,
                                                                  new_server_time) AS cur_prev_duration,
                                                         datediff(SECOND,
                                                                  prev_server_time,
                                                                  next_server_time) AS prev_next_duration
FROM bun_log_db.app_event_type_search
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND search_term IS NOT NULL
  AND search_term != ''
  AND search_term != ' '
  AND user_id > 0
  AND device_type IN ('a',
                      'i');
                      
                      
                      
DROP TABLE IF EXISTS workspace.keyword_ctr;


CREATE TABLE workspace.keyword_ctr AS
SELECT i.ref_term AS keyword,
       COUNT(i.imp_id) AS imp_count,
       count(v.imp_id) AS view_count,
       view_count / imp_count::float AS ctr
FROM
  (SELECT lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS ref_term,
          imp_id
   FROM bun_log_db.app_event_type_impression
   WHERE YEAR || MONTH || DAY = to_char(CURRENT_DATE - INTERVAL '1 day', 'yyyymmdd')
     AND ref_term IS NOT NULL
     AND ref_term != ''
     AND ref_term != ' '
     AND imp_id IS NOT NULL
     AND device_type IN ('a',
                         'i')) i
LEFT JOIN
  (SELECT lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS ref_term,
          imp_id
   FROM bun_log_db.app_event_type_view
   WHERE YEAR || MONTH || DAY = to_char(CURRENT_DATE - INTERVAL '1 day', 'yyyymmdd')
     AND ref_term IS NOT NULL
     AND ref_term != ''
     AND ref_term != ' '
     AND imp_id IS NOT NULL
     AND device_type IN ('a',
                         'i')) v ON i.imp_id = v.imp_id
GROUP BY 1;

DROP TABLE IF EXISTS workspace.keyword_view_count;


CREATE TABLE workspace.keyword_view_count AS
SELECT lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       COUNT(*) AS view_count
FROM bun_log_db.app_event_type_view v
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND event_action = 'view_content'
  AND content_type = 'product'
  AND v.ref_term IS NOT NULL
  AND v.ref_term != ''
  AND v.ref_term != ' '
  AND device_type IN ('a',
                      'i')
GROUP BY 1;


DROP TABLE IF EXISTS workspace.search_results_count;


CREATE TABLE workspace.search_results_count AS
SELECT lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       MAX((search_results_count)::int) AS results_count
FROM bun_log_db.api_event_type_search
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND device_type IN ('a',
                      'i')
GROUP BY 1;

DROP TABLE IF EXISTS workspace.keyword_search_count;


CREATE TABLE workspace.keyword_search_count AS
SELECT keyword,
       count(*) AS search_count
FROM workspace.search_term_log_modified
GROUP BY 1;


DROP TABLE IF EXISTS workspace.related_keyword_prev_cur;


CREATE TABLE workspace.related_keyword_prev_cur AS
SELECT prev_keyword AS keyword,
       keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 600
  AND cur_prev_duration >= 1
  AND keyword_no_space != prev_keyword_no_space
  AND keyword NOT LIKE '%삽니다%'
  AND keyword != '무료나눔'
  AND keyword != '무나'
  AND prev_keyword NOT LIKE '%' + keyword + '%'
GROUP BY 1,
         2;

DROP TABLE IF EXISTS workspace.related_keyword_prev_next;


CREATE TABLE workspace.related_keyword_prev_next AS
SELECT prev_keyword AS keyword,
       next_keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 300
  AND cur_prev_duration >= 1
  AND next_keyword_no_space != prev_keyword_no_space
  AND next_keyword NOT LIKE '%삽니다%'
  AND next_keyword != '무료나눔'
  AND next_keyword != '무나'
  AND prev_keyword NOT LIKE '%' + next_keyword + '%'
GROUP BY 1,
         2;

DROP TABLE IF EXISTS workspace.related_keyword_combined;


CREATE TABLE workspace.related_keyword_combined AS
SELECT *
FROM
  (SELECT c.keyword,
          c.related_keyword,
          c.count + n.count AS related_count,

     (SELECT COUNT(*)
      FROM search_term_log_modified) AS total_search_count,
          sc.search_count,
          rsc.search_count AS related_search_count,
          related_count / total_search_count::float AS support,
          related_count / sc.search_count::float AS confidence,
          confidence / (related_search_count / total_search_count::float) AS lift
   FROM workspace.related_keyword_prev_cur c
   JOIN workspace.related_keyword_prev_next n ON c.keyword = n.keyword
   AND c.related_keyword = n.related_keyword
   JOIN workspace.keyword_search_count sc ON c.keyword = sc.keyword
   JOIN workspace.keyword_search_count rsc ON c.related_keyword = rsc.keyword)
WHERE related_count > 2;


DROP TABLE IF EXISTS workspace.related_keyword_df_temp;


CREATE TABLE workspace.related_keyword_df_temp AS
SELECT k.keyword,
       k.related_keyword,
       k.related_count,
       k.search_count,
       k.related_search_count,
       rc.results_count,
       v.view_count AS related_view_count,
       k.total_search_count,
       k.support,
       k.confidence,
       k.lift,
       c.ctr
FROM workspace.related_keyword_combined k
JOIN workspace.search_results_count rc ON k.related_keyword = rc.keyword
JOIN workspace.keyword_view_count v ON k.related_keyword = v.keyword
JOIN workspace.keyword_ctr c ON k.related_keyword = c.keyword
WHERE results_count > 1
ORDER BY 4 DESC,
         1,
         10 DESC;
         
         

DROP TABLE IF EXISTS workspace.search_term_log_modified;


CREATE TABLE workspace.search_term_log_modified AS
SELECT user_id,
       session_id,
       lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       LAG(keyword,
           1) OVER (PARTITION BY user_id
                    ORDER BY server_time_kst) AS prev_keyword,
       LEAD(keyword,
            1) OVER (PARTITION BY user_id
                     ORDER BY server_time_kst) AS next_keyword,
        REPLACE(search_term,
                ' ',
                '') AS keyword_no_space,
        REPLACE(prev_keyword,
                ' ',
                '') AS prev_keyword_no_space,
        REPLACE(next_keyword,
                ' ',
                '') AS next_keyword_no_space,
        server_time_kst :: TIMESTAMP AS new_server_time,
        lag(server_time_kst,
            1) OVER (PARTITION BY user_id
                     ORDER BY server_time_kst):: TIMESTAMP AS prev_server_time,
        lead(server_time_kst,
             1) OVER (PARTITION BY user_id
                      ORDER BY server_time_kst):: TIMESTAMP AS next_server_time,
         datediff(SECOND,
                  prev_server_time,
                  new_server_time) AS cur_prev_duration,
         datediff(SECOND,
                  prev_server_time,
                  next_server_time) AS prev_next_duration
FROM bun_log_db.app_event_type_search
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND search_term IS NOT NULL
  AND search_term != ''
  AND search_term != ' '
  AND user_id > 0
  AND device_type IN ('a',
                      'i');



DROP TABLE IF EXISTS workspace.keyword_view_count;


CREATE TABLE workspace.keyword_view_count AS
SELECT lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       COUNT(*) AS view_count
FROM bun_log_db.app_event_type_view v
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND event_action = 'view_content'
  AND content_type = 'product'
  AND v.ref_term IS NOT NULL
  AND v.ref_term != ''
  AND v.ref_term != ' '
  AND device_type IN ('a',
                      'i')
GROUP BY 1;


DROP TABLE IF EXISTS workspace.search_results_count;


CREATE TABLE workspace.search_results_count AS
SELECT lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       MAX((search_results_count):: int) AS results_count
FROM bun_log_db.api_event_type_search
WHERE YEAR || MONTH || DAY >= to_char(CURRENT_DATE - INTERVAL '5 day',
                                                              'yyyymmdd')
  AND YEAR || MONTH || DAY < to_char(CURRENT_DATE,
                                     'yyyymmdd')
  AND device_type IN ('a',
                      'i')
GROUP BY 1;


DROP TABLE IF EXISTS workspace.keyword_search_count;


CREATE TABLE workspace.keyword_search_count AS
SELECT keyword,
       count(*) AS search_count
FROM workspace.search_term_log_modified
GROUP BY 1;


DROP TABLE IF EXISTS workspace.related_keyword_prev_cur;


CREATE TABLE workspace.related_keyword_prev_cur AS
SELECT prev_keyword AS keyword,
       keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 600
  AND cur_prev_duration >= 1
  AND keyword_no_space != prev_keyword_no_space
GROUP BY 1,
         2;


DROP TABLE IF EXISTS workspace.related_keyword_prev_next;


CREATE TABLE workspace.related_keyword_prev_next AS
SELECT prev_keyword AS keyword,
       next_keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 300
  AND cur_prev_duration >= 1
  AND next_keyword_no_space != prev_keyword_no_space
GROUP BY 1,
         2;


DROP TABLE IF EXISTS workspace.related_keyword_combined;


CREATE TABLE workspace.related_keyword_combined AS
SELECT *
FROM
  (SELECT c.keyword,
          c.related_keyword,
          c.count + n.count AS related_count,

     (SELECT COUNT(*)
      FROM search_term_log_modified) AS total_search_count,
          sc.search_count,
          rsc.search_count AS related_search_count,
          related_count / total_search_count :: float AS support,
          related_count / sc.search_count :: float AS confidence,
          confidence / (rsc.search_count / total_search_count :: float) AS lift
   FROM workspace.related_keyword_prev_cur c
   JOIN workspace.related_keyword_prev_next n ON c.keyword = n.keyword
   AND c.related_keyword = n.related_keyword
   JOIN workspace.keyword_search_count sc ON c.keyword = sc.keyword
   JOIN workspace.keyword_search_count rsc ON c.related_keyword = rsc.keyword)
WHERE related_count > 2;


DROP TABLE IF EXISTS workspace.related_keyword_df_temp;


CREATE TABLE workspace.related_keyword_df_temp AS
SELECT k.keyword,
       k.related_keyword,
       k.related_count,
       k.search_count,
       k.related_search_count,
       rc.results_count,
       v.view_count AS related_view_count,
       k.total_search_count,
       k.support,
       k.confidence,
       k.lift
FROM workspace.related_keyword_combined k
JOIN workspace.search_results_count rc ON k.related_keyword = rc.keyword
JOIN workspace.keyword_view_count v ON k.related_keyword = v.keyword
WHERE results_count > 1
ORDER BY 4 DESC,
         1,
         10 DESC;



DROP TABLE IF EXISTS workspace.related_keyword_df_filtered;


CREATE TABLE workspace.related_keyword_df_filtered AS
SELECT *
FROM workspace.related_keyword_df_temp
WHERE confidence > 1/pow(search_count, {n})
  AND lift > {min_lift};


DROP TABLE IF EXISTS workspace.related_keyword_df_max;


CREATE TABLE workspace.related_keyword_df_max AS
SELECT keyword,
       max(confidence) AS max_confidence,
       max(lift) AS max_lift
FROM workspace.related_keyword_df_filtered
GROUP BY 1;


DROP TABLE IF EXISTS workspace.related_keyword_df;


CREATE TABLE workspace.related_keyword_df AS
SELECT a.*,
       ({confidence_weight} * a.confidence / b.max_confidence::float) + 
       ({lift_weight}*a.lift/b.max_lift::float) AS score
FROM workspace.related_keyword_df_filtered a
JOIN workspace.related_keyword_df_max b ON a.keyword = b.keyword
ORDER BY a.keyword,
         score DESC;
         
WITH imp AS
  (SELECT content_id,
          lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS ref_term,
          count(*) imp
   FROM bun_log_db.api_event_type_impression_ad
   WHERE YEAR || MONTH || DAY = '20210223'
     AND ref_source = 'sa'
     AND report = 'true'
   GROUP BY content_id,
            ref_term),
     clk AS
  (SELECT content_id,
          lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS ref_term,
          count(*) clk
   FROM bun_log_db.api_event_type_click_ad clk
   WHERE YEAR || MONTH || DAY = '20210223'
     AND ref_source = 'sa'
     AND report = 'true'
   GROUP BY content_id,
            ref_term)
SELECT imp.content_id,
       imp.ref_term,
       imp.imp,
       CASE
           WHEN clk.clk IS NULL THEN 0
           ELSE clk.clk
       END clk,
       CASE
           WHEN clk.clk IS NULL THEN 0
           ELSE clk.clk
       END / cast(imp.imp AS decimal(10, 4)) ctr
FROM imp
LEFT JOIN clk ON clk.ref_term = imp.ref_term
AND clk.content_id = imp.content_id;



DROP TABLE IF EXISTS workspace.golf_category;


CREATE TABLE workspace.golf_category AS
SELECT id,
       name,
       keyword
FROM service1_quicket.product_info
WHERE category_id = 700600400
  AND status IN (0,
                 1)
  AND name NOT LIKE '%남성%'
       AND keyword NOT LIKE '%남성%'
       AND name NOT LIKE '%남자%'
       AND keyword NOT LIKE '%남자%'
       AND name NOT LIKE '%여성%'
       AND keyword NOT LIKE '%여성%'
       AND name NOT LIKE '%여자%'
       AND keyword NOT LIKE '%여자%'
;





DROP TABLE IF EXISTS workspace.keyword_label;


CREATE TABLE workspace.keyword_label AS
SELECT keyword,
       row_number() OVER () - 1 AS label
FROM
  (SELECT DISTINCT keyword
   FROM workspace.related_keyword_df)
ORDER BY 1;


DROP TABLE IF EXISTS workspace.related_keyword_label;


CREATE TABLE workspace.related_keyword_label AS
SELECT related_keyword,
       row_number() OVER () - 1 AS label
FROM
  (SELECT DISTINCT related_keyword
   FROM workspace.related_keyword_df)
ORDER BY 1;


DROP TABLE IF EXISTS workspace.libsvm_temp;


CREATE TABLE workspace.libsvm_temp AS
SELECT a.keyword,
       a.related_keyword,
       a.score,
       k.label AS keyword_label,
       r.label AS related_keyword_label,

  (SELECT max(label)
   FROM workspace.keyword_label) AS max_keyword_label,

  (SELECT max(label)
   FROM workspace.related_keyword_label) AS max_related_keyword_label
FROM workspace.related_keyword_df a
JOIN workspace.keyword_label k ON a.keyword = k.keyword
JOIN workspace.related_keyword_label r ON a.related_keyword = r.related_keyword;

DROP TABLE IF EXISTS workspace.libsvm_data;

CREATE TABLE workspace.libsvm_data AS
SELECT cast(score*1000 as int)||' '|| keyword_label||':1'||' '||related_keyword_label + max_keyword_label||':1'
FROM workspace.libsvm_temp;



--------------------------------------------------------
--------------------------------------------------------

DROP TABLE IF EXISTS workspace.rk1;


CREATE TABLE workspace.rk1 AS
SELECT user_id,
       search_term,
       lag(search_term,
           1) OVER (PARTITION BY user_id
                    ORDER BY server_time_kst) AS prev_search_term,
       lead(search_term,
            1) OVER (PARTITION BY user_id
                     ORDER BY server_time_kst) AS next_search_term
FROM bun_log_db.app_event_type_search
WHERE YEAR||MONTH||DAY >= to_char(CURRENT_DATE - interval '6 day',
                                                          'yyyymmdd')
  AND YEAR||MONTH||DAY < to_char(CURRENT_DATE - interval '1 day',
                                                         'yyyymmdd');


DROP TABLE IF EXISTS workspace.rk2;


CREATE TABLE workspace.rk2 AS
SELECT lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       lower(trim(regexp_replace(regexp_replace(prev_search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS prev_keyword,
       lower(trim(regexp_replace(regexp_replace(next_search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS next_keyword,
       count(*) AS cnt
FROM workspace.rk1
GROUP BY 1,
         2,
         3;
         
DROP TABLE IF EXISTS workspace.rk3;


CREATE TABLE workspace.rk3 AS
SELECT *
FROM workspace.rk2
WHERE keyword != prev_keyword
  OR keyword != next_keyword;  


DROP TABLE IF EXISTS workspace.keyword_label;


CREATE TABLE workspace.keyword_label AS
SELECT keyword,
       row_number() OVER () - 1 AS label
FROM
  (SELECT DISTINCT keyword
   FROM workspace.rk3)
ORDER BY 1;


DROP TABLE IF EXISTS workspace.prev_keyword_label;


CREATE TABLE workspace.prev_keyword_label AS
SELECT prev_keyword,
       row_number() OVER () - 1 AS label
FROM
  (SELECT DISTINCT prev_keyword
   FROM workspace.rk3)
ORDER BY 1;

DROP TABLE IF EXISTS workspace.next_keyword_label;


CREATE TABLE workspace.next_keyword_label AS
SELECT next_keyword,
       row_number() OVER () - 1 AS label
FROM
  (SELECT DISTINCT next_keyword
   FROM workspace.rk3)
ORDER BY 1;


DROP TABLE IF EXISTS workspace.libsvm_temp;


CREATE TABLE workspace.libsvm_temp AS
SELECT a.cnt,
	   a.keyword,
       a.prev_keyword,
       a.next_keyword,
       k.label AS keyword_label,
       p.label AS prev_keyword_label,
       n.label AS next_keyword_label,

  (SELECT max(label)
   FROM workspace.keyword_label) AS max_keyword_label,

  (SELECT max(label)
   FROM workspace.prev_keyword_label) AS max_prev_keyword_label,
   
   (SELECT max(label)
   FROM workspace.next_keyword_label) AS max_next_keyword_label
FROM workspace.rk3 a
JOIN workspace.keyword_label k ON a.keyword = k.keyword
JOIN workspace.prev_keyword_label p ON a.prev_keyword = p.prev_keyword
JOIN workspace.next_keyword_label n ON a.next_keyword = n.next_keyword;

DROP TABLE IF EXISTS workspace.libsvm_data;

CREATE TABLE workspace.libsvm_data AS
SELECT cnt||' '||keyword_label||':1'||' '||max_keyword_label+prev_keyword_label||':1'||' '||max_keyword_label+max_prev_keyword_label+next_keyword_label||':1'
FROM workspace.libsvm_temp;





select id, name, keyword,
case 
when name like '%베드민턴%' or keyword like '%베드민턴%' 
or name like '%테니스%' or keyword like '%테니스%'
or name like '%볼링%' or keyword like '%볼링%'
or name like '%탁구%' or keyword like '%탁구%'
or name like '%당구%' or keyword like '%당구%' then 1
else 0 
end as label
from service1_quicket.product_info 
where category_id IN (700100999, 700950010, 700950999, 700950020) and status in (0, 1)
 ;
 
----------------------------

DROP TABLE IF EXISTS workspace.search_term_log_modified;
CREATE TABLE workspace.search_term_log_modified AS (
    SELECT user_id,                                                                       --uid
           session_id,                                                                    --세션 id
           lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' ')))    AS keyword,               --검색어
           LAG(keyword, 1)
           OVER ( PARTITION BY user_id
               ORDER BY server_time_kst)                        AS prev_keyword,          --이전 검색어
           LEAD(keyword, 1)
           OVER ( PARTITION BY user_id
               ORDER BY server_time_kst)                        AS next_keyword,          --다음 검색어
           REPLACE(search_term, ' ', '')                        AS keyword_no_space,      --검색어 띄어쓰기 없애기
           REPLACE(prev_keyword, ' ', '')                       AS prev_keyword_no_space, --이전 검색어 띄어쓰기 없애기
           REPLACE(next_keyword, ' ', '')                       AS next_keyword_no_space, --다음 검색어 띄어쓰기 없애기
           server_time_kst::TIMESTAMP                           AS new_server_time,       --검색 시간
           LAG(server_time_kst, 1) OVER (
               PARTITION BY user_id
               ORDER BY server_time_kst)::TIMESTAMP             AS prev_server_time,      --이전 검색어 검색 시간
           LEAD(server_time_kst, 1) OVER (
               PARTITION BY user_id
               ORDER BY server_time_kst)::TIMESTAMP             AS next_server_time,      --다음 검색어 검색 시간
           datediff(SECOND, prev_server_time, new_server_time)  AS cur_prev_duration,     --검색 시간 차 (현재-이전)
           datediff(SECOND, prev_server_time, next_server_time) AS prev_next_duration     --검색 시간 차 (다음-이전)
    FROM bun_log_db.app_event_type_search
    WHERE year || month || day >= '20210305'
      AND year || month || day < '20210310'
      AND search_term IS NOT NULL
      AND search_term != ''
      AND search_term != ' '
      AND user_id > 0
      AND device_type IN ('a', 'i')
);



DROP TABLE IF EXISTS workspace.keyword_view_count;
CREATE TABLE workspace.keyword_view_count AS
SELECT lower(trim(regexp_replace(regexp_replace(ref_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       COUNT(*)                                                                           AS view_count
FROM bun_log_db.app_event_type_view v
WHERE year || month || day >= '20210305'
  AND year || month || day < '20210310'
  AND event_action = 'view_content'
  AND content_type = 'product'
  AND v.ref_term IS NOT NULL
  AND v.ref_term != ''
  AND v.ref_term != ' '
  AND device_type IN ('a', 'i')
GROUP BY 1;


DROP TABLE IF EXISTS workspace.search_results_count;
CREATE TABLE workspace.search_results_count AS
SELECT lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS keyword,
       MAX((search_results_count):: int)                                                     AS results_count
FROM bun_log_db.api_event_type_search
WHERE year || month || day >= '20210305'
  AND year || month || day < '20210310'
  AND device_type IN ('a', 'i')
GROUP BY 1;


DROP TABLE IF EXISTS workspace.keyword_search_count;
CREATE TABLE workspace.keyword_search_count AS
SELECT keyword,
       count(*) AS search_count
FROM workspace.search_term_log_modified
GROUP BY 1;


DROP TABLE IF EXISTS workspace.related_keyword_prev_cur;
CREATE TABLE workspace.related_keyword_prev_cur AS
SELECT prev_keyword AS keyword,
       keyword      AS related_keyword,
       COUNT(*)     AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 600
  AND cur_prev_duration >= 1
  AND keyword_no_space != prev_keyword_no_space
GROUP BY 1, 2;


DROP TABLE IF EXISTS workspace.related_keyword_prev_next;
CREATE TABLE workspace.related_keyword_prev_next AS
SELECT prev_keyword AS keyword,
       next_keyword AS related_keyword,
       COUNT(*)     AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND prev_next_duration <= 300
  AND prev_next_duration >= 1
  AND next_keyword_no_space != prev_keyword_no_space
GROUP BY 1, 2;


DROP TABLE IF EXISTS workspace.related_keyword_combined;
CREATE TABLE workspace.related_keyword_combined AS
SELECT *
FROM (SELECT c.keyword,
             c.related_keyword,
             c.count + n.count                                             AS related_count,

             (SELECT COUNT(*)
              FROM search_term_log_modified)                          AS total_search_count,
             sc.search_count,
             rsc.search_count                                              AS related_search_count,
             related_count / total_search_count :: float                   AS support,
             related_count / sc.search_count :: float                      AS confidence,
             confidence / (rsc.search_count / total_search_count :: float) AS lift
      FROM workspace.related_keyword_prev_cur c
               JOIN workspace.related_keyword_prev_next n ON c.keyword = n.keyword
          AND c.related_keyword = n.related_keyword
               JOIN workspace.keyword_search_count sc ON c.keyword = sc.keyword
               JOIN workspace.keyword_search_count rsc ON c.related_keyword = rsc.keyword) a
WHERE related_count > 2;


DROP TABLE IF EXISTS workspace.related_keyword_df_temp;
CREATE TABLE workspace.related_keyword_df_temp AS
SELECT k.keyword,
       k.related_keyword,
       k.related_count,
       k.search_count,
       k.related_search_count,
       rc.results_count,
       v.view_count AS related_view_count,
       k.total_search_count,
       k.support,
       k.confidence,
       k.lift
FROM workspace.related_keyword_combined k
         JOIN workspace.search_results_count rc ON k.related_keyword = rc.keyword
         JOIN workspace.keyword_view_count v ON k.related_keyword = v.keyword
WHERE results_count > 1
ORDER BY 4 DESC, 1, 10 DESC;


DROP TABLE IF EXISTS workspace.related_keyword_df_filtered;
CREATE TABLE workspace.related_keyword_df_filtered AS
SELECT *
FROM workspace.related_keyword_df_temp
WHERE confidence > 1 / pow(search_count, 0.6)
  AND lift > 1;


DROP TABLE IF EXISTS workspace.related_keyword_df_max;
CREATE TABLE workspace.related_keyword_df_max AS
SELECT keyword,
       max(confidence) AS max_confidence,
       max(lift)       AS max_lift
FROM workspace.related_keyword_df_filtered
GROUP BY 1;

DROP TABLE IF EXISTS workspace.related_keyword_df;


CREATE TABLE workspace.related_keyword_df AS
SELECT a.keyword,
       a.related_keyword,
       a.related_count,
       a.search_count,
       ROUND((0.7 * a.confidence / b.max_confidence:: float) + (0.3 * a.lift / b.max_lift:: float),
             4) AS score
FROM workspace.related_keyword_df_filtered a
JOIN workspace.related_keyword_df_max b ON a.keyword = b.keyword
ORDER BY a.search_count DESC,
         a.keyword,
         score DESC;
         

grant select on workspace.keyword_view_count to PUBLIC;