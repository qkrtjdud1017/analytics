SELECT id AS pid,
       new_category_id,
       name
FROM
  (SELECT id,
          name,
          lower(replace(name, ' ', '')) AS name_strip,
          lower(keyword) AS re_keyword,
          CASE
              WHEN name_strip LIKE '%남성%'
                   OR name_strip LIKE '%남자%'
                   OR name_strip LIKE '%남s%'
                   OR name_strip LIKE '%남m%'
                   OR name_strip LIKE '%남l%' THEN '700600500'
              WHEN name_strip LIKE '%여성%'
                   OR name_strip LIKE '%여자%'
                   OR name_strip LIKE '%여s%'
                   OR name_strip LIKE '%여m%'
                   OR name_strip LIKE '%여l%'
                   OR name_strip LIKE '%스커트%'
                   OR name_strip LIKE '%원피스%'
                   OR name_strip LIKE '%치마%' THEN '700600400'
              WHEN re_keyword LIKE '%남성%'
                   OR re_keyword LIKE '%남자%'
                   OR re_keyword LIKE '%남s%'
                   OR re_keyword LIKE '%남m%'
                   OR re_keyword LIKE '%남l%' THEN '700600500'
              WHEN re_keyword LIKE '%여성%'
                   OR re_keyword LIKE '%여자%'
                   OR re_keyword LIKE '%여s%'
                   OR re_keyword LIKE '%여m%'
                   OR re_keyword LIKE '%여l%'
                   OR re_keyword LIKE '%스커트%'
                   OR re_keyword LIKE '%원피스%'
                   OR re_keyword LIKE '%치마%' THEN '700600400'
              ELSE '700600900'
          END AS new_category_id
   FROM service1_quicket.product_info
   WHERE category_id = 700600400
     AND status = 0)