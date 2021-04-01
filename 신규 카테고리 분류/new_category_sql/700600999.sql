SELECT id AS pid,
       new_category_id,
       name
FROM
  (SELECT id,
          name,
          replace(name, ' ', '') AS name_strip,
          CASE
              WHEN keyword LIKE '%골프%'
                   OR keyword LIKE '%드라이버%'
                   OR keyword LIKE '%드라이브%'
                   OR keyword LIKE '%우드%'
                   OR keyword LIKE '%유틸%'
                   OR keyword LIKE '%하이브리드%'
                   OR keyword LIKE '%아이언%'
                   OR keyword LIKE '%웨지%'
                   OR keyword LIKE '%퍼터%'
                   OR keyword LIKE '%샤프트%'
                   OR keyword LIKE '%헤드%'
                   OR name_strip LIKE '%골프%'
                   OR name_strip LIKE '%드라이버%'
                   OR name_strip LIKE '%드라이브%'
                   OR name_strip LIKE '%우드%'
                   OR name_strip LIKE '%유틸%'
                   OR name_strip LIKE '%하이브리드%'
                   OR name_strip LIKE '%아이언%'
                   OR name_strip LIKE '%웨지%'
                   OR name_strip LIKE '%퍼터%'
                   OR name_strip LIKE '%샤프트%'
                   OR name_strip LIKE '%헤드%' THEN '700600900'
              ELSE '700600999'
          END AS new_category_id
   FROM service1_quicket.product_info
   WHERE category_id = 700600999
     AND status = 0)