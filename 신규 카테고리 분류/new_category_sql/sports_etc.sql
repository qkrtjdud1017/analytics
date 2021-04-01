SELECT id AS pid,
       CASE
           WHEN name LIKE '%베드민턴%'
                OR name LIKE '%배드민턴%'
                OR keyword LIKE '%베드민턴%'
                OR keyword LIKE '%배드민턴%' THEN '700140'
           WHEN name LIKE '%테니스%'
                OR name LIKE '%태니스%'
                OR keyword LIKE '%테니스%'
                OR keyword LIKE '%태니스%' THEN '700150'
           WHEN name LIKE '%볼링%'
                OR keyword LIKE '%볼링%' THEN '700160'
           WHEN name LIKE '%탁구%'
                OR keyword LIKE '%탁구%' THEN '700170'
           WHEN name LIKE '%당구%'
                OR keyword LIKE '%당구%' THEN '700180'
           ELSE '700950010'
       END AS new_category_id,
       name
FROM service1_quicket.product_info
WHERE category_id IN (700100999,
                      700950010,
                      700950999,
                      700950020)
  AND status = 0