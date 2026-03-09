
  
    

  create  table "ny_taxi_db"."gold"."dim_service_type__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    ROW_NUMBER() OVER (ORDER BY service_type)::INT  AS service_type_key,
    service_type,
    CASE service_type
        WHEN 'yellow' THEN 'Yellow Medallion Taxi'
        WHEN 'green'  THEN 'Green Boro Taxi'
    END AS description
FROM (VALUES ('yellow'), ('green')) AS t(service_type)
  );
  