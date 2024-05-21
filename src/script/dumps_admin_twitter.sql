psql --host=data-process-rds.ccym4z6uvp7m.ap-east-1.rds.amazonaws.com --port=5432 --username=postgres --dbname=nextid -c "COPY (select * from firefly_account_connection where connection_platform='twitter' and data_source='admin' and connection_name='') TO STDOUT WITH CSV HEADER" > /Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/data/admin_twitter/admin/admin.csv


UPDATE public.firefly_account_connection
SET connection_name = 'biconomy',
    action = 'update',
    update_time = CURRENT_TIMESTAMP
WHERE 
    account_id = '17847' AND
    connection_id = '1140637286637473792';