UPDATE public.firefly_account_connection
SET connection_name = 'biconomy',
    action = 'update',
    update_time = CURRENT_TIMESTAMP
WHERE 
    account_id = '17847' AND
    connection_id = '1140637286637473792';