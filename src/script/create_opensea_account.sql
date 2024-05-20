CREATE TABLE public.opensea_account (
    id                    SERIAL PRIMARY KEY,
    address               VARCHAR(255) NOT NULL DEFAULT ''::character varying,
    twitter_username      VARCHAR(255) NOT NULL DEFAULT ''::character varying,
    instagram_username    VARCHAR(255) NOT NULL DEFAULT ''::character varying,
    twitter_is_verified   INTEGER NOT NULL DEFAULT 0,
    instagram_is_verified INTEGER NOT NULL DEFAULT 0,
    is_pick               INTEGER NOT NULL DEFAULT 0,
    updated_at            TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at            TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    twitter_id            VARCHAR(64) NOT NULL DEFAULT ''::character varying,
    instagram_id          VARCHAR(64) NOT NULL DEFAULT ''::character varying,
    CONSTRAINT opensea_account_address_uk UNIQUE (address)
);

-- Creating indexes
CREATE INDEX opensea_account_ins_handle_idx ON public.opensea_account (instagram_username);
CREATE INDEX opensea_account_ins_verified_idx ON public.opensea_account (instagram_is_verified);
CREATE INDEX opensea_account_pick_idx ON public.opensea_account (is_pick);
CREATE INDEX opensea_account_twitter_handle_idx ON public.opensea_account (twitter_username);
CREATE INDEX opensea_account_twitter_verified_idx ON public.opensea_account (twitter_is_verified);


-- Loading data
psql --host=data-process-rds.ccym4z6uvp7m.ap-east-1.rds.amazonaws.com --port=5432 --username=postgres --dbname=nextid -c "\copy public.opensea_account (id, address, twitter_username, instagram_username, twitter_is_verified, instagram_is_verified, is_pick, updated_at, created_at, twitter_id, instagram_id) FROM '/Users/fuzezhong/Documents/GitHub/zhongfuze/data_process/data/opensea_account/opensea_account.csv' WITH (FORMAT csv, HEADER);"