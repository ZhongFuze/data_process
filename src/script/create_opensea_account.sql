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
