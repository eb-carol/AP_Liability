----- declaring beginning balances



NET SALES
------- use tiny tim for non-transfer related orders

drop table hive.team_data_insights.tiny_tim_net_sales_through_2018_dec_no_transfers;
  create table hive.team_data_insights.tiny_tim_net_sales_through_2018_dec_no_transfers
  as
  select * from (
  select
   coalesce(oi.event_id, ev.event_id) event_id
  , e.current_status
  , e.uid as org_uid
  , u.account_type
  , coalesce(oi.currency, ev.currency) as currency
  , e.end_date

  , - sum(refund__ap_organizer__gts__epp	+refund__ap_organizer__org_to_att_tax__epp+
      refund__ap_organizer__royalty__epp+	refund__ap_organizer__transfer__epp+
      sale__ap_organizer__gts__epp	+sale__ap_organizer__org_to_att_tax__epp	+
      sale__ap_organizer__royalty__epp	+sale__ap_organizer__transfer__epp)/100.00
      as epp_net_sales
  , - sum(- sale__gtf_esf__offline /*gtf offline*/ - sale__eb_tax__offline /*offline sales eb tax*/ -- DEDUCTABLES: subtract offline gtf and eb tax
      - refund__gtf_esf__offline /*gtf offline*/ - refund__eb_tax__offline /*eb tax offline */)/100.00
      as offline_net_sales

  from hive.df_financial.order_itemizations oi
  left join (select distinct id order_id, event event_id, currency from hive.eb.orders group by 1,2,3) ev on oi.order_id = ev.order_id
  left join hive.eb.events e on coalesce(oi.event_id, ev.event_id) = e.id
  left join hive.eb.users u on u.id = e.uid

  where
  transaction_created >= DATE '2018-01-01' ---- do not change.
  and transaction_created < DATE '2019-01-01' ---- change.
  and coalesce(oi.event_id, ev.event_id) not in (select * from hive.team_data_insights.creator_event_exclusions)
  and coalesce(oi.event_id, ev.event_id) not in (select event_id from hive.team_data_insights.creator_event_level_through_11_30_2018 group by 1 having count(*) > 1)
  and oi.order_id not in (select order_id from hive.team_data_insights.ap_org_transfer_orders)
  group by 1,2,3,4,5,6
);


--- rewrite creator balance due to include transfer orders from eb_history (includes EB/EBHistory up to 2017 and all transfers)
drop table hive.team_data_insights.creator_balance_due_with_transfers_a_2018_dec;
  CREATE TABLE hive.team_data_insights.creator_balance_due_ebh_a_2018_dec
  as
  select
    o.event_id,
    o.current_status,
    o.org_uid,
    o.account_type,
    o.currency,
    o.end_date,
    round(sum(o.epp_net_sales),2) AS epp_net_sales,
    round(sum(o.offline_net_sales),2) AS offline_net_sales
  from (
    SELECT
      base.event_id,
      base.current_status,
      base.uid org_uid,
      base.account_type,
      base.currency,
      base.end_date,
      -sum(case when base.payment_type = 'eventbrite' then base.gross - base.mg_fee else 0 end) epp_net_sales,
      sum(case when base.payment_type = 'offline' then  base.mg_fee else 0 end) offline_net_sales
    FROM
      (
      SELECT order_data.*
      FROM
        (select
          ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY o.changed  DESC, o.log_position desc) rnum,
          o.order_id,
          o.event_id,
          o.gross,
          o.mg_fee,
          o.payment_type,
          o.currency,
          e.uid,
          u.account_type,
          o.changed,
          o.status AS order_status,
          e.start_date,
          e.end_date,
          e.current_status
        from hive.eb_history.History_Orders o
        join hive.eb.Events e on e.id = o.event_id
        left join hive.eb.users u on u.id = e.uid
        where
          cast(substr(cast(o.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= TIMESTAMP '2013-05-13 13:30:06' --- do not change
          and (cast(substr(cast(o.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2018-01-01' --- do not change.
          or o.order_id in (select order_id from hive.team_data_insights.ap_org_transfer_orders))
          and o.payment_type in ('eventbrite','offline')
          and o.status IN (100,200,220,400,350,300,250)

        ) order_data
      WHERE rnum = 1 and order_status = 100
      ) base
    group by 1,2,3,4,5,6
  union (
    SELECT
      o.event AS event_id,
      e.current_status,
      e.uid as org_uid,
      u.account_type,
      o.currency,
      e.end_date,
      -sum(case when o.payment_type = 'eventbrite' then o.gross - o.mg_fee else 0 end) epp_net_sales,
      sum(case when o.payment_type = 'offline' then  o.mg_fee else 0 end) offline_net_sales
      -- sum(IF(o.payment_type = 'eventbrite',(o.gross-o.mg_fee),-o.mg_fee)) net_sales
    FROM hive.eb.Orders o
    join hive.eb.Events e on e.id = o.event
    left join hive.eb.users u on u.id = e.uid
    WHERE
      status = 100 AND
      cast(substr(cast(o.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2018-01-01' AND --- do not change
      o.payment_type in ('eventbrite','offline') AND
      o.id not in (
              SELECT order_data.order_id
              FROM
                (select
                  ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY o.changed DESC) rnum,
                  o.order_id,
                  o.changed,
                  o.status AS order_status
                from hive.eb_history.History_Orders o
                where
                cast(substr(cast(o.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= TIMESTAMP '2013-05-13 13:30:06' --- do not change
                and (cast(substr(cast(o.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2018-01-01' --- do not change.
                or o.order_id in (select order_id from hive.team_data_insights.ap_org_transfer_orders))
                  and o.payment_type in ('eventbrite','offline')
                  and o.status IN (100,200,220,400,350,300,250)
                ) order_data
              WHERE rnum = 1 and order_status = 100
              )
    group by 1,2,3,4,5,6
    )
  ) o
  where epp_net_sales <> 0 or offline_net_sales <> 0
  group by 1,2,3,4,5,6;



---- combine tiny tim 2018 (non transfers), eb/eb history 2017, eb history 2018+ for transfers

drop table hive.team_data_insights.eb_history_through_2017_with_transfers_and_tt_2018_to_2018_dec_net_sales_a;
    create table hive.team_data_insights.creator_balance_due_2018_dec_net_sales_a

    as
    select
    coalesce(t.event_id, a.event_id) event_id,
    -- a.payment_types,
    coalesce(a.account_type,t.account_type) account_type,
    coalesce(a.current_status, t.current_status) current_status,
    coalesce(a.org_uid, t.org_uid) org_uid,
    coalesce(a.currency, t.currency) currency,
    coalesce(a.end_date, t.end_date) end_date,
    coalesce(a.epp_net_sales,0)+coalesce(t.epp_net_sales,0) epp_net_sales,
    coalesce(a.offline_net_sales,0)+coalesce(t.offline_net_sales,0) offline_net_sales
    from hive.team_data_insights.tiny_tim_net_sales_through_2018_dec_no_transfers t
    full join hive.team_data_insights.creator_balance_due_ebh_a_2018_dec a on a.event_id = t.event_id and a.currency = t.currency;




PULL IN PARENT EID

drop table hive.team_data_insights.eb_history_through_2017_with_transfers_and_tt_2018_to_2018_dec_net_sales_aa;
CREATE TABLE hive.team_data_insights.creator_balance_due_2018_dec_net_sales_aa
as
select a.*, IF(e.event_parent is null,id,event_parent) parent
from hive.team_data_insights.creator_balance_due_2018_dec_net_sales_a a
join hive.eb.events e on e.id = a.event_id
;

FINAL NET SALES AGGREGATION (EXCLUDES NON-EPP EVENTS)

drop table hive.team_data_insights.creator_balance_due_2018_dec_net_sales;
CREATE TABLE hive.team_data_insights.creator_balance_due_2018_dec_net_sales
as
select a.event_id,
  a.current_status,
  a.account_type,
  a.end_date,
  a.org_uid,
  a.currency,
  a.epp_net_sales,
  case when (p.accept_eventbrite = 1 or p.accept_eventbrite_old = 1) and (accept_paypal != 1 and accept_authnet != 1)
    then a.offline_net_sales else 0 end as offline_net_sales
from hive.team_data_insights.creator_balance_due_2018_dec_net_sales_aa a
left join (
  select * from (
    select row_number() over (partition by event_id order by changed desc, log_position desc) rnum, *
    from hive.eb_history.history_payment_options where changed < date '2019-01-01')
  where rnum = 1) p on p.event_id = a.parent
;


PAYOUTS TABLE CREATION


---- cutoff dates
    -- and date(created) = date '2018-12-26' and date(trx_date) = date '2018-12-28' --- GBP dec
    --      - there is a .27 amount transaction that is showing as paid out in hubert's but its showing as status deleted in db.
    -- and date(created) = date '2018-12-27' and date(trx_date) = date '2018-12-31'  --- EUR dec
    -- and date(created) = date '2018-12-28' and date(trx_date) = date '2019-01-02'  --- CAD dec
    -- and date(created) = date '2018-12-28' and date(trx_date) = date '2019-01-02'  --- AUD dec
    -- and date(created) = date '2018-12-31' and date(trx_date) = date '2019-01-02'  --- USD dec --- id 7239062, 7239063 , 7239064, 7240183, 7240415  trx date on 1/3 and created on 12/31
-- and date(created) = date '2018-12-28' and date(trx_date) = date '2019-01-02'  --- NZD dec


drop table hive.team_data_insights.scratch_history_payouts_2018_dec_new;
      CREATE TABLE hive.team_data_insights.scratch_history_payouts_2018_dec_new
          as
          select
          payout_id,
          event_id,
          user_id,
          currency,
          country,
          payout_type,
          trx_date,
          created,
          changed,
          trx_id,
          notes,
          account_type,
          reason,
          status,
          status_old,
          amount,
          payment_id,
          withheld_status,
          'history_payout' source
          from (
          select *, row_number() over (partition by payout_id order by changed desc, log_position desc) rnum
    --       from hive.eb.Payouts
          from hive.eb_history.history_payouts
          where
          (payout_type = 'ACH'
            and status not in (0,10,12,1,8)

          and ((cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-01-03' --- change last date.
          and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
              cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-01-03' -- change date.
          and changed < date '2019-01-03' -- change date
          and currency in ('AUD','CAD','NZD','USD','HKD','SGD'))

          or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2018-12-29' --- change last date.
          and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
              cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2018-12-29' -- change date.
          and changed < date '2018-12-29' -- change date
          and currency = 'GBP')

          -- mexico did not exist in dec 2019.
          -- or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-05-01' --- change last date.
          -- and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
          --     cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-05-01' -- change date. --- dummy date
          -- and changed < date '2019-05-01' -- change date
          -- and currency = 'MXN')

          or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-01-01' --- change last date.
          and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
              cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-01-01' -- change date.
          and changed < date '2019-01-01' -- change date
          and currency = 'EUR')

          or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2017-01-01' -- do not change date
          and changed < date '2019-01-01' -- change date
          and currency in ('AUD','CAD','EUR','GBP','NZD','USD','SGD','HKD','MXN'))
        ))

          or (payout_type = 'CHECK'
          and (status != 0 and status_old is not null) and status != 8
          and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
          cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-01-01' -- change date.
          and changed < date '2019-01-01' -- change date
          and currency in ('AUD','CAD','EUR','GBP','NZD','USD','BRL','ARS','MXN','SGD','HKD')
          )

          or (status = 8
          and cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-01-01' -- change date.
          and changed < date '2019-01-01'
          and currency in ('AUD','CAD','EUR','GBP','NZD','USD','BRL','ARS','MXN','SGD','HKD')
        )
      )

          where rnum = 1

        union (

          select
          payout_id,
          event_id,
          user_id,
          currency,
          country,
          payout_type,
          trx_date,
          created,
          changed,
          trx_id,
          notes,
          account_type,
          reason,
          status,
          status_old,
          amount,
          payment_id,
          withheld_status,
          'history_payout' source
          from (
          select *, row_number() over (partition by payout_id order by changed desc, log_position desc) rnum
    --       from hive.eb.Payouts
          from hive.eb_history.history_payouts
          where payout_id not in (select payout_id from hive.eb_history.history_payouts where status = 8)
          and payout_type = 'ACH' and ((currency = 'BRL' and changed < date '2019-01-01') OR (currency = 'ARS' and changed < date '2018-12-29'))
          and status not in (0,10,12,2)
          and payout_id not in (7878303,7882802,7882803,7884094,7884095,7884096,7884098,7884099,7884100,7884101,7884102,7884104,7884105,7884107,7884113,7884114,7884115,7884116,7884118,7884123,7884124,
                  7884127,7884129,7884130,7884135,7884136,7884137,7884140,7884141,7884145,7884146,7884151,7884153,7884154,7884159,7884161,7884164,7884166,7884170,7884172,7884174,7884175,7884183,7884185,7884186,7884187,7884189,
                  7884190,7884194,7884198,7884207,7884208,7884209,7884211,7884213,7884214,7884216,7884224,7884241,7884242,7884244,7884245,7884247,
                  7884253,7884267,7884275,7884277,7884280,7884282,7884283,7884284,7884285,7884286,7884289,7884290,7884291,7884293,7884294,7884295) -- BRL list from lorena where she manually voided them because of an issue that happened in jan (never was uploaded to bank)
          and payout_id not in (7883590,7884106,7884110,7884132,7884139,7884143,7884149,7884156,7884163,7884165,7884167,7884168,7884169,
                  7884184,7884212,7884215,7884220,7884227,7884243,7884268,7884269,7884274,7884276,7884278,7884279,7884281,7884288) --- ARS list from lorena where she manually voided them because of an issue that happened in jan (never was uploaded to bank)
        )
        where rnum = 1
      )
    union (
      select
      id as payout_id,
      event as event_id,
      user_id,
      currency,
      country,
      payout_type,
      trx_date,
      created,
      changed,
      trx_id,
      notes,
      account_type,
      reason,
      status,
      null status_old,
      amount,
      payment_id,
      withheld_status,
      'payout' source
      from hive.eb.payouts
      where created < date '2019-01-01' -- somewhat arbitrary date.
        and id not in (select distinct payout_id from hive.eb_history.history_payouts)); -- exclude any payouts that are included in history
    --     and id not in (select payout_id from (
    --       select *, row_number() over (partition by payout_id order by changed desc) rnum
    -- --       from hive.eb.Payouts
    --       from hive.eb_history.history_payouts
    --       where (payout_type = 'ACH' and status != 8 and (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-05-01' --- change last date.
    --       and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
    --           cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-05-02' -- change date.
    --       and currency in ('AUD','CAD','NZD','USD','HKD','SGD'))
    --       or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-05-01' --- change last date.
    --       and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
    --           cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-04-30' -- change date.
    --       and currency = 'GBP')
    --       or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-05-01' --- change last date.
    --       and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
    --           cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-05-01' -- change date. --- dummy date
    --       and currency = 'MXN')
    --
    --       or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-05-01' --- change last date.
    --       and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
    --           cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-05-01' -- change date.
    --       and currency = 'EUR'))
    --
    --       or (payout_type = 'CHECK'
    --       and cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' -- change date.
    --       and currency in ('AUD','CAD','EUR','GBP','NZD','USD','BRL','ARS','MXN','SGD','HKD'))
    --
    --       or (status = 8
    --       -- and (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) between date '2017-01-01' and date '2019-04-01' --- change last date.
    --       and coalesce(cast(substr(cast(trx_date at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp),
    --           cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)) < date '2019-05-01' -- change date.
    --       and currency in ('AUD','CAD','EUR','GBP','NZD','USD','BRL','ARS','MXN','SGD','HKD'))
    --
    --       or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and currency in ('ARS','BRL')) -- change date
    --       or (cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2017-01-01' and currency in ('AUD','CAD','EUR','GBP','NZD','USD','SGD','HKD','MXN')) -- do not change date
    --       )
    --       where rnum = 1));

       FINAL NET SALES + PAYOUTS

--- assumes all withheld payments from eb (transactions from 6/1/2013 and before) were not in file.
drop table hive.team_data_insights.creator_payouts_due_2018_dec_detailed;
      -- CREATE TABLE hive.team_data_insights.creator_payouts_due_ebh_with_transfers_and_tt_2018_to_2018_dec_detailed
      CREATE TABLE hive.team_data_insights.creator_payouts_due_2018_dec_detailed
      as
      SELECT
      -- e.*
      coalesce(e.event_id,p.event_id) as event_id,
      e.current_status,
      e.account_type,
      e.end_date,
      coalesce(e.org_uid,p.user_id) org_uid,
      coalesce(e.currency,p.currency) currency,
      e.epp_net_sales,
      e.offline_net_sales,

      sum(case when payout_type = 'ACH'
          and (
                (coalesce(e.currency,p.currency) not in ('BRL','ARS') and (p.status in (0,1,2,3,4,6,7,10,11,12,13)
                or (p.status = 5 and p.status_old = 2 and (p.withheld_status in (1,2,4,5) or p.reason in (6,7))))
                )
                or (coalesce(e.currency,p.currency) in ('BRL','ARS') and (p.status in (1,3,4,6,7,11,13) or
                (p.status = 5 and p.status_old = 2 and (p.withheld_status in (1,2,4,5) or p.reason in (6,7)))))
              )
        then p.amount else 0 end) ach_confirmed_in_file,

        sum(case when payout_type = 'ACH' and ((coalesce(e.currency,p.currency) in ('BRL','ARS') and p.status = 1)
          or (coalesce(e.currency,p.currency) not in ('BRL','ARS') and p.status in (0,1,2,10,11,12,13)))
          then p.amount else 0 end) ach_paid_out_in_file,

        sum(case when payout_type = 'ACH' and p.status = 5 and p.status_old = 2 and p.withheld_status = 2
          then p.amount else 0 end) ach_withheld_fraud_in_file,

        sum(case when payout_type = 'ACH' and p.status = 5 and p.status_old = 2 and p.withheld_status = 4
          then p.amount else 0 end) ach_withheld_trust_safety_in_file,

        sum(case when payout_type = 'ACH' and p.status = 5 and p.status_old = 2 and p.withheld_status = 5
          then p.amount else 0 end) ach_withheld_canceled_event_in_file,

        sum(case when payout_type = 'ACH' and p.status = 5 and p.status_old = 2 and p.withheld_status = 1
          then p.amount else 0 end) ach_withheld_ach_recoup_in_file,

        sum(case when payout_type = 'ACH' and p.status = 5 and p.status_old = 2 and p.withheld_status is null and p.reason in (6,7)
          then p.amount else 0 end) ach_withheld_other_fraud_in_file,

          sum(case when payout_type = 'ACH' and p.status = 5 and (p.status_old != 2 or p.status_old is null) and p.withheld_status = 2
            then p.amount else 0 end) ach_withheld_fraud_not_in_file,

          sum(case when payout_type = 'ACH' and p.status = 5 and (p.status_old != 2 or p.status_old is null) and p.withheld_status = 4
            then p.amount else 0 end) ach_withheld_trust_safety_not_in_file,

          sum(case when payout_type = 'ACH' and p.status = 5 and (p.status_old != 2 or p.status_old is null) and p.withheld_status = 5
            then p.amount else 0 end) ach_withheld_canceled_event_not_in_file,

          sum(case when payout_type = 'ACH' and p.status = 5 and (p.status_old != 2 or p.status_old is null) and p.withheld_status = 1
            then p.amount else 0 end) ach_withheld_ach_recoup_not_in_file,

          sum(case when payout_type = 'ACH' and p.status = 5 and (p.status_old != 2 or p.status_old is null) and p.withheld_status is null and p.reason in (6,7)
            then p.amount else 0 end) ach_withheld_other_fraud_not_in_file,

        -sum(case when payout_type = 'ACH' and p.status = 3
          then p.amount else 0 end) ach_deleted_in_file,

        -sum(case when payout_type = 'ACH' and p.status = 4
          then p.amount else 0 end) ach_voided_in_file,

        -sum(case when payout_type = 'ACH' and p.status = 6
          then p.amount else 0 end) ach_returned_in_file,

        -sum(case when payout_type = 'ACH' and p.status = 7
          then p.amount else 0 end) ach_reissued_in_file,

        sum(case when p.status = 8 then p.amount else 0 end) wire_paid_out,

        sum(case when payout_type = 'CHECK' and status != 8
          then p.amount else 0 end) check_written,

        sum(case when p.status IS NULL then 0
          when payout_type = 'CHECK'
          and p.status in (0,1,2,10,11,12,13)
          then p.amount else 0 end) check_completed,

        sum(case when payout_type = 'CHECK'
          and p.status = 5 and (p.withheld_status in (1,2,4,5)
          or (p.withheld_status is null and p.reason in (6,7)))
          then p.amount else 0 end) check_withheld,

        sum(case when payout_type = 'CHECK'
          and p.status = 3
          then p.amount else 0 end) check_deleted,

          -sum(case when payout_type = 'CHECK'
            and p.status = 4
            then p.amount else 0 end) check_voided,

        -sum(case when payout_type = 'CHECK'
            and p.status in (6,7)
            then p.amount else 0 end) check_reissued_returned

      -- from hive.team_data_insights.eb_history_through_2017_with_transfers_and_tt_2018_to_2018_dec_net_sales e
      from hive.team_data_insights.creator_balance_due_2018_dec_net_sales e
      -- from hive.team_data_insights.tiny_tim_net_sales_through_2018_dec_new e
      -- left join hive.team_data_insights.scratch_payouts_2018_dec_new p on e.event_id = p.event and p.currency = e.currency
      -- full join hive.team_data_insights.scratch_payouts_2018_dec_new p on e.event_id = p.event and p.currency = e.currency
      full join hive.team_data_insights.scratch_history_payouts_2018_dec_new p on e.event_id = p.event_id and p.currency = e.currency
      group by 1,2,3,4,5,6,7,8;



CREATE CREDITS CHARGES TABLE
      CREATE TABLE hive.team_data_insights.scratch_invoice_charges_2018_dec
      as
      select * from hive.eb.Invoice_Charges
      where cast(substr(cast(created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-01-01' --- change date
      and currency in ('ARS','AUD','BRL','CAD','EUR','GBP','NZD','USD','HKD','SGD','MXN')
      ;


    --   CREATE TABLE hive.team_data_insights.scratch_invoice_charges_2018_dec_new
    --   as
    --   select * from (
    --   select row_number() over (partition by invoice_charge_id order by changed desc, log_position desc) rnum, * from hive.eb_history.history_Invoice_Charges
    --   where cast(substr(cast(changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' --- change date
    --   and currency in ('ARS','AUD','BRL','CAD','EUR','GBP','NZD','USD','HKD','SGD','MXN')
    -- ) where rnum = 1;



      FINAL NET SALES + PAYOUTS + INVOICE CHARGES
      -- drop table hive.team_data_insights.creator_event_level_ebh_with_transfers_and_tt_2018_to_2018_dec;
      -- CREATE TABLE hive.team_data_insights.creator_event_level_ebh_with_transfers_and_tt_2018_to_2018_dec
      -- as
      -- SELECT
      -- p.*,
      -- sum(case when ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp)
      --             between timestamp '2013-08-14 03:13:18.000' and date '2019-05-01' and i.payout_id is not NULL) --- change date
      --           or i.charge_type in ('CREDIT_BACKUP_FUNDING','CREDIT_BALANCE_TRANSFER','CHARGE_BALANCE_TRANSFER')
      --           or (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) --- do not change date
      -- then i.amount else 0 end) credits_charges
      -- from hive.team_data_insights.creator_payouts_due_ebh_with_transfers_and_tt_2018_to_2018_dec p
      -- left join hive.team_data_insights.scratch_invoice_charges_2018_dec i on i.event = p.event_id
      -- where p.event_id not in (select event_id from hive.df_risk.risk_metrics_events_with_wire_ins)
      -- group by 1,2,3,4,5,6,7,8;

drop table hive.team_data_insights.creator_event_level_2018_dec_detailed;
      -- CREATE TABLE hive.team_data_insights.creator_event_level_ebh_with_transfers_and_tt_2018_to_2018_dec_detailed
      CREATE TABLE hive.team_data_insights.creator_event_level_2018_dec_detailed
          as
          SELECT
          p.*,
          sum(case when i.charge_type in ('CREDIT_BACKUP_FUNDING','CHARGE_UNDERPAYMENT_REFUND') and cast(substr(cast(i.created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_backup_funding_total,
          sum(case when i.charge_type = 'CREDIT_BALANCE_TRANSFER' and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_balance_transfer_total,
          sum(case when i.charge_type = 'CHARGE_BALANCE_TRANSFER' and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) charge_balance_transfer_total,
          sum(case when i.charge_type in ('CREDIT_BACKUP_FUNDING','CHARGE_UNDERPAYMENT_REFUND') and i.processed = 0 and cast(substr(cast(i.created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_backup_funding_not_applied,
          sum(case when i.charge_type = 'CREDIT_BALANCE_TRANSFER' and i.processed = 0 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_balance_transfer_not_applied,
          sum(case when i.charge_type = 'CHARGE_BALANCE_TRANSFER' and i.processed = 0 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) charge_balance_transfer_not_applied,
          sum(case when i.charge_type in ('CREDIT_BACKUP_FUNDING','CHARGE_UNDERPAYMENT_REFUND') and i.processed = 1 and cast(substr(cast(i.created at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_backup_funding_applied,
          sum(case when i.charge_type = 'CREDIT_BALANCE_TRANSFER' and i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) credit_balance_transfer_applied,
          sum(case when i.charge_type = 'CHARGE_BALANCE_TRANSFER' and i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' then i.amount end) charge_balance_transfer_applied,

          sum(case when charge_type in ('CHARGE_ATTENDEE_DISPUTES') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end) as charge_attendee_disputes,
          -- sum(case when charge_type in ('CHARGE_UNDERPAYMENT_REFUND','CREDIT_BACKUP_FUNDING_REFUND_FAILED') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
          --             and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
          --             (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end) as backup_funding,
          sum(case when charge_type in ('CHARGE_CHARGEBACK_LOSS') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as charge_chargeback_loss,
          sum(case when charge_type in ('CREDIT_SITE_OUTAGE', 'CREDIT_FEE_CREDIT-CrabApple','CREDIT_FEE_CREDIT - CrabApple') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as crabapple,
          sum(case when charge_type in ('CHARGE_SHIPPING_FEE','ATD Card Reader','CHARGE_SCANNER_RENTAL','Shipping Fee','Scanner Rental','CREDIT_ATD_CARD_READER','CHARGE_ONSITE_TECHNOLOGY')
                      and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as equipment,
          sum(case when charge_type in ('Fee Credit','CREDIT_FEE_CREDIT','CREDIT_REFUND_OVERCHARGED_FEES') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as fee_credit,
          sum(case when charge_type in ('Ticket Fee','Printed Tickets Fee') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as fees_revenue,
          sum(case when charge_type in ('Marketing Referral') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as marketing_referral,
          sum(case when charge_type in ('CHARGE_OTHER','Equipment Problems','CREDIT_REFERRAL_CREDIT','CREDIT_DISSATISFIED_CUSTOMER','CREDIT_OTHER','CHARGE_FEE_CORRECTION','Referral Credit','CHARGE_LOST_DAMAGED_EQUIPMENT','CREDIT_PAYOUT_ISSUE',
                      'CHARGE_STAFFING_ACCOUNT_MANAGEMENT','Dissatisfied Customer','Lost/Damaged Equipment','CREDIT_BUG','Adjustment','CREDIT_EQUIPMENT_PROBLEMS','CREDIT_REFUNDS_AFTER_INVOICE','CREDIT_OVERPAYMENT_FOR_ATTENDEE_REFUNDS',
                      'CHARGE_ADDITIONAL_PAYMENT_PROCESSING_FEE','CHARGE_REFUNDS_AFTER_INVOICE','Payout Issue','Bug','Refunds After Invoice Issued','CHARGE_LATE_FEES','Other')
                      and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as miscellaneous,
          sum(case when charge_type in ('CHARGE_RFID_CHIPS') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as rfid,
          sum(case when charge_type in ('CHARGE_TEMPORARY_REFUND_RESERVE','CHARGE_EVENT_PROMOTION_RALLY','Transfer Charge','CHARGE_OTHER_INVOICES','Refund','Attendee Disputes',
                      'Mailchimp','Transfer Credit','Site Outage','Payout transaction charge','Setup Fee','payout transaction charge','Chargeback Loss')
                      and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as old,
          sum(case when charge_type in ('CHARGE_STAFFING_FIELD_OPERATIONS','CHARGE_ONSITE_SERVICE') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as staffing,
          sum(case when charge_type in ('CHARGE_BRAND_PAGES') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as charge_brand_pages,
          sum(case when charge_type in ('CHARGE_EVENT_PROMOTION_ALGORITHMIC') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as charge_event_promotion_algorithmic,
          sum(case when charge_type in ('CHARGE_RECOUPABLE_UPFRONT_RECOUPMENT') and ((i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                      and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) or
                      (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id)) then i.amount end)  as charge_recoup_upfront_recoupment,
          sum(case when (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) >= timestamp '2013-08-14 03:13:18.000'
                        and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01' and i.payout_id is not NULL) --- change date
                    or (i.charge_type in ('CREDIT_BACKUP_FUNDING','CREDIT_BALANCE_TRANSFER','CHARGE_BALANCE_TRANSFER') and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < date '2019-05-01') -- change date
                    or (i.processed = 1 and cast(substr(cast(i.changed at time zone 'America/Los_Angeles' as varchar),1,19) as timestamp) < timestamp '2013-08-14 03:13:18.000' and i.parent_id = i.id) --- do not change date
          then i.amount else 0 end) credits_charges_total
          -- from hive.team_data_insights.creator_payouts_due_ebh_with_transfers_and_tt_2018_to_2018_dec_detailed p
          from hive.team_data_insights.creator_payouts_due_2018_dec_detailed p
          left join hive.team_data_insights.scratch_invoice_charges_2018_dec i on i.event = p.event_id and p.currency = i.currency
          -- left join hive.team_data_insights.scratch_invoice_charges_2018_dec_new i on i.event_id = p.event_id and p.currency = i.currency
          where p.event_id not in (select event_id from hive.df_risk.risk_metrics_events_with_wire_ins)
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31;






------- balance
select
  round(coalesce(epp_net_sales,0)
  + coalesce(offline_net_sales,0)
  + coalesce(ach_confirmed_in_file,0)
  + coalesce(ach_voided_in_file,0)
  + coalesce(ach_deleted_in_file,0)
  + coalesce(ach_returned_in_file,0)
  + coalesce(ach_reissued_in_file,0)
  + coalesce(wire_paid_out,0)
  + coalesce(check_written,0)
  + coalesce(check_reissued_returned,0)
  + coalesce(check_voided,0)
  + coalesce(check_deleted,0) --- double check - this one doesn't line up with accounting
  + coalesce(credits_charges_total,0),2)
AS balance,
*
from hive.team_data_insights.creator_event_level_2018_dec_detailed
