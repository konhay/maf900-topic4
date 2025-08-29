library(RPostgres)

# Authorizing te WRDS 

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='bing_han')

# List the database names 

res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables
                   where table_type ='VIEW'
                   or table_type ='FOREIGN TABLE'
                   order by table_schema")
data <- dbFetch(res, n=-1)
data
dbClearResult(res)


# CRSP monthly fund retruns

# Check the data size 

res <- dbSendQuery(wrds, "select count(0)
                   from crsp.wrds_msfv2_query
                   where 1=1
                   and securitytype = 'FUND'
                   and yyyymm between '201001' and '202412'
                   limit 1")
data <- dbFetch(res, n = -1)
dbClearResult(res)
data

# Data sample of CRSP 

res <- dbSendQuery(wrds, "select permno, ticker, securitynm, yyyymm,
    mthret, mthprc, mthcap, shrout, mthvol
                   from crsp.wrds_msfv2_query
                   where 1=1
                   and securitytype = 'FUND'
                   and yyyymm between '201001' and '202412'
                   limit 20")
data_crsp <- dbFetch(res, n = -1)
dbClearResult(res)
data_crsp


# FactSet Historical Fund Holding Data

# Check the entity_proper_name

res <- dbSendQuery(wrds, "SELECT DISTINCT entity_proper_name
                  FROM factset.wrds_own_fund
                  WHERE 1=1
                  and report_date between '2010-01-01' and '2024-12-31'
                  and entity_proper_name LIKE '%ETF%'
                  limit 100")
data <- dbFetch(res, n = -1)
dbClearResult(res)
data

# Check the data size 

res <- dbSendQuery(wrds, "SELECT COUNT(0)
                  FROM factset.wrds_own_fund
                  WHERE 1=1
                  and report_date between '2010-01-01' and '2024-12-31'
                  and entity_proper_name LIKE '%ETF%'
                  limit 100")
data <- dbFetch(res, n = -1)
dbClearResult(res)
data

# Data sample of FactSet  

res <- dbSendQuery(wrds, "SELECT factset_fund_id,
                  entity_proper_name,
                  report_date,
                  fsym_id,
                  security_name,
                  cusip,
                  issue_type,
                  adj_holding,
                  adj_mv,
                  adj_price
                  FROM factset.wrds_own_fund
                  WHERE 1=1
                  and report_date between '2010-01-01' and '2024-12-31'
                  and entity_proper_name LIKE '%ETF%'
                  limit 100")
data_factset <- dbFetch(res, n = -1)
dbClearResult(res)
data_factset

# What to be done next is the combination of these two data sample.
