library(RPostgres)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='bing_han')

res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables
                   where table_type ='VIEW'
                   or table_type ='FOREIGN TABLE'
                   order by table_schema")
data <- dbFetch(res, n=-1)
data
dbClearResult(res)

res <- dbSendQuery(wrds, "select count(0)
                   from crsp.wrds_msfv2_query
                   where 1=1
                   and securitytype = 'FUND'
                   and mthprcdt between '2010-01-01' and '2024-12-31'
                   limit 1")
data <- dbFetch(res, n = -1)
dbClearResult(res)
data

res <- dbSendQuery(wrds, "select *
                   from crsp.wrds_msfv2_query
                   where 1=1
                   and securitytype = 'FUND'
                   and mthprcdt between '2010-01-01' and '2024-12-31'
                   limit 20")
data <- dbFetch(res, n = -1)
dbClearResult(res)
data

