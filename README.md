
# Configure Postgres

```bash
$ psql -U postgres -d quant_async -h localhost -p 5432 

postgres=# CREATE DATABASE quant_async;
postgres=# CREATE USER quant_async WITH PASSWORD 'quant_async';
postgres=# GRANT ALL PRIVILEGES ON DATABASE quant_async TO quant_async;

$ psql -U postgres -d quant_async -h localhost -p 5432 
quant_async=# GRANT CREATE ON SCHEMA public TO quant_async;
```
