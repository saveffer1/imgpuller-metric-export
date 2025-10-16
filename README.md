# imgpuller-metric-export
http server with simple api for zabbix docker image pull monitor

## build the image
docker compose build --no-cache

## run

### first time
docker compose run --rm imgpuller --init-db

### run docker
docker compose up -d

## development

dotenvx run -- cargo run -- --init-db

dotenvx run -- cargo watch -x run
